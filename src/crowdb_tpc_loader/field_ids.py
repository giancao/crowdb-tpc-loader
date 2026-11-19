"""Write the target Iceberg table's field IDs into generated Parquet before upload."""

from __future__ import annotations

import os
from typing import Any, Callable

from .errors import ValidationError
from .models import ParquetPart, TableData
from .validation import require_arrow

FIELD_ID = b"PARQUET:field_id"


def assigned_ids(names: list[str]) -> dict[str, int]:
    """IDs a new flat Iceberg table receives: 1..N in column order."""
    return {name: index for index, name in enumerate(names, start=1)}


def schema_field_ids(schema: Any) -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    for field in schema:
        raw = (field.metadata or {}).get(FIELD_ID)
        result[field.name] = int(raw) if raw is not None else None
    return result


def _writer_options(source: Any, arrow: Any, pa: Any) -> dict[str, Any]:
    """Keep the generator's codecs and integer-backed decimals."""
    metadata = source.metadata
    codecs = {}
    if metadata.num_row_groups:
        group = metadata.row_group(0)
        for index in range(group.num_columns):
            column = group.column(index)
            codec = column.compression
            codecs[column.path_in_schema] = "NONE" if codec == "UNCOMPRESSED" else codec
    integer_decimal = any(
        pa.types.is_decimal(field.type) and source.schema.column(index).physical_type in {"INT32", "INT64"}
        for index, field in enumerate(arrow)
    )
    return {"compression": codecs or "NONE", "store_decimal_as_integer": integer_decimal}


def stamp_part(part: ParquetPart, table: str) -> ParquetPart:
    """Rewrite one file row group by row group; replace it only after its footer is checked."""
    pa, pq = require_arrow()
    temporary = part.path.with_name(part.path.name + ".field-ids.tmp")
    try:
        with pq.ParquetFile(part.path) as source:
            arrow = source.schema_arrow
            wanted = assigned_ids(arrow.names)
            target = pa.schema(
                [
                    field.with_metadata(
                        {**(field.metadata or {}), FIELD_ID: str(wanted[field.name]).encode()}
                    )
                    for field in arrow
                ]
            )
            groups = source.metadata.num_row_groups
            with pq.ParquetWriter(temporary, target, **_writer_options(source, arrow, pa)) as writer:
                for index in range(groups):
                    group = source.read_row_group(index)
                    writer.write_table(
                        pa.Table.from_arrays(group.columns, schema=target),
                        row_group_size=max(group.num_rows, 1),
                    )
        footer = pq.read_metadata(temporary)
        schema = footer.schema.to_arrow_schema()
        if (
            schema.names != arrow.names
            or schema_field_ids(schema) != wanted
            or footer.num_rows != part.rows
            or footer.num_row_groups != groups
        ):
            raise ValidationError(f"{table}: rewritten {part.path.name} does not match its source")
        os.replace(temporary, part.path)
    except ValidationError:
        temporary.unlink(missing_ok=True)
        raise
    except Exception as exc:
        temporary.unlink(missing_ok=True)
        raise ValidationError(f"{table}: cannot write field IDs into {part.path.name}: {exc}") from exc
    return ParquetPart(part.path, footer.num_rows, part.path.stat().st_size, schema)


def stamp_dataset(tables: dict[str, TableData], emit: Callable[[str], None]) -> dict[str, TableData]:
    """Return the dataset as it will be uploaded, checksummed and kept."""
    result: dict[str, TableData] = {}
    for name, data in tables.items():
        parts = tuple(stamp_part(part, name) for part in data.parts)
        result[name] = TableData(name, parts, parts[0].schema)
        emit(f"Field IDs {name}: {len(parts)} file(s), {result[name].size_bytes:,} bytes")
    return result
