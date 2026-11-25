"""Add merge-on-read delete files to a freshly loaded table.

PyIceberg has no public API for this, so the delete manifest and snapshot are produced with
its snapshot producer. Only format-version 2 delete files are written: position and equality.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Sequence

from .backend import data_location
from .errors import ArgumentError, LoadError
from .field_ids import FIELD_ID
from .models import ParquetPart, TableData
from .validation import require_arrow

MODES = ("position", "equality")
# Columns that identify one row. An equality delete on fewer columns removes every row sharing them.
EQUALITY_KEYS = {
    "region": ("r_regionkey",),
    "nation": ("n_nationkey",),
    "supplier": ("s_suppkey",),
    "customer": ("c_custkey",),
    "part": ("p_partkey",),
    "partsupp": ("ps_partkey", "ps_suppkey"),
    "orders": ("o_orderkey",),
    "lineitem": ("l_orderkey", "l_linenumber"),
}
# Reserved field IDs of a position delete file (Iceberg spec, "Position Delete Files").
FILE_PATH_ID, POS_ID = 2147483546, 2147483545


def _with_id(pa: Any, name: str, dtype: Any, field_id: int, nullable: bool) -> Any:
    return pa.field(name, dtype, nullable=nullable, metadata={FIELD_ID: str(field_id).encode()})


def _delete_table(
    pa: Any, pq: Any, table: Any, name: str, part: ParquetPart, uri: str, mode: str, every: int
):
    """Rows to delete from one data file: every `every`-th row, by position or by row key."""
    if mode == "position":
        positions = pa.array(range(0, part.rows, every), pa.int64())
        schema = pa.schema(
            [
                _with_id(pa, "file_path", pa.string(), FILE_PATH_ID, False),
                _with_id(pa, "pos", pa.int64(), POS_ID, False),
            ]
        )
        return pa.table([pa.array([uri] * len(positions), pa.string()), positions], schema=schema), None
    fields = [table.schema().find_field(column) for column in EQUALITY_KEYS[name]]
    rows = pq.read_table(part.path, columns=[field.name for field in fields])
    rows = rows.take(pa.array(range(0, part.rows, every), pa.int64()))
    schema = pa.schema(
        [_with_id(pa, f.name, rows.schema.field(f.name).type, f.field_id, True) for f in fields]
    )
    return pa.table(rows.columns, schema=schema), [field.field_id for field in fields]


def _stage(
    table: Any,
    name: str,
    mode: str,
    deletes: Any,
    equality_ids: list[int] | None,
    local: Path,
    index: int,
    label: str,
    row: dict,
    checkpoint: Callable[[dict, str | None], None],
    uploader: Callable,
    buffer_size: int,
    emit: Callable[[str], None],
) -> tuple[dict, Any]:
    """Write, journal and upload one delete file; return its journal item and manifest data file."""
    _, pq = require_arrow()
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.typedef import Record

    pq.write_table(deletes, local)
    remote = data_location(table, f"{name}/delete-{index:06d}.parquet")
    item = {"remote_uri": remote, "mode": mode, "row_count": deletes.num_rows, "state": "upload_started"}
    row.setdefault("delete_files", []).append(item)
    checkpoint(row, None)  # Persist the URI before touching remote storage.
    emit(f"Upload {name}: {mode} delete file {label}, {deletes.num_rows:,} rows")
    size = local.stat().st_size
    uploader(table, ParquetPart(local, deletes.num_rows, size, deletes.schema), remote, buffer_size, emit)
    item["state"] = "uploaded_unregistered"
    content = DataFileContent.POSITION_DELETES if mode == "position" else DataFileContent.EQUALITY_DELETES
    return item, DataFile.from_args(
        content=content,
        file_path=item["remote_uri"],
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=deletes.num_rows,
        file_size_in_bytes=size,
        equality_ids=equality_ids,
        sort_order_id=None,
        spec_id=table.metadata.default_spec_id,
        key_metadata=None,
    )


def _commit(
    table: Any,
    name: str,
    mode: str,
    staged: list[tuple[dict, Any]],
    run_id: str,
    row: dict,
    checkpoint: Callable[[dict, str | None], None],
    emit: Callable[[str], None],
) -> None:
    """Commit the staged delete files in one snapshot and record its ID. Never retried."""
    from pyiceberg.manifest import (
        ManifestContent,
        ManifestEntry,
        ManifestEntryStatus,
        ManifestWriterV2,
    )
    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    class DeleteManifestWriter(ManifestWriterV2):
        def content(self) -> ManifestContent:
            return ManifestContent.DELETES

        @property
        def _meta(self) -> dict[str, str]:
            return {**super()._meta, "content": "deletes"}

    class AddDeleteFiles(_FastAppendFiles):
        def _manifests(self):
            metadata = self._transaction.table_metadata
            with DeleteManifestWriter(
                metadata.spec(),
                metadata.schema(),
                self.new_manifest_output(),
                self._snapshot_id,
                self._compression,
            ) as writer:
                for file in self._added_data_files:
                    writer.add(
                        ManifestEntry.from_args(
                            status=ManifestEntryStatus.ADDED,
                            snapshot_id=self._snapshot_id,
                            sequence_number=None,
                            file_sequence_number=None,
                            data_file=file,
                        )
                    )
            return [writer.to_manifest_file(), *self._existing_manifests()]

    for item, _ in staged:
        item["state"] = "commit_unknown"
    checkpoint(row, None)  # Persist the uncertain state before the commit.
    emit(f"Register {name}: {len(staged)} {mode} delete file(s)")
    try:
        with table.transaction() as transaction:
            producer = AddDeleteFiles(
                Operation.DELETE,
                transaction,
                table.io,
                snapshot_properties={"crowdb-tpc-loader.run-id": run_id},
            )
            for _, file in staged:
                producer.append_data_file(file)
            producer.commit()
    except Exception as exc:
        raise LoadError(
            f"{name}: delete commit did not complete ({exc}); its state is uncertain and it was not retried"
        ) from exc
    for item, _ in staged:
        item["state"] = "registered"
    row["delete_snapshot_id"] = table.metadata.current_snapshot_id


def add_deletes(
    table: Any,
    data: TableData,
    uris: list[str],
    mode: str,
    fraction: float,
    run_id: str,
    row: dict,
    checkpoint: Callable[[dict, str | None], None],
    uploader: Callable,
    buffer_size: int,
    emit: Callable[[str], None],
) -> None:
    """Upload one delete file per data file and commit them in one snapshot. Never retried."""
    pa, pq = require_arrow()
    every = max(1, round(1 / fraction))
    row.setdefault("delete_files", [])
    staged = []
    for index, (part, uri) in enumerate(zip(data.parts, uris)):
        if part.rows == 0:
            continue
        deletes, equality_ids = _delete_table(pa, pq, table, data.name, part, uri, mode, every)
        local = part.path.with_name(f"{part.path.stem}.{mode}-deletes.parquet")
        label = f"{index + 1}/{len(data.parts)}"
        staged.append(
            _stage(
                table,
                data.name,
                mode,
                deletes,
                equality_ids,
                local,
                index,
                label,
                row,
                checkpoint,
                uploader,
                buffer_size,
                emit,
            )
        )
    if not staged:
        return
    _commit(table, data.name, mode, staged, run_id, row, checkpoint, emit)
    checkpoint(row, None)


def key_rounds(options: Any, data: TableData) -> list[list[int]] | None:
    """Data file indexes per round when the table is deleted by one key set; None otherwise."""
    name = data.name
    if getattr(options, "delete_mode", None) != "equality":
        return None
    if options.delete_tables and name not in options.delete_tables:
        return None
    if name not in options.delete_keys_files:
        if not options.delete_fraction:
            return None
        if name not in options.delete_key_columns and options.delete_commits == 1:
            return None
    filled = [index for index, part in enumerate(data.parts) if part.rows]
    count = max(1, min(options.delete_commits, len(filled)))
    rounds = [filled[len(filled) * r // count : len(filled) * (r + 1) // count] for r in range(count)]
    # Empty files hold no keys; they are registered with the first round.
    rounds[0] = sorted(set(range(len(data.parts))) - set(filled) | set(rounds[0]))
    return rounds


def select_keys(paths: Sequence[Path], columns: Sequence[str], every: int) -> Any:
    """Every `every`-th of the sorted distinct key tuples in the given data files."""
    pa, pq = require_arrow()
    names = list(columns)
    distinct = [pq.read_table(path, columns=names).group_by(names).aggregate([]) for path in paths]
    keys = pa.concat_tables(distinct).group_by(names).aggregate([]).select(names)
    keys = keys.sort_by([(name, "ascending") for name in names])
    return keys.take(pa.array(range(0, keys.num_rows, every), pa.int64()))


def keys_file_schema(path: Path, table: str, schema: Any) -> Any:
    """Check a keys file's columns against the table's Arrow schema; return the schema to cast it to."""
    pa, pq = require_arrow()
    try:
        footer = pq.read_metadata(path)
        source = footer.schema.to_arrow_schema()
    except Exception as exc:
        raise ArgumentError(f"Keys file for {table} is not a readable Parquet file: {path}: {exc}") from exc
    unknown = [name for name in source.names if name not in schema.names]
    if unknown or not source.names or len(set(source.names)) != len(source.names):
        raise ArgumentError(
            f"Keys file for {table} must have distinct columns named after table columns; "
            f"got {source.names}, unknown {unknown}"
        )
    if footer.num_rows == 0:
        raise ArgumentError(f"Keys file for {table} has no rows: {path}")
    target = pa.schema([pa.field(name, schema.field(name).type) for name in source.names])
    try:
        source.empty_table().cast(target)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ArgumentError(
            f"Keys file for {table} cannot be cast to the table's column types: {exc}"
        ) from exc
    return target


def read_keys(path: Path, table: str, schema: Any) -> Any:
    """Distinct rows of a keys file, cast to the table's column types."""
    pa, pq = require_arrow()
    target = keys_file_schema(path, table, schema)
    try:
        keys = pq.read_table(path).select(target.names).cast(target)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise LoadError(f"{table}: keys file values do not fit the table's column types: {exc}") from exc
    return keys.group_by(target.names, use_threads=False).aggregate([]).select(target.names)


def add_key_deletes(
    table: Any,
    data: TableData,
    indexes: Sequence[int],
    snapshot_id: int | None,
    options: Any,
    run_id: str,
    row: dict,
    checkpoint: Callable[[dict, str | None], None],
    uploader: Callable,
    buffer_size: int,
    emit: Callable[[str], None],
) -> None:
    """Commit one equality-delete snapshot for one round of data files. Never retried.

    The keys are the table's keys file, or every Nth sorted distinct key of this round's data files;
    they are split into delete files of at most `options.delete_file_rows` rows.
    """
    pa, _ = require_arrow()
    name = data.name
    paths = [data.parts[index].path for index in indexes if data.parts[index].rows]
    if name in options.delete_keys_files:
        keys = read_keys(options.delete_keys_files[name], name, data.schema)
    elif paths:
        columns = options.delete_key_columns.get(name, EQUALITY_KEYS[name])
        keys = select_keys(paths, columns, max(1, round(1 / options.delete_fraction)))
    else:
        return
    fields = [table.schema().find_field(column) for column in keys.column_names]
    schema = pa.schema(
        [_with_id(pa, f.name, keys.schema.field(f.name).type, f.field_id, True) for f in fields]
    )
    equality_ids = [field.field_id for field in fields]
    journal = row.setdefault("delete_files", [])
    rounds = row.setdefault("delete_rounds", [])
    limit = options.delete_file_rows
    offsets = range(0, keys.num_rows, limit)
    staged = []
    for number, offset in enumerate(offsets):
        index = len(journal)
        staged.append(
            _stage(
                table,
                name,
                "equality",
                pa.table(keys.slice(offset, limit).columns, schema=schema),
                equality_ids,
                data.parts[0].path.with_name(f"{name}.equality-deletes-{index:06d}.parquet"),
                index,
                f"{number + 1}/{len(offsets)} of round {len(rounds) + 1}",
                row,
                checkpoint,
                uploader,
                buffer_size,
                emit,
            )
        )
    _commit(table, name, "equality", staged, run_id, row, checkpoint, emit)
    rounds.append(
        {
            "snapshot_id": snapshot_id,
            "delete_snapshot_id": row["delete_snapshot_id"],
            "data_files": len(indexes),
            "delete_files": len(staged),
            "delete_rows": keys.num_rows,
        }
    )
    checkpoint(row, None)
