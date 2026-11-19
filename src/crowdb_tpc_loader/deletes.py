"""Add merge-on-read delete files to a freshly loaded table.

PyIceberg has no public API for this, so the delete manifest and snapshot are produced with
its snapshot producer. Only format-version 2 delete files are written: position and equality.
"""

from __future__ import annotations

from typing import Any, Callable

from .backend import data_location
from .errors import LoadError
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
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
        ManifestContent,
        ManifestEntry,
        ManifestEntryStatus,
        ManifestWriterV2,
    )
    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.update.snapshot import _FastAppendFiles
    from pyiceberg.typedef import Record

    every = max(1, round(1 / fraction))
    content = DataFileContent.POSITION_DELETES if mode == "position" else DataFileContent.EQUALITY_DELETES
    journal = row.setdefault("delete_files", [])
    files = []
    for index, (part, uri) in enumerate(zip(data.parts, uris)):
        if part.rows == 0:
            continue
        deletes, equality_ids = _delete_table(pa, pq, table, data.name, part, uri, mode, every)
        local = part.path.with_name(f"{part.path.stem}.{mode}-deletes.parquet")
        pq.write_table(deletes, local)
        remote = data_location(table, f"{data.name}/delete-{index:06d}.parquet")
        item = {"remote_uri": remote, "mode": mode, "row_count": deletes.num_rows, "state": "upload_started"}
        journal.append(item)
        checkpoint(row, None)  # Persist the URI before touching remote storage.
        emit(
            f"Upload {data.name}: {mode} delete file {index + 1}/{len(data.parts)}, {deletes.num_rows:,} rows"
        )
        size = local.stat().st_size
        uploader(table, ParquetPart(local, deletes.num_rows, size, deletes.schema), remote, buffer_size, emit)
        item["state"] = "uploaded_unregistered"
        files.append(
            DataFile.from_args(
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
        )
    if not files:
        return

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

    for item in journal:
        item["state"] = "commit_unknown"
    checkpoint(row, None)  # Persist the uncertain state before the commit.
    emit(f"Register {data.name}: {len(files)} {mode} delete file(s)")
    try:
        with table.transaction() as transaction:
            producer = AddDeleteFiles(
                Operation.DELETE,
                transaction,
                table.io,
                snapshot_properties={"crowdb-tpc-loader.run-id": run_id},
            )
            for file in files:
                producer.append_data_file(file)
            producer.commit()
    except Exception as exc:
        raise LoadError(
            f"{data.name}: delete commit did not complete ({exc}); its state is uncertain and it was not retried"
        ) from exc
    for item in journal:
        item["state"] = "registered"
    row["delete_snapshot_id"] = table.metadata.current_snapshot_id
    checkpoint(row, None)
