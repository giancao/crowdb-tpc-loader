"""Equality deletes on real Parquet, registered in a temporary SQLite catalog over real HTTP.

Not CrowDB, and nothing here reads the table back through the deletes: PyIceberg cannot scan
equality deletes. The tests check which keys are written and where the commits sit.
"""

import json
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crowdb_tpc_loader.backend import IcebergBackend
from crowdb_tpc_loader.deletes import keys_file_schema, read_keys, select_keys
from crowdb_tpc_loader.errors import ArgumentError, LoadError
from crowdb_tpc_loader.field_ids import stamp_dataset
from crowdb_tpc_loader.loader import load_tables
from crowdb_tpc_loader.models import ParquetPart, TableData
from crowdb_tpc_loader.schemas import arrow_schema
from crowdb_tpc_loader.transfers import upload_part
from tests.unit import test_http_fileio

pytestmark = pytest.mark.integration
server = test_http_fileio.server  # Shared real HTTP fixture.


def test_keys_are_every_nth_distinct_tuple_across_files(tmp_path):
    paths = [tmp_path / "a.parquet", tmp_path / "b.parquet"]
    pq.write_table(pa.table({"k": [3, 1, 3, 2], "s": ["x", "y", "x", "z"]}), paths[0])
    pq.write_table(pa.table({"k": [1, 5, 4, 3], "s": ["y", "y", "x", "q"]}), paths[1])
    # 1 and 3 occur in both files and count once: distinct keys are 1..5.
    assert select_keys(paths, ["k"], 2).to_pydict() == {"k": [1, 3, 5]}
    # Distinct tuples in order: (1,y) (2,z) (3,q) (3,x) (4,x) (5,y).
    assert select_keys(paths, ["k", "s"], 2).to_pydict() == {"k": [1, 3, 4], "s": ["y", "q", "x"]}


def test_keys_file_columns_and_types(tmp_path):
    schema = arrow_schema("tpch", "orders")
    path = tmp_path / "keys.parquet"
    pq.write_table(
        pa.table({"o_orderdate": ["1995-01-02"] * 2, "o_custkey": pa.array([7, 7], pa.int32())}), path
    )
    keys = read_keys(path, "orders", schema)
    assert keys.schema.types == [pa.date32(), pa.int64()] and keys.num_rows == 1
    pq.write_table(pa.table({"o_nope": [1]}), path)
    with pytest.raises(ArgumentError, match="unknown"):
        keys_file_schema(path, "orders", schema)
    pq.write_table(pa.table({"o_orderdate": [1.5]}), path)
    with pytest.raises(ArgumentError, match="cannot be cast"):
        keys_file_schema(path, "orders", schema)
    pq.write_table(pa.table({"o_orderdate": ["not a date"]}), path)
    with pytest.raises(LoadError, match="do not fit"):
        read_keys(path, "orders", schema)


@pytest.fixture
def load(options, report, redactor, tmp_path, server):
    """Load `region` files with the given keys into a fresh table; return its report row and the table."""
    pytest.importorskip("sqlalchemy")
    from pyiceberg.catalog.sql import SqlCatalog

    def run(files, **deletes):
        catalog = SqlCatalog(
            "http_test",
            **{
                "uri": f"sqlite:///{tmp_path / 'catalog.sqlite'}",
                "warehouse": server.url + "/warehouse",
                "py-io-impl": "crowdb_tpc_loader.http_fileio.HttpFileIO",
                "http.auth-origins": server.url,
                "http.spool-directory": str(tmp_path / "spool"),
            },
        )
        backend = IcebergBackend(replace(options, delete_mode="equality", **deletes), redactor)
        backend.catalog = catalog
        backend.ensure_namespace()
        schema = arrow_schema("tpch", "region")
        parts = []
        for index, keys in enumerate(files):
            path = tmp_path / "region" / f"part-{index}.parquet"
            path.parent.mkdir(exist_ok=True)
            rows = {
                "r_regionkey": keys,
                "r_name": [f"n{key}" for key in keys],
                "r_comment": ["c"] * len(keys),
            }
            pq.write_table(pa.table(rows, schema=schema), path)
            parts.append(ParquetPart(path, len(keys), path.stat().st_size, schema))
        data = stamp_dataset({"region": TableData("region", tuple(parts), schema)}, lambda message: None)
        report.generated_table(data["region"], tmp_path)

        def upload(table, part, uri, buffer_size, emit):
            # Data and delete files alike: the URI is on disk as started before anything is sent.
            row = json.loads(report.paths[0].read_text())["tables"]["region"]
            journal = {
                item["remote_uri"]: item["state"] for item in row["files"] + row.get("delete_files", [])
            }
            assert journal[uri] == "upload_started"
            return upload_part(table, part, uri, buffer_size, emit)

        load_tables(backend, data, report, 65536, lambda message: None, uploader=upload, upload_workers=1)
        return report.table("region"), catalog.load_table((*options.namespace, "region"))

    return run


def commits(table):
    """Snapshot operations in commit order, and each live file's (content, sequence number, rows)."""
    snapshots = sorted(table.metadata.snapshots, key=lambda snapshot: snapshot.sequence_number)
    files = {}
    for manifest in table.current_snapshot().manifests(table.io):
        for entry in manifest.fetch_manifest_entry(table.io):
            item = entry.data_file
            name = item.file_path.rsplit("/", 1)[1]
            files[name] = (int(item.content), entry.sequence_number, item.record_count)
    return snapshots, files


def deleted_keys(tmp_path):
    return [
        pq.read_table(path)["r_regionkey"].to_pylist()
        for path in sorted((tmp_path / "region").glob("*deletes*.parquet"))
    ]


def test_rounds_interleave_data_and_delete_commits(load, tmp_path):
    row, table = load(
        [[0, 1, 2, 3], [0, 4, 5, 6], [2, 7]], delete_fraction=0.5, delete_commits=2, delete_file_rows=2
    )
    snapshots, files = commits(table)
    assert [snapshot.summary.operation.value for snapshot in snapshots] == ["append", "delete"] * 2
    # A delete applies to data with a lower sequence number: the round-1 delete of key 2 must
    # not reach part 2, so it sits before the second data commit.
    assert files == {
        "part-000000.parquet": (0, 1, 4),
        "delete-000000.parquet": (2, 2, 2),
        "part-000001.parquet": (0, 3, 4),
        "part-000002.parquet": (0, 3, 2),
        "delete-000001.parquet": (2, 4, 2),
        "delete-000002.parquet": (2, 4, 1),
    }
    # Round 1 holds keys 0..3; round 2 holds 0, 2, 4, 5, 6, 7, split at two rows per file.
    assert deleted_keys(tmp_path) == [[0, 2], [0, 4], [6]]
    ids = [snapshot.snapshot_id for snapshot in snapshots]
    assert [(item["snapshot_id"], item["delete_snapshot_id"]) for item in row["delete_rounds"]] == [
        (ids[0], ids[1]),
        (ids[2], ids[3]),
    ]
    assert (row["status"], row["snapshot_id"], row["delete_snapshot_id"]) == ("succeeded", ids[2], ids[3])
    assert all(item["state"] == "registered" for item in row["files"] + row["delete_files"])


def test_default_equality_shape_is_one_delete_file_per_data_file(load, tmp_path):
    row, table = load([[0, 1, 2, 3], [0, 4, 5, 6]], delete_fraction=0.5)
    snapshots, files = commits(table)
    assert [snapshot.summary.operation.value for snapshot in snapshots] == ["append", "delete"]
    assert files == {
        "part-000000.parquet": (0, 1, 4),
        "part-000001.parquet": (0, 1, 4),
        "delete-000000.parquet": (2, 2, 2),
        "delete-000001.parquet": (2, 2, 2),
    }
    # Every second row of each file by its key, not every second distinct key of the table.
    assert deleted_keys(tmp_path) == [[0, 2], [0, 5]]
    assert row["delete_snapshot_id"] == snapshots[1].snapshot_id and "delete_rounds" not in row
