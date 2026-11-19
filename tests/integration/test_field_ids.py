"""Field-ID stamping on real Parquet files."""

from decimal import Decimal

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from crowdb_tpc_loader.errors import ValidationError
from crowdb_tpc_loader.field_ids import schema_field_ids, stamp_dataset, stamp_part
from crowdb_tpc_loader.models import ParquetPart, TableData

pytestmark = pytest.mark.integration


def write(path, groups=3):
    schema = pa.schema(
        [
            pa.field("k", pa.int64()),
            pa.field("price", pa.decimal128(15, 2)),
            pa.field("name", pa.string()),
        ]
    )
    table = pa.table(
        {
            "k": list(range(30)),
            "price": [Decimal(index) / 100 for index in range(30)],
            "name": [f"n{index}" for index in range(30)],
        },
        schema=schema,
    )
    pq.write_table(
        table, path, row_group_size=30 // groups, compression="zstd", store_decimal_as_integer=True
    )
    return table, ParquetPart(path, 30, path.stat().st_size, schema)


def test_stamp_keeps_rows_layout_and_encoding(tmp_path):
    source, part = write(tmp_path / "t.parquet")
    stamped = stamp_part(part, "t")
    footer = pq.read_metadata(part.path)
    assert schema_field_ids(footer.schema.to_arrow_schema()) == {"k": 1, "price": 2, "name": 3}
    assert schema_field_ids(stamped.schema) == {"k": 1, "price": 2, "name": 3}
    assert footer.num_row_groups == 3 and footer.num_rows == 30
    assert footer.row_group(0).column(0).compression == "ZSTD"
    assert footer.schema.column(1).physical_type == "INT64"
    assert stamped.size_bytes == part.path.stat().st_size
    assert pq.read_table(part.path).replace_schema_metadata().equals(source, check_metadata=False)
    assert not list(tmp_path.glob("*.tmp"))


def test_stamp_dataset_returns_final_files(tmp_path):
    _, part = write(tmp_path / "t.parquet", groups=1)
    result = stamp_dataset({"t": TableData("t", (part,), part.schema)}, lambda message: None)
    assert schema_field_ids(result["t"].schema) == {"k": 1, "price": 2, "name": 3}
    assert result["t"].parts[0].size_bytes == part.path.stat().st_size


def test_unreadable_file_fails_and_leaves_no_temporary(tmp_path):
    path = tmp_path / "t.parquet"
    path.write_bytes(b"not parquet")
    with pytest.raises(ValidationError, match="cannot write field IDs"):
        stamp_part(ParquetPart(path, 1, path.stat().st_size, None), "t")
    assert path.read_bytes() == b"not parquet"
    assert not list(tmp_path.glob("*.tmp"))
