from types import SimpleNamespace

import pytest

from crowdb_tpc_loader.backend import data_location
from crowdb_tpc_loader.errors import CompatibilityError


@pytest.mark.parametrize("suffix", ["../foreign.parquet", "%2E%2E/foreign.parquet", "/foreign.parquet"])
def test_native_provider_rejects_traversal_before_upload(suffix):
    base = "s3://iceberg/" + "/".join(["1" * 32, "2" * 32, "3" * 32])
    table = SimpleNamespace(
        location=lambda: base,
        location_provider=lambda: SimpleNamespace(new_data_location=lambda _: base + "/" + suffix),
    )
    with pytest.raises(CompatibilityError, match="outside the native table scope"):
        data_location(table, "file.parquet")


def test_native_provider_rejects_foreign_prefix_and_accepts_scoped_unicode():
    base = "s3://iceberg/" + "/".join(["1" * 32, "2" * 32, "3" * 32])
    table = SimpleNamespace(
        location=lambda: base,
        location_provider=lambda: SimpleNamespace(new_data_location=lambda _: base + "-foreign/data.parquet"),
    )
    with pytest.raises(CompatibilityError, match="outside the native table scope"):
        data_location(table, "file.parquet")
    table.location_provider = lambda: SimpleNamespace(new_data_location=lambda _: base + "/data/雪.parquet")
    assert data_location(table, "file.parquet") == base + "/data/雪.parquet"
