import hashlib
import io
import json
import uuid
from types import SimpleNamespace
from urllib.parse import urlsplit, parse_qs

import pytest

from crowdb_tpc_loader import native_upload
from crowdb_tpc_loader.errors import LoadError


def test_native_upload_identity_is_stable_uri_input_and_signed_request_returns_one_location(
    tmp_path, monkeypatch
):
    identity = native_upload.new_upload_id()
    assert uuid.UUID(identity).version == 7
    ids = "/".join(["1" * 32, "2" * 32, "3" * 32])
    uri = f"s3://iceberg/{ids}/data/part-{identity}.parquet"
    location = f"s3://iceberg/{ids}/f/v1/part.locator.parquet"
    path = tmp_path / "part.parquet"
    path.write_bytes(b"validated parquet bytes")
    properties = {
        "s3.endpoint": "http://native.example",
        "s3.access-key-id": "test-access",
        "s3.secret-access-key": "test-secret",
        "s3.session-token": "test-session",
    }
    sent = []

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def put(self, url, **kwargs):
            sent.append(url)
            assert parse_qs(urlsplit(url).query)["crowdb-native-upload"] == [identity]
            assert kwargs["allow_redirects"] is False
            headers = native_upload.requests.structures.CaseInsensitiveDict(kwargs["headers"])
            assert headers["x-amz-content-sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
            assert "AWS4-HMAC-SHA256" in kwargs["headers"]["Authorization"]
            assert kwargs["data"].read() == path.read_bytes()
            return SimpleResponse()

    class SimpleResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        raw = io.BytesIO(json.dumps({"location": location}).encode())

    monkeypatch.setattr(native_upload.requests, "Session", Session)
    result = native_upload.upload(properties, uri, path, path.stat().st_size, 7, lambda total: None)
    assert result[0] == location
    assert result[1] == hashlib.md5(path.read_bytes()).hexdigest()
    assert len(sent) == 1


def test_native_upload_validates_owner_identity_and_request_limit_before_transfer(tmp_path):
    path = tmp_path / "part.parquet"
    path.write_bytes(b"data")
    properties = {"crowdb.native-injection-max-bytes": "1"}
    identity = native_upload.new_upload_id()
    uri = "s3://iceberg/" + "/".join(["1" * 32, "2" * 32, "3" * 32]) + f"/data/part-{identity}.parquet"
    with pytest.raises(LoadError, match="exceeds"):
        native_upload.upload(properties, uri, path, 4, 7, lambda total: None)
    with pytest.raises(LoadError, match="identity"):
        native_upload.upload(
            properties, uri.replace(identity, str(uuid.uuid4())), path, 4, 7, lambda total: None
        )
    assert not native_upload.enabled(SimpleNamespace())
