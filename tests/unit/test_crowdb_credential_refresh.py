"""Exercise native Arrow S3 requests across a credential window over real HTTP."""

import io
import json
import pickle
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from crowdb_tpc_loader import file_credentials
from crowdb_tpc_loader.crowdb_fileio import CrowdbFileIO
from crowdb_tpc_loader.errors import CompatibilityError
from crowdb_tpc_loader.file_credentials import EXPIRY, REFRESH
from crowdb_tpc_loader.file_stream import BLOCK

PATH = "bucket/t/table/value.parquet"


@pytest.fixture
def grant_server(monkeypatch):
    state = SimpleNamespace(
        now=100_000,
        expires=105_000,
        data=b"PAR1" + b"x" * (BLOCK * 2) + b"PAR1",
        requests=[],
        expire_on_read=False,
        status=200,
        prefix="s3://bucket/t/table/",
        response_expiry=1_005_000,
        duplicate=False,
    )

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def respond(self, code, data=b"", headers=None):
            self.send_response(code)
            for key, value in (headers or {}).items():
                self.send_header(key, str(value))
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)

        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            token = self.headers.get("X-Amz-Security-Token")
            state.requests.append((self.command, self.path, token))
            if self.path.startswith("/v1/"):
                assert self.headers["Authorization"] == "Bearer catalog-token"
                config = {
                    "s3.access-key-id": "new-key",
                    "s3.secret-access-key": "new-secret",
                    "s3.session-token": "new-token",
                    EXPIRY: str(state.response_expiry),
                }
                entry = {"prefix": state.prefix, "config": config}
                body = json.dumps({"storage-credentials": [entry] * (2 if state.duplicate else 1)}).encode()
                self.respond(state.status, body, {"Location": state.url + "/must-not-follow"})
                return
            if self.path != "/" + PATH:
                raise AssertionError("Exact-object operations must not list the bucket: " + self.path)
            if token not in {"old-token", "new-token"} or (
                token == "old-token" and state.now >= state.expires
            ):
                self.respond(403, b"<Error><Code>AccessDenied</Code></Error>")
                return
            headers = {
                "Accept-Ranges": "bytes",
                "ETag": '"immutable-object"',
                "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            }
            if self.command == "HEAD":
                self.respond(200, state.data, headers)
                return
            start, end = self.headers["Range"].removeprefix("bytes=").split("-")
            start, end = int(start), int(end)
            headers["Content-Range"] = f"bytes {start}-{end}/{len(state.data)}"
            if state.expire_on_read:
                state.now = state.expires + 1
                state.expire_on_read = False
            self.respond(206, state.data[start : end + 1], headers)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setattr(file_credentials, "_now_ms", lambda: state.now)
    state.properties = {
        "uri": state.url,
        "token": "catalog-token",
        "http.timeout": "2",
        "client.region": "us-east-1",
        "s3.endpoint": state.url,
        "s3.access-key-id": "old-key",
        "s3.secret-access-key": "old-secret",
        "s3.session-token": "old-token",
        EXPIRY: str(state.expires),
        REFRESH: "/v1/namespaces/test/tables/table/credentials?table-id=stable",
    }
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_existing_open_stream_refreshes_between_bounded_native_reads(grant_server):
    state = grant_server
    state.expire_on_read = True
    fileio = CrowdbFileIO(state.properties)
    with fileio.new_input("s3://" + PATH).open() as stream:
        assert stream.read() == state.data
        stream.seek(-4, 2)
        assert stream.read(4) == b"PAR1"
        assert stream.read() == b""
    refreshes = [item for item in state.requests if item[1].startswith("/v1/")]
    assert len(refreshes) == 1
    assert any(item[0] == "GET" and item[2] == "new-token" for item in state.requests)


def test_arrow_scan_and_pickled_fileio_refresh_without_reloading_table(grant_server):
    state = grant_server
    expected = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    data = io.BytesIO()
    pq.write_table(expected, data)
    state.data = data.getvalue()
    fileio = CrowdbFileIO(state.properties)
    dataset = ds.dataset([PATH], filesystem=fileio.fs_by_scheme("s3", "bucket"), format="parquet")
    state.now = state.expires + 1
    assert dataset.to_table().equals(expected)
    independent = pickle.loads(pickle.dumps(fileio))
    with independent.new_input("s3://" + PATH).open() as stream:
        assert stream.read(4) == b"PAR1"
    assert sum(item[1].startswith("/v1/") for item in state.requests) == 2


@pytest.mark.parametrize(
    "status,error", [(401, PermissionError), (403, PermissionError), (503, OSError), (302, OSError)]
)
def test_refresh_failure_is_reported_without_old_grant_fallback_or_redirect(grant_server, status, error):
    state = grant_server
    state.now, state.status = state.expires + 1, status
    with pytest.raises(error, match=f"HTTP {status}"):
        CrowdbFileIO(state.properties).new_input("s3://" + PATH).open()
    assert len(state.requests) == 1
    assert state.requests[0][1].startswith("/v1/")


@pytest.mark.parametrize("change", ["foreign", "expired", "duplicate"])
def test_invalid_refresh_is_rejected_before_object_request(grant_server, change):
    state = grant_server
    state.now = state.expires + 1
    if change == "foreign":
        state.prefix = "s3://bucket/t/another/"
    elif change == "expired":
        state.response_expiry = state.now - 1
    else:
        state.duplicate = True
    with pytest.raises(CompatibilityError):
        CrowdbFileIO(state.properties).new_input("s3://" + PATH).open()
    assert len(state.requests) == 1


def test_cross_origin_refresh_is_rejected_before_credentials_leave_catalog(grant_server):
    state = grant_server
    state.properties[REFRESH] = "http://127.0.0.1:1/credentials"
    with pytest.raises(CompatibilityError, match="catalog origin"):
        CrowdbFileIO(state.properties).new_input("s3://" + PATH)
    assert not state.requests


@pytest.mark.parametrize("fail_scan", [False, True])
def test_limited_iceberg_scan_closes_native_reader_before_releasing_file(
    grant_server, tmp_path, monkeypatch, fail_scan
):
    from pyiceberg.catalog.sql import SqlCatalog
    from crowdb_tpc_loader.file_stream import CredentialReader

    state = grant_server
    expected = pa.table({"id": range(200_000), "value": ["scan-lifetime"] * 200_000})
    data = io.BytesIO()
    pq.write_table(expected, data, row_group_size=8192)
    state.data = data.getvalue()
    properties = {**state.properties, "py-io-impl": "crowdb_tpc_loader.crowdb_fileio.CrowdbFileIO"}
    catalog = SqlCatalog(
        "lifetime", uri=f"sqlite:///{tmp_path / 'catalog.db'}", warehouse=str(tmp_path / "warehouse")
    )
    catalog.create_namespace("example")
    table = catalog.create_table(("example", "rows"), schema=expected.schema, properties=properties)
    table.add_files(["s3://" + PATH])
    closed = []
    scanner_type = ds.Scanner
    close_file = CredentialReader.close

    def record_file_close(self):
        if not self.closed:
            closed.append("file")
        return close_file(self)

    monkeypatch.setattr(CredentialReader, "close", record_file_close)

    class Reader:
        def __init__(self, reader):
            self.reader = reader

        def __iter__(self):
            if fail_scan:
                raise OSError("injected scan failure")
            yield from self.reader

        def close(self):
            closed.append("reader-start")
            self.reader.close()
            closed.append("reader-end")

    class Scanner:
        def __init__(self, scanner):
            self.scanner = scanner

        @staticmethod
        def from_fragment(*args, **kwargs):
            return Scanner(scanner_type.from_fragment(*args, **kwargs))

        def to_batches(self):
            return self.scanner.to_batches()

        def to_reader(self):
            return Reader(self.scanner.to_reader())

    monkeypatch.setattr(ds, "Scanner", Scanner)
    if fail_scan:
        with pytest.raises(OSError, match="injected scan failure"):
            table.scan(limit=10).to_arrow()
    else:
        assert table.scan(limit=10).to_arrow().equals(expected.slice(0, 10))
    assert closed.count("reader-start") == closed.count("reader-end") == closed.count("file") == 1
    assert closed.index("reader-start") < closed.index("file"), "native reader must own file shutdown"
