"""Real local HTTP protocol acceptance; no storage-container claims."""

import base64
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
import json
import socket
import struct
from threading import Thread
from types import SimpleNamespace

import pytest

from crowdb_tpc_loader.dataset_load import DatasetRunner
from crowdb_tpc_loader.dataset_data import generate_dataset, validate_dataset_input
from crowdb_tpc_loader.dataset_wire import verify_stream
from crowdb_tpc_loader.dataset_http import DatasetHTTP
from crowdb_tpc_loader.dataset_verify import verify_dataset
from crowdb_tpc_loader.errors import LoadError
from crowdb_tpc_loader.security import Redactor


def frame(kind, value=b""):
    return bytes([kind]) + struct.pack(">I", len(value)) + value


def batch_bytes(snapshot, samples, payloads):
    fields, bodies = [], []
    for sample in samples:
        values = {name: base64.b64decode(value["data"]) for name, value in sample["fields"].items()}
        values["payload"] = payloads[sample["id"]]
        for name, body in values.items():
            fields.append(
                {
                    "sample": sample["id"],
                    "field": name,
                    "state": "value",
                    "length": len(body),
                    "md5": hashlib.md5(body).hexdigest(),
                }
            )
            bodies.append(body)
    header = {
        "snapshot": snapshot,
        "sample_ids": [s["id"] for s in samples],
        "fields": fields,
        "next": None,
        "end": True,
    }
    result = b"CDBDS001" + frame(1, json.dumps(header).encode())
    for body in bodies:
        for offset in range(0, len(body), 7):
            chunk = body[offset : offset + 7]
            result += frame(2, hashlib.md5(chunk).digest() + chunk)
        result += frame(3)
    return result + frame(0)


@contextmanager
def dataset_server(mode=None):
    state = {
        "calls": [],
        "uploads": {},
        "manifest": None,
        "published": False,
        "id": "d" * 32,
        "snapshot": "e" * 32,
    }

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def handle_request(self):
            assert self.headers.get("Authorization") == "Bearer protocol-test-secret"
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            relative = self.path.split("/datasets/training", 1)[1]
            state["calls"].append((self.command, relative))
            value = (
                json.loads(body) if body and self.headers.get("Content-Type") == "application/json" else body
            )
            reply = None
            if relative == "":
                if self.command == "PUT":
                    state["created"] = True
                reply = {
                    "id": state["id"],
                    "name": "training",
                    "namespace": "dataset-ns",
                    "latest": state["snapshot"] if state["published"] else None,
                    "stable": None,
                }
            elif relative == "/uploads":
                reply = {**value, "state": "open", "parts": [], "expires_at": 9999999999}
                state["uploads"][value["id"]] = {**reply, "data": b""}
            elif relative.startswith("/uploads/"):
                identity = relative.split("/")[2]
                upload = state["uploads"][identity]
                if "/parts/" in relative:
                    part = int(relative.rsplit("/", 1)[1])
                    assert part == len(upload["parts"])
                    assert len(body) <= upload["part_size"]
                    reply = {"index": part, "length": len(body), "md5": hashlib.md5(body).hexdigest()}
                    upload["parts"].append(reply)
                    upload["data"] += body
                else:
                    assert len(upload["data"]) == upload["length"]
                    assert hashlib.md5(upload["data"]).hexdigest() == upload["md5"]
                    upload["state"] = "complete"
                    reply = {key: value for key, value in upload.items() if key != "data"}
            elif relative in {"/snapshots/prepare", "/snapshots/publish"}:
                if relative.endswith("prepare"):
                    state["manifest"], state["operation"] = value["manifest"], value["operation"]
                    state["manifest"]["schema"] = sorted(
                        state["manifest"]["schema"], key=lambda field: field["name"]
                    )
                else:
                    assert value["manifest"]["samples"] == state["manifest"]["samples"]
                    assert value["operation"] == state["operation"]
                    state["published"] = mode != "unknown"
                    if mode in {"lost_reply", "unknown"}:
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                reply = {
                    "id": state["snapshot"],
                    "operation": state["operation"],
                    "parent": None,
                    "state": "published",
                    "is_latest": state["published"],
                }
            elif relative.startswith("/operations/") or relative == "/snapshots/" + state["snapshot"]:
                reply = {
                    "id": state["snapshot"],
                    "operation": state["operation"],
                    "parent": None,
                    "state": "published",
                    "is_latest": state["published"],
                }
            elif relative.endswith("/manifest"):
                reply = state["manifest"]
            elif relative == "/reads":
                selected = [
                    sample for sample in state["manifest"]["samples"] if sample["id"] in value["sample_ids"]
                ]
                payloads = {
                    s["id"]: state["uploads"][s["fields"]["payload"]["upload"]]["data"] for s in selected
                }
                selected = [
                    {"id": s["id"], "fields": {k: v for k, v in s["fields"].items() if k != "payload"}}
                    for s in selected
                ]
                encoded = batch_bytes(state["snapshot"], selected, payloads)
                if mode == "truncate":
                    encoded = encoded[:-5]
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.crowdb.dataset-batch.v1")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return
            else:
                raise AssertionError(relative)
            encoded = json.dumps(reply).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        do_GET = do_POST = do_PUT = handle_request

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def load(root, origin, **overrides):
    options = dict(
        dataset_uri=origin,
        namespace="dataset-ns",
        name="training",
        token="protocol-test-secret",
        timeout=2,
        samples=3,
        payload_bytes=19,
        seed=42,
        part_size=5,
        upload_workers=2,
        input_dir=None,
        work_dir=root / "work",
        report_file=root / "report.json",
        verify=True,
        keep_files=False,
    )
    options.update(overrides)
    return DatasetRunner(SimpleNamespace(**options), lambda _: None, Redactor(["protocol-test-secret"])).run()


def test_full_http_multipart_publication_and_hash_readback(tmp_path):
    with dataset_server() as (origin, state):
        result = load(tmp_path, origin)
    assert result.exit_code == 0, result.report
    assert result.report["verification"]["samples"] == 3
    assert result.report["verification"]["fields"] == 9
    assert all(len(upload["parts"]) == 4 for upload in state["uploads"].values())
    assert result.report["work_directory_removed"]
    assert "protocol-test-secret" not in (tmp_path / "report.json").read_text()


def test_lost_publication_reply_is_read_only_reconciled_once(tmp_path):
    with dataset_server("lost_reply") as (origin, state):
        result = load(tmp_path, origin)
    assert result.exit_code == 0, result.report
    assert result.report["publication_reconciled"]
    assert state["calls"].count(("POST", "/snapshots/publish")) == 1
    assert sum(path.startswith("/operations/") for _, path in state["calls"]) == 1


def test_unresolved_publication_keeps_report_and_data_without_replay(tmp_path):
    with dataset_server("unknown") as (origin, state):
        result = load(tmp_path, origin)
    assert result.exit_code != 0
    assert result.report["publication_state"] == "unknown"
    assert not result.report.get("work_directory_removed")
    assert (tmp_path / "report.json").is_file()
    assert state["calls"].count(("POST", "/snapshots/publish")) == 1


def test_prepared_resource_readback_does_not_confirm_publication(tmp_path):
    with dataset_server("unknown") as (origin, state):
        result = load(tmp_path, origin)
        client = DatasetHTTP(origin, "dataset-ns", "training", "protocol-test-secret", 2)
        try:
            proof = verify_dataset(client, result.report)
        finally:
            client.close()
    assert proof["samples"] == 3 and proof["is_latest"] is False
    assert result.report["publication_state"] == "unknown"
    assert state["calls"].count(("POST", "/snapshots/publish")) == 1


def test_truncated_remote_read_is_failure_after_confirmed_publication(tmp_path):
    with dataset_server("truncate") as (origin, _):
        result = load(tmp_path, origin)
    assert result.exit_code != 0
    assert result.report["publication_state"] == "published"
    assert not result.report.get("work_directory_removed")


def test_changed_local_input_fails_before_dataset_creation(tmp_path):
    source = generate_dataset(tmp_path / "source", samples=1, payload_bytes=12)
    (source / "payload/00000000.bin").write_bytes(b"changed-data")
    with dataset_server() as (origin, state):
        result = load(tmp_path, origin, input_dir=source)
    assert result.exit_code != 0 and state["calls"] == []


def test_wire_rejects_wrong_snapshot_extra_bytes_and_field_corruption(tmp_path):
    root = generate_dataset(tmp_path / "source", samples=1, payload_bytes=11)
    _, inventory = validate_dataset_input(root)
    samples = inventory["samples"]
    data = {s["id"]: (root / s["payload"]["path"]).read_bytes() for s in samples}
    wire = batch_bytes("e" * 32, samples, data)
    assert verify_stream(BytesIO(wire), "e" * 32, samples)["samples"] == 1
    for changed, snapshot in [(wire, "f" * 32), (wire + b"trailing", "e" * 32), (wire[:-5], "e" * 32)]:
        with pytest.raises(LoadError):
            verify_stream(BytesIO(changed), snapshot, samples)
    data[samples[0]["id"]] = b"x" * 11
    with pytest.raises(LoadError, match="metadata|checksum"):
        verify_stream(BytesIO(batch_bytes("e" * 32, samples, data)), "e" * 32, samples)
