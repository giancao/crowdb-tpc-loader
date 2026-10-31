"""Standalone S3 side effects use explicit local generator/client doubles."""

import base64
from dataclasses import replace
import hashlib
from io import BytesIO
import json
from types import SimpleNamespace

import pytest

from crowdb_tpc_loader import cli
from crowdb_tpc_loader.errors import ArgumentError
from crowdb_tpc_loader.s3_target import S3Runner
from crowdb_tpc_loader.schemas import inventory


class FakeS3:
    def __init__(self, report_path, *, occupied=False, lose_reply=False, corrupt=False):
        self.objects = {}
        self.report_path = report_path
        self.occupied, self.lose_reply, self.corrupt = occupied, lose_reply, corrupt
        self.calls = []

    def head_bucket(self, **kwargs):
        self.calls.append("head_bucket")

    def list_objects_v2(self, **kwargs):
        self.calls.append("list")
        return {"Contents": [{"Key": "existing"}]} if self.occupied else {}

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        before = json.loads(self.report_path.read_text())
        assert "IfNoneMatch" not in kwargs
        assert key.startswith("fresh/" + before["run_id"] + "/")
        body = kwargs["Body"]
        body = body.read() if hasattr(body, "read") else body
        assert len(body) == kwargs["ContentLength"]
        assert base64.b64encode(hashlib.md5(body).digest()).decode() == kwargs["ContentMD5"]
        if key.endswith(".parquet"):
            files = [part for row in before["tables"].values() for part in row["files"]]
            assert any(
                part["state"] == "upload_started" and part["remote_uri"].endswith(key) for part in files
            )
        else:
            assert before["phase"] in {"reserving-prefix", "publishing-inventory"}
        self.objects[key] = body
        self.calls.append(key)
        if self.lose_reply and key.endswith(".parquet"):
            raise OSError("upload response lost")

    def get_object(self, **kwargs):
        body = self.objects[kwargs["Key"]]
        return {"Body": BytesIO(body + b"bad" if self.corrupt else body)}


def runner(options, redactor, make_data, client, *, generation_fails=False):
    def generated(generate_options, emit, redactor):
        def run():
            if generation_fails:
                return SimpleNamespace(exit_code=3, report={"errors": ["explicit generator failure"]})
            root = generate_options.output_dir
            data = make_data(inventory(generate_options.benchmark), root=root / "data")
            tables = {}
            for name, table in data.items():
                tables[name] = {
                    "status": "succeeded",
                    "row_count": table.rows,
                    "size_bytes": table.size_bytes,
                    "file_count": 1,
                    "files": [
                        {
                            "local_path": str(table.parts[0].path.relative_to(root)),
                            "size_bytes": table.size_bytes,
                            "row_count": table.rows,
                            "state": "local",
                            "remote_uri": None,
                        }
                    ],
                }
            return SimpleNamespace(
                exit_code=0,
                report={"generator": {"implementation": "explicit-test-double"}, "tables": tables},
            )

        return SimpleNamespace(run=run)

    transport = SimpleNamespace(bind=lambda _: client, close=lambda: None)
    return S3Runner(
        replace(
            options,
            target="s3",
            s3_bucket="test-bucket",
            s3_prefix="fresh",
            s3_endpoint="http://s3.test",
            s3_properties={},
            verify=True,
        ),
        lambda _: None,
        redactor,
        generator_runner=generated,
        transport_factory=lambda **_: transport,
    )


def test_s3_full_generation_journal_upload_and_readback(options, redactor, make_data, tmp_path):
    client = FakeS3(options.report_file)
    result = runner(options, redactor, make_data, client).run()
    assert result.exit_code == 0, result.report
    assert result.report["target"] == "s3"
    assert result.report["requested_s3_prefix"] == "fresh"
    assert result.report["s3_prefix"] == "fresh/" + result.report["run_id"]
    assert result.report["inventory_state"] == "stored"
    assert result.report["summary"]["uncertain_uploads"] == []
    assert result.report["summary"]["unregistered_uploads"] == []
    assert len([key for key in client.objects if key.endswith(".parquet")]) == 8
    assert not any(key.endswith(".avro") for key in client.objects)
    assert result.report["work_directory_removed"]
    assert options.work_dir.is_dir()


def test_generation_failure_creates_no_remote_objects(options, redactor, make_data):
    client = FakeS3(options.report_file)
    result = runner(options, redactor, make_data, client, generation_fails=True).run()
    assert result.exit_code and not client.objects
    assert result.report["work_directory"]


def test_occupied_prefix_does_not_generate_or_mutate(options, redactor, make_data):
    client = FakeS3(options.report_file, occupied=True)
    result = runner(options, redactor, make_data, client).run()
    assert result.exit_code and not client.objects
    assert result.report["work_directory"] is None


def test_lost_upload_reply_retains_unknown_keys_and_local_files(options, redactor, make_data):
    client = FakeS3(options.report_file, lose_reply=True)
    result = runner(options, redactor, make_data, client).run()
    assert result.exit_code
    assert len(result.report["summary"]["uncertain_uploads"]) == 8
    assert not result.report.get("work_directory_removed")
    assert not any(key.endswith("inventory.json") for key in client.objects)


def test_readback_integrity_failure_is_not_success(options, redactor, make_data):
    client = FakeS3(options.report_file, corrupt=True)
    result = runner(options, redactor, make_data, client).run()
    assert result.exit_code
    assert not result.report.get("work_directory_removed")
    assert not any(key.endswith("inventory.json") for key in client.objects)


def test_s3_options_do_not_need_catalog_and_hide_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "example-access-value")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "example-private-value")
    argv = [
        "load",
        "--benchmark",
        "tpch",
        "--target",
        "s3",
        "--s3-endpoint",
        "http://s3.test",
        "--s3-bucket",
        "test-bucket",
        "--s3-prefix",
        "test/run",
        "--verify",
    ]
    redactor = cli.initial_redactor(argv)
    options = cli.to_options(cli.build_parser(redactor).parse_args(argv), redactor)
    assert options.target == "s3" and options.catalog_uri is None
    assert options.verify
    assert "example-private-value" not in repr(options)
    assert "example-private-value" not in redactor.text("error example-private-value")
    for prefix in ["../escape", "/root", "empty//key"]:
        changed = argv.copy()
        changed[changed.index("--s3-prefix") + 1] = prefix
        with pytest.raises(ArgumentError):
            cli.to_options(cli.build_parser(redactor).parse_args(changed), redactor)
