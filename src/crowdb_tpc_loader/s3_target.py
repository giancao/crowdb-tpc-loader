"""Generate complete TPC data and journal standalone S3 uploads into owned keys."""

from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time
import uuid

from .errors import ArgumentError, LoadError
from .journal_workers import journal_workers
from .report import RunReport
from .runner import RunResult, Runner
from .schemas import inventory
from .s3_upload import file_checksums, upload_file
from .upload_client import UploadClient
from .util import cleanup_owned, create_work_directory, mark_owned


def _put_json(client, bucket, key, value):
    body = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentLength=len(body),
        ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode(),
    )


def _verify(client, bucket, key, length, digest, buffer_size):
    response = client.get_object(Bucket=bucket, Key=key)
    stream, size, md5 = response["Body"], 0, hashlib.md5()
    try:
        while chunk := stream.read(buffer_size):
            size += len(chunk)
            if size > length:
                raise LoadError("S3 object exceeds validated file length")
            md5.update(chunk)
    finally:
        stream.close()
    if size != length or md5.hexdigest() != digest:
        raise LoadError("Standalone S3 readback length/checksum mismatch")


class S3Runner:
    def __init__(self, options, emit, redactor, generator_runner=Runner, transport_factory=UploadClient):
        self.options, self.emit, self.redactor = options, emit, redactor
        self.generator_runner, self.transport_factory = generator_runner, transport_factory
        self.report = RunReport(options, uuid.uuid4().hex, redactor)
        self.requested_prefix = options.s3_prefix
        self.options = replace(options, s3_prefix=options.s3_prefix + "/" + self.report.data["run_id"])
        self.report.data.update(
            target="s3",
            s3_endpoint=options.s3_endpoint,
            s3_bucket=options.s3_bucket,
            requested_s3_prefix=options.s3_prefix,
            s3_prefix=self.options.s3_prefix,
        )
        self.work, self.report_path = None, None

    def _prepare(self, client):
        options, report = self.options, self.report
        if options.report_file and (options.report_file.exists() or options.report_file.is_symlink()):
            raise ArgumentError("--report-file already exists; choose a new file")
        client.head_bucket(Bucket=options.s3_bucket)
        occupied = client.list_objects_v2(
            Bucket=options.s3_bucket, Prefix=self.requested_prefix + "/", MaxKeys=1
        )
        if occupied.get("Contents"):
            raise LoadError("Standalone S3 prefix is occupied; choose a new prefix")
        self.work = create_work_directory(options.work_dir, report.data["run_id"])
        mark_owned(self.work, report.data["run_id"])
        report_path = (
            options.report_file or self.work.parent / f"crowdb-tpc-{report.data['run_id']}.json"
        ).absolute()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("x"):
            pass
        self.report_path = report_path
        report.paths = [self.report_path, self.work / "run-summary.json"]
        report.data.update(report_file=str(self.report_path), work_directory=str(self.work))
        report.save()
        source = self.work / "source"
        generated = self.generator_runner(
            replace(options, command="generate", target="iceberg", output_dir=source, report_file=None),
            self.emit,
            self.redactor,
        ).run()
        if generated.exit_code:
            raise LoadError(
                "Local TPC generation/validation failed: " + "; ".join(generated.report.get("errors", []))
            )
        if set(generated.report["tables"]) != set(inventory(options.benchmark)) or any(
            row["status"] != "succeeded" for row in generated.report["tables"].values()
        ):
            raise LoadError("Generation did not validate the complete TPC table inventory")
        report.data["generator"] = generated.report["generator"]
        for name, value in generated.report["tables"].items():
            row = deepcopy(value)
            row["status"] = "generated"
            for item in row["files"]:
                item["local_path"] = "source/" + item["local_path"]
            report.data["tables"][name] = row
        report.data.update(
            phase="reserving-prefix",
            prefix_state="unknown",
            owner_uri=f"s3://{options.s3_bucket}/{options.s3_prefix}/_crowdb-tpc-owner.json",
        )
        report.save()
        _put_json(
            client,
            options.s3_bucket,
            options.s3_prefix + "/_crowdb-tpc-owner.json",
            {
                "run_id": report.data["run_id"],
                "benchmark": options.benchmark,
                "scale_factor": str(options.sf),
            },
        )
        report.data["prefix_state"] = "owned"
        report.phase("uploading")

    def _table(self, client, name, value, checkpoint):
        options, row = self.options, deepcopy(value)
        row["status"] = "uploading"
        try:
            for index, item in enumerate(row["files"]):
                path = self.work / item["local_path"]
                started = time.monotonic()
                size, digest, part_digests = file_checksums(
                    path, item["size_bytes"], options.upload_buffer_mib * 1024**2
                )
                if size != item["size_bytes"]:
                    raise LoadError("Validated local file changed size")
                key = f"{options.s3_prefix}/{name}/part-{index:06d}.parquet"
                item.update(
                    remote_uri=f"s3://{options.s3_bucket}/{key}",
                    state="upload_started",
                    md5=digest,
                    md5_duration_seconds=time.monotonic() - started,
                )
                checkpoint(row)
                started = time.monotonic()
                upload_file(
                    options.s3_properties,
                    item["remote_uri"],
                    path,
                    size,
                    lambda _: None,
                    md5=digest,
                    client=client,
                    part_digests=part_digests,
                    buffer_size=options.upload_buffer_mib * 1024**2,
                    create_only=False,
                )
                item.update(state="stored", transfer_duration_seconds=time.monotonic() - started)
                checkpoint(row)
                if options.verify:
                    started = time.monotonic()
                    _verify(client, options.s3_bucket, key, size, digest, options.upload_buffer_mib * 1024**2)
                    item.update(state="verified", verification_duration_seconds=time.monotonic() - started)
                    checkpoint(row)
            row["status"] = "succeeded"
            checkpoint(row)
            self.emit(f"Stored {name}: {row['row_count']} rows, {row['file_count']} file(s)")
        except BaseException as exc:
            unknown = any(item.get("state") == "upload_started" for item in row["files"])
            row.update(status="uncertain" if unknown else "failed", error=str(exc))
            checkpoint(row)
            raise

    def run(self):
        report, transport, code = self.report, None, 0
        try:
            transport = self.transport_factory(
                timeout=self.options.timeout,
                connections=min(self.options.upload_workers, 8),
                retry_attempts=0,
            )
            client = transport.bind(self.options.s3_properties)
            self._prepare(client)

            def save(name, row):
                report.data["tables"][name] = row
                report.save()

            journal_workers(
                list(report.data["tables"].items()),
                lambda name, value, checkpoint: self._table(client, name, value, checkpoint),
                save,
                min(self.options.upload_workers, 8),
            )
            report.data.update(
                phase="publishing-inventory",
                inventory_state="unknown",
                inventory_uri=f"s3://{self.options.s3_bucket}/{self.options.s3_prefix}/_crowdb-tpc-inventory.json",
            )
            report.save()
            _put_json(
                client,
                self.options.s3_bucket,
                self.options.s3_prefix + "/_crowdb-tpc-inventory.json",
                {"run_id": report.data["run_id"], "tables": report.data["tables"]},
            )
            report.data.update(phase="complete", inventory_state="stored")
            report.finish("succeeded")
            if not self.options.keep_files:
                report.paths = [path for path in report.paths if not path.is_relative_to(self.work)]
                cleanup_owned(self.work, report.data["run_id"])
                report.data["work_directory_removed"] = True
                report.save()
        except BaseException as exc:
            code = 130 if isinstance(exc, (KeyboardInterrupt, SystemExit)) else getattr(exc, "exit_code", 1)
            try:
                report.finish("interrupted" if code == 130 else "failed", str(exc))
            except Exception as save_error:
                report.data["warnings"].append(f"Report save failed: {save_error}")
        finally:
            if transport is not None:
                transport.close()
        return RunResult(code, self.redactor.data(report.data), self.report_path)
