"""Fresh Dataset loading with durable identities and explicit uncertain outcomes."""

from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
import time
import uuid

from .dataset_data import generate_dataset, open_payload, validate_counts, validate_dataset_input
from .dataset_http import DatasetHTTP
from .dataset_verify import verify_dataset, verify_manifest
from .errors import ArgumentError, CommitUncertainError, LoadError
from .journal_workers import journal_workers
from .report import atomic_json, timestamp
from .runner import RunResult
from .util import cleanup_owned, create_work_directory, mark_owned


class DatasetRunner:
    def __init__(self, options, emit, redactor, client_factory=DatasetHTTP):
        self.options, self.emit, self.redactor, self.client_factory = options, emit, redactor, client_factory
        self.work, self.input, self.report_path = None, None, None
        self.data = {
            "report_format_version": 1,
            "target": "dataset",
            "command": "load",
            "run_id": uuid.uuid4().hex,
            "dataset_uri": options.dataset_uri,
            "namespace": options.namespace,
            "name": options.name,
            "status": "running",
            "phase": "preflight",
            "started_at": timestamp(),
            "uploads": {},
            "errors": [],
        }

    def save(self):
        self.data["updated_at"] = timestamp()
        if self.report_path:
            atomic_json(self.report_path, self.redactor.data(self.data))

    def _input(self):
        options = self.options
        if not 1 <= options.part_size <= 4 * 1024**2 or not 1 <= options.upload_workers <= 8:
            raise ArgumentError("Dataset requires part size 1–4194304 and 1–8 upload workers")
        if options.report_file and (options.report_file.exists() or options.report_file.is_symlink()):
            raise ArgumentError("Report path already exists; choose a new file")
        if options.input_dir:
            self.input, inventory = validate_dataset_input(options.input_dir)
        else:
            validate_counts(options.samples, options.payload_bytes, options.seed)
            self.work = create_work_directory(options.work_dir, self.data["run_id"])
            mark_owned(self.work, self.data["run_id"])
            self.input = generate_dataset(
                self.work / "data", options.samples, options.payload_bytes, options.seed
            )
            self.input, inventory = validate_dataset_input(self.input)
        if any(
            (sample["payload"]["length"] + options.part_size - 1) // options.part_size > 10_000
            for sample in inventory["samples"]
        ):
            raise ArgumentError("Dataset payload exceeds the selected part size's 10000-part limit")
        default = (
            self.work.parent if self.work else Path.cwd()
        ) / f"crowdb-dataset-{self.data['run_id']}.json"
        report_path = (options.report_file or default).expanduser().absolute()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("x"):
            pass
        self.report_path = report_path
        self.data.update(
            report_file=str(self.report_path),
            input_directory=str(self.input),
            work_directory=str(self.work) if self.work else None,
            inventory=inventory,
            sample_count=inventory["sample_count"],
            payload_bytes=inventory["sample_count"] * inventory["payload_bytes"],
        )
        self.data["uploads"] = {
            sample["id"]: {"id": uuid.uuid4().hex, "state": "planned"} for sample in inventory["samples"]
        }
        self.save()

    def _upload(self, client, sample, checkpoint):
        row = deepcopy(self.data["uploads"][sample["id"]])
        payload = sample["payload"]
        client = client.fork()
        try:
            row["state"] = "creating"
            checkpoint(row)
            request = {
                "id": row["id"],
                "length": payload["length"],
                "part_size": self.options.part_size,
                "md5": payload["md5"],
            }
            created = client.call("POST", "/uploads", value=request)
            if (
                any(created.get(key) != value for key, value in request.items())
                or created.get("state") != "open"
            ):
                raise LoadError("Dataset upload reservation identity/geometry mismatch")
            row.update(state="uploading", acknowledged_parts=0)
            checkpoint(row)
            with open_payload(self.input, sample) as stream:
                length, part = 0, 0
                while chunk := stream.read(self.options.part_size):
                    if length + len(chunk) > payload["length"]:
                        raise LoadError("Dataset input grew during upload")
                    result = client.call("PUT", f"/uploads/{row['id']}/parts/{part}", data=chunk)
                    expected = {"index": part, "length": len(chunk), "md5": hashlib.md5(chunk).hexdigest()}
                    if any(result.get(key) != value for key, value in expected.items()):
                        raise LoadError("Dataset upload part checksum/identity mismatch")
                    length, part = length + len(chunk), part + 1
                    row["acknowledged_parts"] = part
                    checkpoint(row)
            if length != payload["length"]:
                raise LoadError("Dataset input shrank during upload")
            row["state"] = "completing"
            checkpoint(row)
            completed = client.call("POST", f"/uploads/{row['id']}/complete")
            if (
                completed.get("id") != row["id"]
                or completed.get("state") != "complete"
                or completed.get("length") != payload["length"]
                or completed.get("md5") != payload["md5"]
            ):
                raise LoadError("Dataset completed upload checksum/identity mismatch")
            row["state"] = "complete"
            checkpoint(row)
        except BaseException as exc:
            row.update(state="uncertain", error=str(exc))
            checkpoint(row)
            raise
        finally:
            client.close()

    def _publish(self, client):
        operation = uuid.uuid4().hex
        inventory = self.data["inventory"]
        manifest = {
            "schema": inventory["schema"],
            "samples": [
                {
                    "id": sample["id"],
                    "fields": {
                        **sample["fields"],
                        "payload": {"kind": "payload", "upload": self.data["uploads"][sample["id"]]["id"]},
                    },
                }
                for sample in inventory["samples"]
            ],
        }
        request = {"operation": operation, "parent": None, "manifest": manifest}
        self.data.update(phase="preparing", publication_state="prepare_unknown", operation_id=operation)
        self.save()
        prepared = client.call("POST", "/snapshots/prepare", value=request)
        if (
            prepared.get("operation") != operation
            or prepared.get("state") not in {"prepared", "published"}
            or prepared.get("is_latest") is not False
        ):
            raise LoadError(f"Dataset prepare identity/state mismatch: {prepared}")
        self.data.update(snapshot_id=prepared["id"], phase="publishing", publication_state="unknown")
        self.save()
        try:
            published = client.call("POST", "/snapshots/publish", value=request)
        except Exception as original:
            try:
                published = client.call("GET", "/operations/" + operation)
            except Exception as reconcile_error:
                raise CommitUncertainError(
                    f"Dataset publication reply failed: {original}; status: {reconcile_error}"
                ) from original
            if published.get("state") != "published" or published.get("is_latest") is not True:
                raise CommitUncertainError(
                    f"Dataset publication remains uncertain after lost reply: {original}"
                ) from original
            self.data["publication_reconciled"] = True
        if (
            published.get("id") != prepared["id"]
            or published.get("operation") != operation
            or published.get("state") != "published"
            or published.get("is_latest") is not True
        ):
            raise LoadError("Dataset publish returned another publication identity/state")
        self.data["publication_state"] = "published"
        self.save()
        verify_manifest(
            client.call("GET", "/snapshots/" + prepared["id"] + "/manifest"), inventory, self.data["uploads"]
        )
        info = client.call("GET")
        if info.get("id") != self.data["dataset_id"] or info.get("latest") != prepared["id"]:
            raise LoadError("Dataset publication identity/latest mismatch")

    def run(self):
        client, code, started = None, 0, time.monotonic()
        try:
            self._input()
            self.data["generation_validation_seconds"] = time.monotonic() - started
            client = self.client_factory(
                self.options.dataset_uri,
                self.options.namespace,
                self.options.name,
                self.options.token,
                self.options.timeout,
            )
            self.data.update(phase="creating", dataset_state="unknown")
            self.save()
            created = client.call("PUT")
            if (
                created.get("name") != self.options.name
                or created.get("namespace") != self.options.namespace
                or created.get("latest") is not None
            ):
                raise LoadError("Dataset create returned an incompatible/nonempty Dataset")
            self.data.update(dataset_id=created["id"], dataset_state="created", phase="uploading")
            self.save()

            def save_upload(identity, row):
                self.data["uploads"][identity] = row
                self.save()

            started = time.monotonic()
            journal_workers(
                [(sample["id"], sample) for sample in self.data["inventory"]["samples"]],
                lambda _, sample, checkpoint: self._upload(client, sample, checkpoint),
                save_upload,
                self.options.upload_workers,
            )
            self.data["transfer_seconds"] = time.monotonic() - started
            started = time.monotonic()
            self._publish(client)
            self.data["publication_seconds"] = time.monotonic() - started
            if self.options.verify:
                started = time.monotonic()
                self.data["verification"] = verify_dataset(client, self.data)
                self.data["verification_seconds"] = time.monotonic() - started
            self.data.update(status="succeeded", phase="complete")
            self.save()
            if self.work and not self.options.keep_files:
                cleanup_owned(self.work, self.data["run_id"])
                self.data["work_directory_removed"] = True
        except BaseException as exc:
            code = 130 if isinstance(exc, (KeyboardInterrupt, SystemExit)) else getattr(exc, "exit_code", 1)
            uncertain = (
                self.data.get("publication_state") in {"unknown", "prepare_unknown"}
                or any(row["state"] == "uncertain" for row in self.data["uploads"].values())
                or self.data.get("dataset_state") == "unknown"
            )
            self.data["status"] = "interrupted" if code == 130 else "uncertain" if uncertain else "failed"
            self.data["errors"].append(str(exc))
        finally:
            if client is not None:
                client.close()
            self.data["finished_at"] = timestamp()
            try:
                self.save()
            except Exception as exc:
                self.data["errors"].append(f"Cannot persist Dataset report: {exc}")
                code = code or 1
        return RunResult(code, self.redactor.data(self.data), self.report_path)
