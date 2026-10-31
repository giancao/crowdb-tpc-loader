"""Separate synthetic Dataset commands, keeping TPC CLI behavior unchanged."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
from types import SimpleNamespace

from .errors import ArgumentError
from .report import atomic_json


def add_dataset_parser(commands, parser_class, positive_int, positive_timeout):
    parent = commands.add_parser("dataset", help="Generate, load and verify synthetic Dataset samples")
    actions = parent.add_subparsers(dest="dataset_command", required=True, parser_class=parser_class)
    for name in ("generate", "load", "verify"):
        parser = actions.add_parser(name)
        parser.add_argument("--report-file", type=Path)
        parser.add_argument("--json", action="store_true")
        parser.add_argument("--quiet", action="store_true")
        if name == "verify":
            parser.add_argument("report", type=Path, help="load report pinning an immutable snapshot")
        else:
            parser.add_argument("--samples", type=positive_int)
            parser.add_argument("--payload-bytes", type=positive_int)
            parser.add_argument("--seed", type=int)
        if name == "generate":
            parser.add_argument("--output-dir", type=Path, required=True)
        else:
            parser.add_argument("--dataset-uri", help="Dataset origin; defaults to DATASET_URI")
            parser.add_argument("--token", help="prefer DATASET_TOKEN to avoid process-list exposure")
            parser.add_argument("--timeout", type=positive_timeout, default=60.0)
        if name == "load":
            parser.add_argument("--namespace", default="dataset-ns")
            parser.add_argument("--name", required=True, help="fresh Dataset name")
            parser.add_argument("--input-dir", type=Path, help="reuse a fully validated generated Dataset")
            parser.add_argument("--work-dir", type=Path)
            parser.add_argument("--part-size", type=positive_int, default=1024**2)
            parser.add_argument("--upload-workers", type=positive_int, default=4)
            parser.add_argument("--keep-files", action="store_true")
            parser.add_argument(
                "--verify", action="store_true", help="read all samples and verify their bytes"
            )


def _token(args, redactor):
    value = args.token if args.token is not None else os.getenv("DATASET_TOKEN")
    redactor.add(value)
    if not value or "\n" in value or "\r" in value:
        raise ArgumentError("Dataset requires --token or DATASET_TOKEN, without newline characters")
    return value


def _options(args, redactor):
    if args.input_dir and any(value is not None for value in (args.samples, args.payload_bytes, args.seed)):
        raise ArgumentError("--input-dir cannot be combined with generation count/size/seed options")
    origin = args.dataset_uri or os.getenv("DATASET_URI")
    if not origin:
        raise ArgumentError("Dataset load requires --dataset-uri or DATASET_URI")
    values = vars(args).copy()
    values.update(
        dataset_uri=origin,
        token=_token(args, redactor),
        samples=args.samples if args.samples is not None else 128,
        payload_bytes=args.payload_bytes if args.payload_bytes is not None else 64 * 1024,
        seed=args.seed if args.seed is not None else 0,
    )
    return SimpleNamespace(**values)


def _verify(args, redactor):
    from .dataset_data import validate_inventory
    from .dataset_http import DatasetHTTP
    from .dataset_verify import verify_dataset

    if args.report.is_symlink() or args.report.stat().st_size > 32 * 1024**2:
        raise ArgumentError("Dataset report must be a bounded regular file")
    source = json.loads(args.report.read_text())
    if source.get("target") != "dataset" or source.get("command") != "load":
        raise ArgumentError("Verification requires a Dataset load report")
    if any(
        key not in source for key in ("dataset_id", "snapshot_id", "operation_id", "inventory", "uploads")
    ):
        raise ArgumentError(
            "Report has no complete snapshot identity; inspect its recorded operation/uploads"
        )
    validate_inventory(source["inventory"])
    client = DatasetHTTP(
        args.dataset_uri or os.getenv("DATASET_URI") or source["dataset_uri"],
        source["namespace"],
        source["name"],
        _token(args, redactor),
        args.timeout,
    )
    try:
        counts = verify_dataset(client, source)
    finally:
        client.close()
    return {
        "target": "dataset",
        "command": "verify",
        "status": "succeeded",
        "dataset_id": source["dataset_id"],
        "snapshot_id": source["snapshot_id"],
        "verification": counts,
        "publication_state": source.get("publication_state", "unknown"),
    }


def run_dataset_cli(args, redactor):
    previous = None
    try:
        previous = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    except ValueError:
        pass
    try:
        if args.report_file and (args.report_file.exists() or args.report_file.is_symlink()):
            raise ArgumentError("Report path already exists; choose a new file")
        code = 0
        if args.dataset_command == "generate":
            from .dataset_data import generate_dataset, validate_dataset_input

            root = generate_dataset(
                args.output_dir,
                args.samples if args.samples is not None else 128,
                args.payload_bytes if args.payload_bytes is not None else 64 * 1024,
                args.seed if args.seed is not None else 0,
            )
            _, inventory = validate_dataset_input(root)
            report = {
                "target": "dataset",
                "command": "generate",
                "status": "succeeded",
                "inventory_file": str(root / "inventory.json"),
                "sample_count": inventory["sample_count"],
                "payload_bytes": inventory["sample_count"] * inventory["payload_bytes"],
            }
            atomic_json(root / "run-summary.json", report)
        elif args.dataset_command == "load":
            from .dataset_load import DatasetRunner

            result = DatasetRunner(
                _options(args, redactor),
                lambda message: print(redactor.text(message), file=sys.stderr),
                redactor,
            ).run()
            report, code = result.report, result.exit_code
        else:
            report = _verify(args, redactor)
        if args.report_file and args.dataset_command != "load":
            atomic_json(args.report_file, redactor.data(report))
        if args.json:
            print(json.dumps(redactor.data(report), indent=2, allow_nan=False))
        else:
            print(redactor.text(f"Dataset {report['command']}: {report['status']}"))
            for key in (
                "report_file",
                "inventory_file",
                "dataset_id",
                "snapshot_id",
                "sample_count",
                "payload_bytes",
                "verification",
            ):
                if key in report:
                    print(redactor.text(f"{key}: {report[key]}"))
            for error in report.get("errors", []):
                print(redactor.text("ERROR: " + error), file=sys.stderr)
        return code
    except (KeyboardInterrupt, SystemExit):
        print("Dataset operation interrupted; retain its report before any retry", file=sys.stderr)
        return 130
    except Exception as exc:
        print(redactor.text(f"ERROR: {exc}"), file=sys.stderr)
        return getattr(exc, "exit_code", 1)
    finally:
        if previous is not None:
            try:
                signal.signal(signal.SIGTERM, previous)
            except ValueError:
                pass
