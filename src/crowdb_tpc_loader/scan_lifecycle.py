"""Close delegated Arrow scan readers before PyIceberg closes their input file.

PyIceberg's task uses Scanner.to_batches without closing its native reader
when a row limit stops iteration. A Python-backed input can then receive a
prefetch callback after close, retaining Python errors in Arrow worker futures
until interpreter finalization. Keep the installed task's projection, filtering
and delete handling, with a task-local scanner factory that owns its reader.
"""

from contextlib import ExitStack, contextmanager
from functools import update_wrapper, wraps
from importlib.metadata import version
from types import FunctionType

import pyarrow.dataset as ds
from packaging.version import Version
import pyiceberg.io.pyarrow as arrowio

from .errors import CompatibilityError
from .file_credentials import EXPIRY, REFRESH


class _ClosingScanner:
    def __init__(self, scanner, lifetime):
        self.scanner = scanner
        self.lifetime = lifetime

    def to_batches(self):
        reader = self.scanner.to_reader()
        self.lifetime.callback(reader.close)
        return reader


class _ScannerFactory:
    def __init__(self, lifetime):
        self.lifetime = lifetime

    def from_fragment(self, *args, **kwargs):
        return _ClosingScanner(ds.Scanner.from_fragment(*args, **kwargs), self.lifetime)


class _DatasetAPI:
    def __init__(self, lifetime):
        self.Scanner = _ScannerFactory(lifetime)

    def __getattr__(self, name):
        return getattr(ds, name)


class _ScanInput:
    def __init__(self, source, lifetime):
        self.source = source
        self.lifetime = lifetime

    @contextmanager
    def open(self):
        with self.source.open() as stream:
            with self.lifetime:
                yield stream


class _ScanFileIO:
    def __init__(self, source, lifetime):
        self.source = source
        self.lifetime = lifetime

    def new_input(self, location):
        return _ScanInput(self.source.new_input(location), self.lifetime)


def install_scan_lifecycle(fileio_type):
    """Adapt only this FileIO; leave other FileIO and global Arrow APIs intact."""
    original = arrowio._task_to_record_batches
    if getattr(original, "_crowdb_reader_lifecycle", False):
        return
    if (
        not isinstance(original, FunctionType)
        or not Version("0.11.1") <= Version(version("pyiceberg")) < Version("0.12")
        or not {"ds", "Scanner", "from_fragment", "to_batches"} <= set(original.__code__.co_names)
    ):
        raise CompatibilityError("PyIceberg scan task API changed; cannot safely manage delegated readers")

    # A private globals mapping avoids replacing pyarrow.dataset.Scanner or
    # copying Iceberg's schema/deletion implementation into this package.
    @wraps(original)
    def task(fileio, *args, **kwargs):
        if isinstance(fileio, fileio_type) and REFRESH in fileio.properties and EXPIRY in fileio.properties:
            lifetime = ExitStack()
            namespace = {**original.__globals__, "ds": _DatasetAPI(lifetime)}
            scoped = FunctionType(
                original.__code__, namespace, original.__name__, original.__defaults__, original.__closure__
            )
            scoped.__kwdefaults__ = original.__kwdefaults__
            update_wrapper(scoped, original)
            return scoped(_ScanFileIO(fileio, lifetime), *args, **kwargs)
        return original(fileio, *args, **kwargs)

    task._crowdb_reader_lifecycle = True
    arrowio._task_to_record_batches = task
