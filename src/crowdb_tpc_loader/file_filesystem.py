"""Keep Arrow dataset scans on the same refreshing object filesystem as FileIO."""

import pyarrow as pa
import pyarrow.fs as fs

from .file_stream import CredentialReader


class CredentialFilesystem(fs.FileSystemHandler):
    def __init__(self, credentials):
        self.credentials = credentials

    def get_type_name(self):
        return "crowdb-delegated-s3"

    def normalize_path(self, path):
        return path

    def _fs(self, path):
        return self.credentials.resolve(path).filesystem

    def open_input_file(self, path):
        return pa.PythonFile(CredentialReader(self.credentials, path), mode="r")

    def open_input_stream(self, path):
        return self.open_input_file(path)

    def get_file_info(self, paths):
        result = []
        for path in paths:
            try:
                with self._fs(path).open_input_file(path) as stream:
                    result.append(fs.FileInfo(path, fs.FileType.File, size=stream.size()))
            except FileNotFoundError:
                result.append(fs.FileInfo(path, fs.FileType.NotFound))
        return result

    def get_file_info_selector(self, selector):
        return self._fs(selector.base_dir).get_file_info(selector)

    def open_output_stream(self, path, metadata):
        return self._fs(path).open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path, metadata):
        return self._fs(path).open_append_stream(path, metadata=metadata)

    def create_dir(self, path, recursive):
        return self._fs(path).create_dir(path, recursive=recursive)

    def delete_file(self, path):
        return self._fs(path).delete_file(path)

    def delete_dir(self, path):
        return self._fs(path).delete_dir(path)

    def delete_dir_contents(self, path, missing_dir_ok=False):
        return self._fs(path).delete_dir_contents(path, missing_dir_ok=missing_dir_ok)

    def delete_root_dir_contents(self):
        raise PermissionError("Table-scoped FileIO cannot delete the filesystem root")

    def move(self, src, dest):
        filesystem = self._fs(src)
        self.credentials.resolve(dest)
        return filesystem.move(src, dest)

    def copy_file(self, src, dest):
        filesystem = self._fs(src)
        self.credentials.resolve(dest)
        return filesystem.copy_file(src, dest)
