"""Seekable PyArrow reads that can cross a delegated credential window."""

from __future__ import annotations

import io
import os

BLOCK = 8 * 1024 * 1024


class CredentialReader(io.RawIOBase):
    def __init__(self, credentials, path: str):
        super().__init__()
        self.credentials, self.path = credentials, path
        self.grant = credentials.resolve(path)
        self.stream = self.grant.filesystem.open_input_file(path)
        self.length = self.stream.size()
        self.position = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        self._checkClosed()
        return self.position

    def seek(self, offset, whence=os.SEEK_SET):
        self._checkClosed()
        if whence == os.SEEK_SET:
            position = offset
        elif whence == os.SEEK_CUR:
            position = self.position + offset
        elif whence == os.SEEK_END:
            position = self.length + offset
        else:
            raise ValueError("invalid whence")
        if position < 0:
            raise ValueError("negative seek position")
        self.position = position
        return position

    def read(self, size=-1):
        self._checkClosed()
        remaining = max(0, self.length - self.position)
        wanted = remaining if size is None or size < 0 else min(size, remaining)
        chunks = []
        while wanted:
            grant = self.credentials.resolve(self.path)
            if grant is not self.grant:
                replacement = grant.filesystem.open_input_file(self.path)
                if replacement.size() != self.length:
                    replacement.close()
                    raise OSError("Object length changed during credential refresh")
                self.stream.close()
                self.stream, self.grant = replacement, grant
            self.stream.seek(self.position)
            data = self.stream.read(min(BLOCK, wanted))
            if not data:
                raise OSError("Object reader made no progress before the expected end")
            chunks.append(data)
            self.position += len(data)
            wanted -= len(data)
        return b"".join(chunks)

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def close(self):
        if not self.closed:
            self.stream.close()
        super().close()
