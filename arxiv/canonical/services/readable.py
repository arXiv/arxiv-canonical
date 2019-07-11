import io
from typing import IO, Optional, List, Callable


class MemoizedReadable:
    """
    A readable object that wraps a ``read()`` callable.

    This gives us lazy, memoized read access to a (presumably expensive)
    resource.
    """

    def __init__(self, read) -> None:
        self._read: Optional[Callable[[None], bytes]] = read
        self._content: Optional[IO[bytes]] = None

    @property
    def _loaded_content(self) -> IO[bytes]:
        if self._read is None:
            raise ValueError('Resource is closed')
        if self._content is None:
            self._content = io.BytesIO(self._read())
        return self._content

    def close(self) -> None:
        """Flush and close this stream."""
        if self._content is not None:
            self._content.close()
        else:
            self._read = None

    @property
    def closed(self) -> None:
        """``True`` if the stream is closed."""
        if self._content is not None:
            return self._content.closed
        return bool(self._read is None)

    def fileno(self) -> int:
        """Return the underlying file descriptor of the stream if it exists."""
        raise OSError('No underlying file')

    def flush(self) -> None:
        """Flush the write buffers of the stream if applicable."""
        return

    def isatty(self) -> bool:
        """Return True if the stream is interactive."""
        return False

    def readable(self) -> bool:
        """Return True if the stream can be read from."""
        if self._content is not None:
            return self._content.readable()
        if self._read is None:
            raise ValueError('I/O attempted on closed stream')
        return True

    def readline(self, size: int = -1) -> bytes:
        """Read and return one line from the stream."""
        return self._loaded_content.readline(size)

    def readlines(self, hint: int = -1) -> List[bytes]:
        """Read and return a list of lines from the stream."""
        return self._loaded_content.readlines(hint)

    def read(self, size: int = -1) -> bytes:
        """Read from the stream."""
        return self._loaded_content.read(size)

    def seek(self, offset: int, whence: int = 0) -> None:
        """Change the stream position to the given byte offset."""
        if self._content is not None:
            self._content.seek(offset, whence)

    def seekable(self) -> bool:
        """Return True if the stream supports random access."""
        if self._content is not None:
            return self._content.seekable()
        return bool(self._read is not None)

    def tell(self) -> int:
        """Return the current stream position."""
        if self._content is not None:
            return self._content.tell()
        return 0

    def truncate(self, size: Optional[int] = None) -> None:
        """Truncation is not supported."""
        raise NotImplementedError('Truncation not supported')

    def writable(self) -> bool:
        """Writing is not supported."""
        return False

    def writelines(self, lines: List[bytes]) -> None:
        """Writing is not supported."""
        raise NotImplementedError('Writing not supported')

    def __del__(self) -> None:
        """Prepare for deletion."""
        if self._content is not None:
            del self._content