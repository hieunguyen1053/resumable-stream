"""Testing stream utilities for resumable-stream tests."""

import asyncio
from typing import AsyncIterator, List, Optional


class TestingStream:
    """
    A testing stream that allows controlled writing and reading.
    Provides a writer interface to push data and an async iterator to consume it.
    """

    def __init__(self) -> None:
        self._buffer: List[str] = []
        self._queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self._closed = False

    @property
    def buffer(self) -> List[str]:
        """Get all chunks that have been written."""
        return self._buffer

    async def write(self, chunk: str) -> None:
        """Write a chunk to the stream."""
        self._buffer.append(chunk)
        await self._queue.put(chunk)

    async def close(self) -> None:
        """Close the stream."""
        self._closed = True
        await self._queue.put(None)

    async def readable(self) -> AsyncIterator[str]:
        """Get an async iterator for reading the stream."""
        while True:
            chunk = await self._queue.get()
            if chunk is None:
                break
            yield chunk


class TestingStreamWriter:
    """Wrapper providing sync-like API for TestingStream writes."""

    def __init__(self, stream: TestingStream) -> None:
        self._stream = stream
        self._loop = asyncio.get_event_loop()

    def write(self, chunk: str) -> None:
        """Write a chunk (schedules async write)."""
        asyncio.ensure_future(self._stream.write(chunk))

    def close(self) -> None:
        """Close the stream (schedules async close)."""
        asyncio.ensure_future(self._stream.close())


def create_testing_stream() -> dict:
    """
    Create a testing stream with writer and readable components.

    Returns:
        Dict with:
        - 'readable': callable returning an async iterator
        - 'writer': TestingStreamWriter for writing/closing
        - 'buffer': list of written chunks
    """
    stream = TestingStream()
    writer = TestingStreamWriter(stream)

    return {
        "readable": stream.readable,
        "writer": writer,
        "buffer": stream.buffer,
    }


async def stream_to_buffer(
    stream: Optional[AsyncIterator[str]],
    max_n_reads: Optional[int] = None,
    timeout_seconds: float = 2.0,
) -> str:
    """
    Consume an async stream into a string buffer.

    Args:
        stream: The async iterator to consume
        max_n_reads: Maximum number of chunks to read (None for unlimited)
        timeout_seconds: Timeout for each read operation

    Returns:
        Concatenated string of all chunks

    Raises:
        ValueError: If stream is None
        TimeoutError: If read times out
    """
    if stream is None:
        raise ValueError("Stream should not be None")

    buffer: List[str] = []
    read_count = 0

    async def read_next():
        return await stream.__anext__()

    try:
        while True:
            try:
                chunk = await asyncio.wait_for(read_next(), timeout=timeout_seconds)
                buffer.append(chunk)
                read_count += 1

                if max_n_reads is not None and read_count >= max_n_reads:
                    break

            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                raise TimeoutError(f"Timeout with buffer {buffer}")

    except StopAsyncIteration:
        pass

    return "".join(buffer)
