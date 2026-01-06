"""Unit tests for resumable-stream runtime."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import AsyncIterator, Optional

from resumable_stream.runtime import (
    create_resumable_stream_context,
    DONE_VALUE,
    DONE_MESSAGE,
)


class MockPublisher:
    """Mock Publisher for testing."""
    
    def __init__(self):
        self.data: dict = {}
        self.channels: dict = {}
        self._connected = False
    
    async def connect(self) -> None:
        self._connected = True
    
    async def publish(self, channel: str, message: str) -> int:
        if channel in self.channels:
            for callback in self.channels[channel]:
                await callback(message)
        return 1
    
    async def set(self, key: str, value: str, *, ex: int = None) -> str:
        self.data[key] = value
        return "OK"
    
    async def get(self, key: str) -> Optional[str]:
        return self.data.get(key)
    
    async def incr(self, key: str) -> int:
        current = self.data.get(key)
        if current == DONE_VALUE:
            raise Exception("value is not an integer")
        val = int(current or 0) + 1
        self.data[key] = str(val)
        return val
    
    def add_subscriber(self, channel: str, callback):
        if channel not in self.channels:
            self.channels[channel] = []
        self.channels[channel].append(callback)
    
    def remove_subscriber(self, channel: str, callback):
        if channel in self.channels:
            self.channels[channel] = [c for c in self.channels[channel] if c != callback]


class MockSubscriber:
    """Mock Subscriber for testing."""
    
    def __init__(self, publisher: MockPublisher):
        self._publisher = publisher
        self._handlers: dict = {}
        self._connected = False
    
    async def connect(self) -> None:
        self._connected = True
    
    async def subscribe(self, channel: str, callback) -> None:
        self._handlers[channel] = callback
        self._publisher.add_subscriber(channel, callback)
    
    async def unsubscribe(self, channel: str) -> None:
        callback = self._handlers.pop(channel, None)
        if callback:
            self._publisher.remove_subscriber(channel, callback)


@pytest.fixture
def mock_publisher():
    return MockPublisher()


@pytest.fixture
def mock_subscriber(mock_publisher):
    return MockSubscriber(mock_publisher)


@pytest.mark.asyncio
async def test_has_existing_stream_returns_none_when_no_stream(
    mock_publisher, mock_subscriber
):
    """Test that has_existing_stream returns None when no stream exists."""
    from resumable_stream.runtime import _ResumableStreamContextImpl
    
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    result = await ctx.has_existing_stream("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_has_existing_stream_returns_true_when_active(
    mock_publisher, mock_subscriber
):
    """Test that has_existing_stream returns True when stream is active."""
    from resumable_stream.runtime import _ResumableStreamContextImpl
    
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    # Set sentinel to active
    mock_publisher.data["test:rs:sentinel:my-stream"] = "1"
    
    result = await ctx.has_existing_stream("my-stream")
    assert result is True


@pytest.mark.asyncio
async def test_has_existing_stream_returns_done_when_finished(
    mock_publisher, mock_subscriber
):
    """Test that has_existing_stream returns 'DONE' when stream is finished."""
    from resumable_stream.runtime import _ResumableStreamContextImpl
    
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    # Set sentinel to done
    mock_publisher.data["test:rs:sentinel:my-stream"] = DONE_VALUE
    
    result = await ctx.has_existing_stream("my-stream")
    assert result == "DONE"


@pytest.mark.asyncio
async def test_create_new_resumable_stream_sets_sentinel(
    mock_publisher, mock_subscriber
):
    """Test that create_new_resumable_stream sets the sentinel key."""
    from resumable_stream.runtime import _ResumableStreamContextImpl
    
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    async def make_stream() -> AsyncIterator[str]:
        yield "hello"
        yield "world"
    
    stream = await ctx.create_new_resumable_stream("my-stream", make_stream)
    
    # Sentinel should be set to "1"
    assert mock_publisher.data["test:rs:sentinel:my-stream"] == "1"
    
    # Consume the stream
    chunks = []
    async for chunk in stream:
        chunks.append(chunk)
    
    assert chunks == ["hello", "world"]
    
    # After consuming, sentinel should be "DONE"
    assert mock_publisher.data["test:rs:sentinel:my-stream"] == DONE_VALUE


@pytest.mark.asyncio
async def test_resumable_stream_returns_none_when_already_done(
    mock_publisher, mock_subscriber
):
    """Test that resumable_stream returns None when stream is already done."""
    from resumable_stream.runtime import _ResumableStreamContextImpl
    
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    # Set sentinel to done
    mock_publisher.data["test:rs:sentinel:my-stream"] = DONE_VALUE
    
    async def make_stream() -> AsyncIterator[str]:
        yield "should not run"
    
    result = await ctx.resumable_stream("my-stream", make_stream)
    assert result is None


@pytest.mark.asyncio
async def test_resume_existing_stream_returns_not_found_when_no_stream(
    mock_publisher, mock_subscriber
):
    """Test that resume_existing_stream returns NOT_FOUND when no stream exists."""
    from resumable_stream.runtime import _ResumableStreamContextImpl
    
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    result = await ctx.resume_existing_stream("nonexistent")
    assert result == "NOT_FOUND"
