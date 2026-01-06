
import asyncio
import pytest
from typing import AsyncIterator
from resumable_stream.runtime import _ResumableStreamContextImpl, DONE_VALUE
from .test_runtime import MockPublisher, MockSubscriber

@pytest.fixture
def mock_publisher():
    return MockPublisher()

@pytest.fixture
def mock_subscriber(mock_publisher):
    return MockSubscriber(mock_publisher)

@pytest.mark.asyncio
async def test_resumed_stream_preserves_chunk_boundaries(
    mock_publisher, mock_subscriber
):
    """Test that resumed stream preserves chunk boundaries (e.g. for JSON lines)."""
    ctx = _ResumableStreamContextImpl(
        key_prefix="test",
        publisher=mock_publisher,
        subscriber=mock_subscriber,
    )
    
    # Producer yields two JSON objects as separate strings
    proceed_event = asyncio.Event()
    async def make_stream() -> AsyncIterator[str]:
        yield '{"id": 1}'
        yield '{"id": 2}'
        # Wait before finishing so the stream stays active and we can resume
        await proceed_event.wait()
    
    # 1. Start the stream and consume it to populate history
    stream = await ctx.create_new_resumable_stream("json-stream", make_stream)
    chunks = []
    
    # We only read 2 chunks because the third item (end) is blocked
    # We need to manually iterate
    iterator = stream.__aiter__()
    chunks.append(await iterator.__anext__())
    chunks.append(await iterator.__anext__())
    
    assert chunks == ['{"id": 1}', '{"id": 2}']
    
    # Wait a bit for background tasks to settle (updates history etc)
    await asyncio.sleep(0.1)
    
    # 2. Resume the stream from the beginning
    resumed_stream = await ctx.resume_existing_stream("json-stream", skip_characters=0)
    assert resumed_stream is not None, "Stream should be resumable while active"
    
    resumed_chunks = []
    iterator_resumed = resumed_stream.__aiter__()
    
    # We expect 2 chunks from history
    try:
        resumed_chunks.append(await asyncio.wait_for(iterator_resumed.__anext__(), timeout=1.0))
        resumed_chunks.append(await asyncio.wait_for(iterator_resumed.__anext__(), timeout=1.0))
    except (StopAsyncIteration, asyncio.TimeoutError):
        pass

    # Clean up
    proceed_event.set()

    # 3. Assert boundaries are preserved
    # CURRENTLY THIS IS EXPECTED TO FAIL because it will return ['{"id": 1}{"id": 2}'] or similar
    # The current implementation joins them, so we likely get one chunk with merged content
    assert len(resumed_chunks) == 2, f"Expected 2 chunks, got {len(resumed_chunks)}: {resumed_chunks}"
    assert resumed_chunks[0] == '{"id": 1}'
    assert resumed_chunks[1] == '{"id": 2}'
