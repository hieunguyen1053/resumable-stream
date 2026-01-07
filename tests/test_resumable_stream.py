"""Tests for resumable-stream package.

Ported from https://github.com/vercel/resumable-stream/blob/main/src/__tests__/tests.ts
"""

import uuid
import pytest

from resumable_stream import create_resumable_stream_context
from testing_utils import create_in_memory_pubsub_for_testing, create_testing_stream, stream_to_buffer


@pytest.fixture
def resume():
    """Create a ResumableStreamContext with fresh in-memory pub/sub."""
    pubsub = create_in_memory_pubsub_for_testing()
    return create_resumable_stream_context(
        publisher=pubsub["publisher"],
        subscriber=pubsub["subscriber"],
        key_prefix=f"test-resumable-stream-{uuid.uuid4()}",
        wait_until=lambda p: None,
    )


class TestResumableStream:
    """Test suite for resumable stream functionality."""

    @pytest.mark.asyncio
    async def test_should_act_like_normal_stream(self, resume):
        """Should act like a normal stream."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")
        writer.write("2\n")
        writer.write("3\n")
        writer.close()

        result = await stream_to_buffer(stream)
        assert result == "1\n2\n3\n"

    @pytest.mark.asyncio
    async def test_should_resume_done_stream(self, resume):
        """Should resume a done stream."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        stream2 = await resume.resumable_stream("test", readable)

        writer.write("1\n")
        writer.write("2\n")
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_has_existing_stream(self, resume):
        """hasExistingStream should return correct states."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        # No stream yet
        assert await resume.has_existing_stream("test") is None

        stream = await resume.resumable_stream("test", readable)
        
        # Stream exists and is active
        assert await resume.has_existing_stream("test") is True
        assert await resume.has_existing_stream("test2") is None

        stream2 = await resume.resumable_stream("test", readable)
        assert await resume.has_existing_stream("test") is True

        writer.write("1\n")
        writer.write("2\n")
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"

        # Stream is done
        assert await resume.has_existing_stream("test") == "DONE"

    @pytest.mark.asyncio
    async def test_resume_done_stream_reverse_read(self, resume):
        """Should resume a done stream with reverse read order."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        stream2 = await resume.resumable_stream("test", readable)

        writer.write("1\n")
        writer.write("2\n")
        writer.close()

        # Read in reverse order
        result2 = await stream_to_buffer(stream2)
        result = await stream_to_buffer(stream)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_resume_in_progress_stream(self, resume):
        """Should resume an in-progress stream."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")

        stream2 = await resume.resumable_stream("test", readable)
        writer.write("2\n")
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_should_actually_stream(self, resume):
        """Should actually stream (partial reads work)."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")

        stream2 = await resume.resumable_stream("test", readable)

        # Read only 1 chunk each
        result = await stream_to_buffer(stream, max_n_reads=1)
        result2 = await stream_to_buffer(stream2, max_n_reads=1)

        assert result == "1\n"
        assert result2 == "1\n"

        writer.write("2\n")
        writer.close()

        # Continue reading
        step1 = await stream_to_buffer(stream)
        step2 = await stream_to_buffer(stream2)

        assert step1 == "2\n"
        assert step2 == "2\n"

    @pytest.mark.asyncio
    async def test_stream_producer_first(self, resume):
        """Should actually stream - producer reads first."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")

        stream2 = await resume.resumable_stream("test", readable)

        # Producer reads first
        result = await stream_to_buffer(stream, max_n_reads=1)
        assert result == "1\n"

        writer.write("2\n")
        writer.close()

        step1 = await stream_to_buffer(stream)
        step2 = await stream_to_buffer(stream2)

        assert step1 == "2\n"
        assert step2 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_stream_consumer_first(self, resume):
        """Should actually stream - consumer reads first."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")

        stream2 = await resume.resumable_stream("test", readable)

        # Consumer reads first
        result2 = await stream_to_buffer(stream2, max_n_reads=1)
        assert result2 == "1\n"

        writer.write("2\n")
        writer.close()

        step1 = await stream_to_buffer(stream)
        step2 = await stream_to_buffer(stream2)

        assert step1 == "1\n2\n"
        assert step2 == "2\n"

    @pytest.mark.asyncio
    async def test_resume_multiple_streams(self, resume):
        """Should resume multiple streams."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")

        stream2 = await resume.resumable_stream("test", readable)
        writer.write("2\n")

        stream3 = await resume.resumable_stream("test", readable)
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)
        result3 = await stream_to_buffer(stream3)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"
        assert result3 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_differentiate_between_streams(self, resume):
        """Should differentiate between streams."""
        stream_data1 = create_testing_stream()
        readable1 = stream_data1["readable"]
        writer1 = stream_data1["writer"]

        stream_data2 = create_testing_stream()
        readable2 = stream_data2["readable"]
        writer2 = stream_data2["writer"]

        stream1 = await resume.resumable_stream("test", readable1)
        stream2 = await resume.resumable_stream("test2", readable2)
        stream12 = await resume.resumable_stream("test", readable1)
        stream22 = await resume.resumable_stream("test2", readable2)

        writer1.write("1\n")
        writer1.write("2\n")
        writer1.close()

        writer2.write("writer2\n")
        writer2.close()

        result1 = await stream_to_buffer(stream1)
        result2 = await stream_to_buffer(stream2)
        result12 = await stream_to_buffer(stream12)
        result22 = await stream_to_buffer(stream22)

        assert result1 == "1\n2\n"
        assert result2 == "writer2\n"
        assert result12 == "1\n2\n"
        assert result22 == "writer2\n"

    @pytest.mark.asyncio
    async def test_skip_characters(self, resume):
        """Should respect skipCharacters (skip 2 chars)."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")
        writer.write("2\n")

        stream2 = await resume.resumable_stream("test", readable, skip_characters=2)
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == "2\n"

    @pytest.mark.asyncio
    async def test_skip_characters_all(self, resume):
        """Should respect skipCharacters (skip all - 4 chars)."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")
        writer.write("2\n")

        stream2 = await resume.resumable_stream("test", readable, skip_characters=4)
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == ""

    @pytest.mark.asyncio
    async def test_skip_characters_zero(self, resume):
        """Should respect skipCharacters (skip 0 chars)."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")
        writer.write("2\n")

        stream2 = await resume.resumable_stream("test", readable, skip_characters=0)
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_return_null_if_done(self, resume):
        """Should return None if stream is done."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.resumable_stream("test", readable)
        writer.write("1\n")
        writer.write("2\n")
        writer.close()

        result = await stream_to_buffer(stream)

        # Should return None for completed stream
        done_stream = await resume.resumable_stream(
            "test",
            lambda: (_ for _ in ()).throw(Exception("Should never be called"))
        )

        assert done_stream is None
        assert result == "1\n2\n"

    @pytest.mark.asyncio
    async def test_deconstructed_apis(self, resume):
        """Should support the deconstructed APIs."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.create_new_resumable_stream("test", readable)
        stream2 = await resume.resume_existing_stream("test")

        writer.write("1\n")
        writer.write("2\n")
        writer.close()

        result = await stream_to_buffer(stream)
        result2 = await stream_to_buffer(stream2)

        assert result == "1\n2\n"
        assert result2 == "1\n2\n"

    @pytest.mark.asyncio
    async def test_return_null_if_done_explicit_apis(self, resume):
        """Should return None if stream is done (explicit APIs)."""
        stream_data = create_testing_stream()
        readable = stream_data["readable"]
        writer = stream_data["writer"]

        stream = await resume.create_new_resumable_stream("test", readable)
        writer.write("1\n")
        writer.write("2\n")
        writer.close()

        result = await stream_to_buffer(stream)

        # Should return None for completed stream
        done_stream = await resume.resume_existing_stream("test")

        assert done_stream is None
        assert result == "1\n2\n"
