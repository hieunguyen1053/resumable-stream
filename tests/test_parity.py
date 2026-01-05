import pytest
import asyncio
from tests.conftest import create_testing_stream, stream_to_buffer

@pytest.mark.asyncio
async def test_should_act_like_a_normal_stream(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.write("3\n")
    await writer.close()
    
    result2 = await stream_to_buffer(stream)
    assert result2 == "1\n2\n3\n"

@pytest.mark.asyncio
async def test_should_resume_a_done_stream(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    stream2 = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"

@pytest.mark.asyncio
async def test_has_existing_stream(ctx):
    readable, writer = await create_testing_stream()
    
    assert await ctx.has_existing_stream("test") is None
    
    stream = await ctx.resumable_stream("test", readable)
    assert await ctx.has_existing_stream("test") is True
    assert await ctx.has_existing_stream("test2") is None
    
    stream2 = await ctx.resumable_stream("test", readable)
    assert await ctx.has_existing_stream("test") is True
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"
    
    # Wait a bit for async cleanup/sentinel update
    await asyncio.sleep(0.1)
    assert await ctx.has_existing_stream("test") == "DONE"

@pytest.mark.asyncio
async def test_should_resume_a_done_stream_reverse_read(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    stream2 = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    # Read stream2 first
    result2 = await stream_to_buffer(stream2)
    result = await stream_to_buffer(stream)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"

@pytest.mark.asyncio
async def test_should_resume_an_in_progress_stream(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    
    stream2 = await ctx.resumable_stream("test", readable)
    
    await writer.write("2\n")
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"

@pytest.mark.asyncio
async def test_should_actually_stream(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    
    stream2 = await ctx.resumable_stream("test", readable)
    
    # Consume partial
    # Note: iterating an async iterator consumes it. 
    # stream_to_buffer with limit breaks, but the rest of iterator is still valid/active?
    # No, `async for` consumes. We can't easily peek without fully consuming or managing buffer.
    # But `stream_to_buffer` creates a NEW list. The stream object itself?
    # `stream` is an AsyncIterator. Once yielded, it's gone.
    # Python generators can be paused.
    
    # In TS `streamToBuffer(stream, 1)` reads 1 chunk.
    # We need `stream_to_buffer` to NOT exhaust the stream if limit is hit.
    # Does `break` in `async for` stop consuming? Yes.
    
    # BUT, `stream` is an iterator. If we break, we can resume iterating it later?
    # Yes, for generator.
    
    result = await stream_to_buffer(stream, limit=1)
    result2 = await stream_to_buffer(stream2, limit=1)
    
    assert result == "1\n"
    assert result2 == "1\n"
    
    await writer.write("2\n")
    await writer.close()
    
    step1 = await stream_to_buffer(stream)
    step2 = await stream_to_buffer(stream2)
    
    assert step1 == "2\n"
    assert step2 == "2\n"

@pytest.mark.asyncio
async def test_should_actually_stream_producer_first(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    
    stream2 = await ctx.resumable_stream("test", readable)
    
    result = await stream_to_buffer(stream, limit=1)
    assert result == "1\n"
    
    await writer.write("2\n")
    await writer.close()
    
    step1 = await stream_to_buffer(stream)
    step2 = await stream_to_buffer(stream2)
    
    assert step1 == "2\n"
    assert step2 == "1\n2\n"

@pytest.mark.asyncio
async def test_should_actually_stream_consumer_first(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    
    stream2 = await ctx.resumable_stream("test", readable)
    
    result2 = await stream_to_buffer(stream2, limit=1)
    assert result2 == "1\n"
    
    await writer.write("2\n")
    await writer.close()
    
    step1 = await stream_to_buffer(stream)
    step2 = await stream_to_buffer(stream2)
    
    assert step1 == "1\n2\n"
    assert step2 == "2\n"

@pytest.mark.asyncio
async def test_should_resume_multiple_streams(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    
    stream2 = await ctx.resumable_stream("test", readable)
    
    await writer.write("2\n")
    
    stream3 = await ctx.resumable_stream("test", readable)
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    result3 = await stream_to_buffer(stream3)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"
    assert result3 == "1\n2\n"

@pytest.mark.asyncio
async def test_should_differentiate_between_streams(ctx):
    readable, writer = await create_testing_stream()
    readable2, writer2 = await create_testing_stream()
    
    stream1 = await ctx.resumable_stream("test", readable)
    stream2 = await ctx.resumable_stream("test2", readable2)
    stream12 = await ctx.resumable_stream("test", readable)
    stream22 = await ctx.resumable_stream("test2", readable2)
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    await writer2.write("writer2\n")
    await writer2.close()
    
    result1 = await stream_to_buffer(stream1)
    result2 = await stream_to_buffer(stream2)
    result12 = await stream_to_buffer(stream12)
    result22 = await stream_to_buffer(stream22)
    
    assert result1 == "1\n2\n"
    assert result2 == "writer2\n"
    assert result12 == "1\n2\n"
    assert result22 == "writer2\n"

@pytest.mark.asyncio
async def test_should_respect_skip_characters(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    
    # "1\n" (2 chars) + "2\n" (2 chars)
    # skip 2 => should get "2\n"
    stream2 = await ctx.resumable_stream("test", readable, skip_characters=2)
    
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == "2\n"

@pytest.mark.asyncio
async def test_should_respect_skip_characters_2(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    
    # "1\n2\n" is 4 chars. skip 4 => ""
    stream2 = await ctx.resumable_stream("test", readable, skip_characters=4)
    
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == ""

@pytest.mark.asyncio
async def test_should_respect_skip_characters_0(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    
    stream2 = await ctx.resumable_stream("test", readable, skip_characters=0)
    
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"

@pytest.mark.asyncio
async def test_should_return_none_if_stream_is_done(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    result = await stream_to_buffer(stream)
    assert result == "1\n2\n"
    
    # Wait for completion
    await asyncio.sleep(0.1)
    
    def fail():
        raise RuntimeError("Should never be called")
        
    assert await ctx.resumable_stream("test", fail) is None

@pytest.mark.asyncio
async def test_should_support_deconstructed_apis(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.create_new_resumable_stream("test", readable)
    stream2 = await ctx.resume_existing_stream("test")
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    result = await stream_to_buffer(stream)
    result2 = await stream_to_buffer(stream2)
    
    assert result == "1\n2\n"
    assert result2 == "1\n2\n"

@pytest.mark.asyncio
async def test_should_return_none_if_stream_is_done_explicit_apis(ctx):
    readable, writer = await create_testing_stream()
    stream = await ctx.create_new_resumable_stream("test", readable)
    
    await writer.write("1\n")
    await writer.write("2\n")
    await writer.close()
    
    result = await stream_to_buffer(stream)
    assert result == "1\n2\n"
    
    await asyncio.sleep(0.1)
    
    assert await ctx.resume_existing_stream("test") is None
