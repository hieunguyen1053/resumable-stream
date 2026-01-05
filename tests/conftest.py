import asyncio
import pytest
import fakeredis.aioredis
from typing import AsyncIterator, Tuple, Optional

from resumable_stream import create_resumable_stream_context, ResumableStreamContext

class VideoWriter:
    def __init__(self, queue: asyncio.Queue):
        self._queue = queue
        self._closed = False

    async def write(self, chunk: str):
        if self._closed:
            raise RuntimeError("Stream is closed")
        await self._queue.put(chunk)

    async def close(self):
        self._closed = True
        await self._queue.put(None)

async def create_testing_stream() -> Tuple[AsyncIterator[str], VideoWriter]:
    queue = asyncio.Queue()
    writer = VideoWriter(queue)

    async def readable() -> AsyncIterator[str]:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk

    return readable, writer

@pytest.fixture
def redis_url():
    return "redis://localhost:6379"

@pytest.fixture
async def redis_server():
    server = fakeredis.FakeServer()
    yield server

@pytest.fixture
async def ctx(redis_server):
    # We use a custom publisher/subscriber connected to the fake server
    # But since create_resumable_stream_context expects a URL or custom pub/sub
    # standard fakeredis with from_url is easiest if we patch it, 
    # OR we can just pass custom pub/sub using fakeredis.aioredis
    
    # Actually, simpler to just use the factory with a mocked connection string 
    # but that won't work easily with fakeredis unless we patch redis.from_url
    # OR we create custom Publisher/Subscriber using fakeredis client.
    
    from resumable_stream.redis_adapter import RedisPublisher, RedisSubscriber
    import uuid
    
    # Create clients connected to same fake server
    pub_client = fakeredis.aioredis.FakeRedis(server=redis_server, decode_responses=True)
    sub_client = fakeredis.aioredis.FakeRedis(server=redis_server, decode_responses=True)
    
    # We need to monkeypatch the internal clients of our adapters
    # or better, allow injecting the client in the adapter (not supported yet)
    # OR subclass them to inject the client.
    
    # Let's subclass to allow injection for testing
    class FakeRedisPublisher(RedisPublisher):
        def __init__(self, client):
            self._client = client
            self._redis_url = "fake"
            
        async def connect(self):
            pass # Already connected
            
    class FakeRedisSubscriber(RedisSubscriber):
        def __init__(self, client):
            self._client = client
            self._redis_url = "fake"
            self._pubsub = client.pubsub()
            self._handlers = {}
            self._listener_task = None
            
        async def connect(self):
            # Just setup pubsub
            pass

    publisher = FakeRedisPublisher(pub_client)
    subscriber = FakeRedisSubscriber(sub_client)
    
    context = create_resumable_stream_context(
        key_prefix=f"test-{uuid.uuid4()}",
        publisher=publisher,
        subscriber=subscriber
    )
    
    yield context
    
    await publisher.close()
    await subscriber.close()

async def stream_to_buffer(stream: AsyncIterator[str], limit: Optional[int] = None) -> str:
    buffer = []
    count = 0
    try:
        async for chunk in stream:
            buffer.append(chunk)
            count += 1
            if limit and count >= limit:
                break
    except Exception:
        pass
    return "".join(buffer)
