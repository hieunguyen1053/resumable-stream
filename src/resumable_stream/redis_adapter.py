"""Redis adapter implementations for Publisher and Subscriber."""

import asyncio
import logging
from typing import Any, Callable, Dict, Optional, Union, Awaitable

import redis.asyncio as redis

from .types import Publisher, Subscriber

logger = logging.getLogger(__name__)


class RedisPublisher(Publisher):
    """Publisher implementation using redis-py async client."""

    def __init__(self, redis_url: str):
        self._redis_url = redis_url
        self._client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        """Connect to Redis server."""
        if self._client is None:
            try:
                self._client = redis.from_url(self._redis_url, decode_responses=True)
                await self._client.ping()
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self._client = None
                raise

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a channel."""
        if self._client is None:
            raise RuntimeError("Publisher not connected")
        return await self._client.publish(channel, message)

    async def set(
        self, key: str, value: str, options: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> Union[str, Any]:
        """Set a key-value pair with optional expiration."""
        if self._client is None:
            raise RuntimeError("Publisher not connected")
        
        ex = kwargs.get("ex")
        if options and "ex" in options:
            ex = options["ex"]
            
        return await self._client.set(key, value, ex=ex)

    async def get(self, key: str) -> Optional[str]:
        """Get a value by key."""
        if self._client is None:
            raise RuntimeError("Publisher not connected")
        return await self._client.get(key)

    async def incr(self, key: str) -> int:
        """Increment a key's integer value."""
        if self._client is None:
            raise RuntimeError("Publisher not connected")
        return await self._client.incr(key)

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None


class RedisSubscriber(Subscriber):
    """Subscriber implementation using redis-py async client with PubSub."""

    def __init__(self, redis_url: str):
        self._redis_url = redis_url
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        self._handlers: Dict[str, Callable[[str], Awaitable[None]]] = {}
        self._listener_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Connect to Redis server and start PubSub."""
        if self._client is None:
            try:
                self._client = redis.from_url(self._redis_url, decode_responses=True)
                self._pubsub = self._client.pubsub()
                await self._client.ping()
            except Exception as e:
                logger.error(f"Failed to connect subscriber to Redis: {e}")
                if self._client:
                    await self._client.aclose()
                self._client = None
                self._pubsub = None
                raise

    async def subscribe(self, channel: str, callback: Callable[[str], Awaitable[None]]) -> None:
        """Subscribe to a channel with a callback for messages."""
        if self._pubsub is None:
            raise RuntimeError("Subscriber not connected")

        self._handlers[channel] = callback
        await self._pubsub.subscribe(channel)

        # Start listener task if not already running
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listen())

    async def _listen(self) -> None:
        """Background task to listen for messages."""
        if self._pubsub is None:
            return

        try:
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    channel = message["channel"]
                    data = message["data"]
                    
                    handler = self._handlers.get(channel)
                    if handler:
                        try:
                            # Ensure we await the callback if it's a coroutine
                            await handler(data)
                        except Exception as e:
                            logger.error(f"Error in subscription handler for {channel}: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Redis subscriber listener error: {e}")
            # Consider if we should reconnect here

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel."""
        if self._pubsub is None:
            return

        self._handlers.pop(channel, None)
        await self._pubsub.unsubscribe(channel)

        # Stop listener if no more handlers
        if not self._handlers and self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            
        if self._pubsub:
            await self._pubsub.aclose()
            
        if self._client:
            await self._client.aclose()
            
        self._client = None
        self._pubsub = None
