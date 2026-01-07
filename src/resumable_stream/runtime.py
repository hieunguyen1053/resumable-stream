"""Runtime implementation for resumable-stream."""

import asyncio
import json
import logging
import uuid
from typing import Any, AsyncIterator, Callable, List, Literal, Optional, Union

from .redis_adapter import RedisPublisher, RedisSubscriber
from .types import (
    CreateResumableStreamContextOptions,
    Publisher,
    ResumableStreamContext,
    Subscriber,
)

logger = logging.getLogger(__name__)

# Constants
DONE_VALUE = "DONE"
DONE_MESSAGE = "\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n"


class _ResumableStreamContext:
    """Internal implementation of ResumableStreamContext."""

    def __init__(
        self,
        key_prefix: str,
        publisher: Publisher,
        subscriber: Subscriber,
        wait_until: Optional[Callable[[Any], None]] = None,
    ):
        self.key_prefix = f"{key_prefix}:rs"
        self.publisher = publisher
        self.subscriber = subscriber
        self.wait_until = wait_until or (lambda p: asyncio.ensure_future(p))

    async def has_existing_stream(
        self, stream_id: str
    ) -> Optional[Union[bool, Literal["DONE"]]]:
        """
        Check if a stream with the given ID exists.

        Returns:
            None if no stream exists, True if a stream is active,
            or "DONE" if the stream is finished.
        """
        sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"
        value = await self.publisher.get(sentinel_key)

        if value is None:
            return None
        if value == DONE_VALUE:
            return "DONE"
        return True

    async def resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> Optional[AsyncIterator[str]]:
        """
        Creates or resumes a resumable stream (idempotent API).
        """
        sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"

        try:
            current_count = await self.publisher.incr(sentinel_key)
        except Exception as e:
            if "not an integer" in str(e).lower():
                return None
            logger.error(f"Error incrementing sentinel for stream {stream_id}: {e}")
            raise

        if current_count == 1:
            return await self._create_producer_stream(stream_id, make_stream)
        else:
            return await self._resume_consumer_stream(stream_id, skip_characters)

    async def create_new_resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> Optional[AsyncIterator[str]]:
        """
        Explicitly creates a new resumable stream.
        """
        sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"

        # Set sentinel to "1" to indicate stream is active
        # We use a 24h expiry by default
        await self.publisher.set(sentinel_key, "1", ex=86400)

        return await self._create_producer_stream(stream_id, make_stream)

    async def resume_existing_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> Union[AsyncIterator[str], None, Literal["NOT_FOUND"]]:
        """
        Resumes a stream that was previously created.
        """
        sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"
        value = await self.publisher.get(sentinel_key)

        if value is None:
            return "NOT_FOUND"
        if value == DONE_VALUE:
            return None

        await self.publisher.incr(sentinel_key)

        return await self._resume_consumer_stream(stream_id, skip_characters)

    async def _create_producer_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
    ) -> AsyncIterator[str]:
        """
        Creates the producer side of a resumable stream.
        Consumes the source stream in background and broadcasts to listeners.
        """
        chunks: List[str] = []
        listener_channels: List[str] = []
        producer_queue: asyncio.Queue = asyncio.Queue()
        is_done = False
        stream_done_future = asyncio.Future()

        # Keep process alive until stream is done
        self.wait_until(stream_done_future)

        async def handle_resume_request(message: str):
            """Handle incoming resume requests from consumers."""
            nonlocal is_done
            try:
                data = json.loads(message)
                listener_id = data.get("listenerId")
                skip_chars = data.get("skipCharacters") or 0

                if listener_id:
                    listener_channels.append(listener_id)

                    chunks_to_send = "".join(chunks)[skip_chars:]

                    await self.publisher.publish(
                        f"{self.key_prefix}:chunk:{listener_id}", chunks_to_send
                    )

                    # If stream is already done, send done signal
                    if is_done:
                        await self.publisher.publish(
                            f"{self.key_prefix}:chunk:{listener_id}", DONE_MESSAGE
                        )
            except Exception as e:
                logger.error(f"Error handling resume request: {e}")

        # Subscribe to resume requests
        request_channel = f"{self.key_prefix}:request:{stream_id}"
        await self.subscriber.subscribe(request_channel, handle_resume_request)

        async def consume_source():
            """Background task to consume source stream and broadcast chunks."""
            nonlocal is_done
            try:
                source_stream = make_stream()
                async for chunk in source_stream:
                    chunks.append(chunk)

                    for listener_id in listener_channels:
                        await self.publisher.publish(
                            f"{self.key_prefix}:chunk:{listener_id}", chunk
                        )

                    await producer_queue.put(chunk)

                is_done = True

                sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"
                await self.publisher.set(sentinel_key, DONE_VALUE, ex=86400)

                for listener_id in listener_channels:
                    await self.publisher.publish(
                        f"{self.key_prefix}:chunk:{listener_id}", DONE_MESSAGE
                    )

                await producer_queue.put(None)

            except Exception as e:
                logger.error(f"Error consuming source stream: {e}")
                await producer_queue.put(None)
            finally:
                try:
                    await self.subscriber.unsubscribe(request_channel)
                except Exception as e:
                    logger.error(f"Error unsubscribing producer: {e}")

                if not stream_done_future.done():
                    stream_done_future.set_result(None)

        asyncio.ensure_future(consume_source())

        async def producer_generator() -> AsyncIterator[str]:
            while True:
                chunk = await producer_queue.get()
                if chunk is None:
                    break
                yield chunk

        return producer_generator()

    async def _resume_consumer_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> AsyncIterator[str]:
        """
        Creates a consumer that resumes an existing stream.
        """
        listener_id = str(uuid.uuid4())
        queue: asyncio.Queue = asyncio.Queue()

        async def handle_chunk(message: str):
            """Handle incoming chunks from producer."""
            await queue.put(message)

        chunk_channel = f"{self.key_prefix}:chunk:{listener_id}"
        await self.subscriber.subscribe(chunk_channel, handle_chunk)

        request_channel = f"{self.key_prefix}:request:{stream_id}"
        await self.publisher.publish(
            request_channel,
            json.dumps(
                {
                    "listenerId": listener_id,
                    "skipCharacters": skip_characters,
                }
            ),
        )

        async def consumer_generator() -> AsyncIterator[str]:
            try:
                while True:
                    try:
                        message = await asyncio.wait_for(queue.get(), timeout=30.0)

                        if message == DONE_MESSAGE:
                            break

                        if message:
                            yield message

                    except asyncio.TimeoutError:
                        sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"
                        value = await self.publisher.get(sentinel_key)
                        if value == DONE_VALUE:
                            break

                    except asyncio.CancelledError:
                        raise

            finally:
                try:
                    await self.subscriber.unsubscribe(chunk_channel)
                except Exception as e:
                    logger.error(f"Error unsubscribing consumer: {e}")

        return consumer_generator()


def create_resumable_stream_context(
    options: Optional[CreateResumableStreamContextOptions] = None, **kwargs: Any
) -> ResumableStreamContext:
    """
    Create a resumable stream context.

    Can be called with an Options object or kwargs.

    Args:
        options: CreateResumableStreamContextOptions object
        **kwargs: Alternative way to pass options

    Returns:
        A ResumableStreamContext instance
    """
    if options is None:
        options = CreateResumableStreamContextOptions(**kwargs)

    redis_url = kwargs.get("redis_url")

    publisher = options.publisher
    subscriber = options.subscriber
    key_prefix = options.key_prefix

    if publisher is None or subscriber is None:
        if not redis_url:
            if not (publisher and subscriber):
                if not redis_url:
                    raise ValueError("Must provide publisher/subscriber OR redis_url")

        if publisher is None:
            publisher = RedisPublisher(redis_url)
            asyncio.create_task(publisher.connect())

        if subscriber is None:
            subscriber = RedisSubscriber(redis_url)
            asyncio.create_task(subscriber.connect())

    return _ResumableStreamContext(
        key_prefix=key_prefix,
        publisher=publisher,
        subscriber=subscriber,
        wait_until=options.wait_until,
    )
