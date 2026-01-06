"""Runtime implementation for resumable-stream."""

import asyncio
import json
import logging
import uuid
from typing import (
    Any,
    AsyncIterator,
    Callable,
    List,
    Optional,
    Union,
    Literal
)

from .types import (
    CreateResumableStreamContextOptions,
    Publisher,
    ResumableStreamContext,
    Subscriber,
    RedisDefaults
)
from .redis_adapter import RedisPublisher, RedisSubscriber

logger = logging.getLogger(__name__)

# Constants
DONE_VALUE = "DONE"
DONE_MESSAGE = "\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n"


class _ResumableStreamContextImpl:
    """Internal implementation of ResumableStreamContext."""

    def __init__(
        self,
        key_prefix: str,
        publisher: Publisher,
        subscriber: Subscriber,
        wait_until: Optional[Callable[[Any], None]] = None
    ):
        self.key_prefix = f"{key_prefix}:rs"
        self.publisher = publisher
        self.subscriber = subscriber
        self.wait_until = wait_until or (lambda p: asyncio.create_task(p))

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

        # Try to increment the sentinel (acts as listener count)
        try:
            current_count = await self.publisher.incr(sentinel_key)
        except Exception as e:
            # If incr fails, the value might be "DONE" (string) which causes incr to fail
            # We assume it's DONE if it's not an integer, but strictly we should check
            if "not an integer" in str(e).lower():
                return None
            logger.error(f"Error incrementing sentinel for stream {stream_id}: {e}")
            raise

        if current_count == 1:
            # We're the first - create a new stream
            return await self._create_producer_stream(stream_id, make_stream)
        else:
            # Stream exists - resume it
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

        # Increment listener count
        await self.publisher.incr(sentinel_key)

        return await self._resume_consumer_stream(stream_id, skip_characters)

    async def _create_producer_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
    ) -> AsyncIterator[str]:
        """
        Creates the producer side of a resumable stream.
        Consumes the source stream and broadcasts to listeners.
        """
        chunks: List[str] = []
        listener_channels: List[str] = []
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

                    # Send buffered chunks preserving boundaries
                    cumulative_len = 0
                    for chunk in chunks:
                        chunk_start = cumulative_len
                        chunk_end = cumulative_len + len(chunk)

                        if chunk_end <= skip_chars:
                            cumulative_len = chunk_end
                            continue

                        if chunk_start < skip_chars:
                            # Partial skip
                            offset = skip_chars - chunk_start
                            partial_chunk = chunk[offset:]
                            if partial_chunk:
                                await self.publisher.publish(
                                    f"{self.key_prefix}:chunk:{listener_id}",
                                    partial_chunk
                                )
                        else:
                            # Full chunk
                            await self.publisher.publish(
                                f"{self.key_prefix}:chunk:{listener_id}",
                                chunk
                            )
                        
                        cumulative_len = chunk_end

                    # If stream is already done, send done signal
                    if is_done:
                        await self.publisher.publish(
                            f"{self.key_prefix}:chunk:{listener_id}",
                            DONE_MESSAGE
                        )
            except Exception as e:
                logger.error(f"Error handling resume request: {e}")

        # Subscribe to resume requests
        request_channel = f"{self.key_prefix}:request:{stream_id}"
        await self.subscriber.subscribe(request_channel, handle_resume_request)

        async def producer_generator() -> AsyncIterator[str]:
            nonlocal is_done

            try:
                source_stream = make_stream()
                async for chunk in source_stream:
                    chunks.append(chunk)

                    # Broadcast to all listeners
                    # Note: We await publish to ensure order and delivery
                    # In high throughput scenarios, we might want to fire and forget or batch
                    # but for now we stick to reliability.
                    for listener_id in listener_channels:
                        await self.publisher.publish(
                            f"{self.key_prefix}:chunk:{listener_id}",
                            chunk
                        )

                    yield chunk

                # Stream completed
                is_done = True

                # Update sentinel to DONE
                sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"
                await self.publisher.set(sentinel_key, DONE_VALUE, ex=86400)

                # Notify all listeners that stream is done
                for listener_id in listener_channels:
                    await self.publisher.publish(
                        f"{self.key_prefix}:chunk:{listener_id}",
                        DONE_MESSAGE
                    )

            finally:
                # Cleanup
                try:
                    await self.subscriber.unsubscribe(request_channel)
                except Exception as e:
                    logger.error(f"Error unsubscribing producer: {e}")
                
                # Signal that we are done
                if not stream_done_future.done():
                    stream_done_future.set_result(None)

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

        # Subscribe to chunks
        chunk_channel = f"{self.key_prefix}:chunk:{listener_id}"
        await self.subscriber.subscribe(chunk_channel, handle_chunk)

        # Request the producer to send us data
        request_channel = f"{self.key_prefix}:request:{stream_id}"
        await self.publisher.publish(
            request_channel,
            json.dumps({
                "listenerId": listener_id,
                "skipCharacters": skip_characters,
            })
        )

        async def consumer_generator() -> AsyncIterator[str]:
            try:
                while True:
                    try:
                        # Wait for next chunk with a timeout to detect stalled streams
                        message = await asyncio.wait_for(queue.get(), timeout=30.0)

                        if message == DONE_MESSAGE:
                            break

                        yield message

                    except asyncio.TimeoutError:
                        # Check if stream is done
                        sentinel_key = f"{self.key_prefix}:sentinel:{stream_id}"
                        value = await self.publisher.get(sentinel_key)
                        if value == DONE_VALUE:
                            break
                        # Otherwise continue waiting / retry logic could be added here
                        
                    except asyncio.CancelledError:
                        raise

            finally:
                # Cleanup
                try:
                    await self.subscriber.unsubscribe(chunk_channel)
                except Exception as e:
                    logger.error(f"Error unsubscribing consumer: {e}")

        return consumer_generator()


def create_resumable_stream_context(
    options: Optional[CreateResumableStreamContextOptions] = None,
    **kwargs: Any
) -> ResumableStreamContext:
    """
    Create a resumable stream context.

    Can be called with an Options object or kwargs.
    
    Args:
        options: CreateResumableStreamContextOptions object
        **kwargs: Alternative way to pass options (key_prefix, publisher, subscriber, redis_url, wait_until)
        
    Returns:
        A ResumableStreamContext instance
    """
    if options is None:
        options = CreateResumableStreamContextOptions(**kwargs)
        
    # Helper to clean up arguments if mixed usage (e.g. legacy kwargs)
    # If redis_url passed in kwargs but not in options (since dataclass might not have it if strictly following types.py)
    redis_url = kwargs.get("redis_url")

    publisher = options.publisher
    subscriber = options.subscriber
    key_prefix = options.key_prefix
    
    if publisher is None or subscriber is None:
        if not redis_url:
            # Try to start default if not provided
            # NOTE: Ideally we should require redis_url if pub/sub not provided
            # But for flexibility we could default to localhost or error out
            # Raising error is safer
            if not (publisher and subscriber):
                 # Check if we can create them from redis_url
                 if not redis_url:
                     # Fallback to localhost default for dev experience? 
                     # Or stricter:
                     raise ValueError("Must provide publisher/subscriber OR redis_url")

        if publisher is None:
            publisher = RedisPublisher(redis_url)
            # Auto connecting might be needed if not handled by caller
            # But let's assume async context or explicit connect.
            # Actually, `connect` is async, we can't await it here easily without making this factory async.
            # Only way is to fire and forget or expect user to connect.
            # The previous implementation did `asyncio.create_task(init_promise)`.
            # We should probably do the same if we create the clients.
            asyncio.create_task(publisher.connect())
            
        if subscriber is None:
            subscriber = RedisSubscriber(redis_url)
            asyncio.create_task(subscriber.connect())

    # Ensure we use the implementation that matches the protocol
    return _ResumableStreamContextImpl(
        key_prefix=key_prefix,
        publisher=publisher,
        subscriber=subscriber,
        wait_until=options.wait_until
    )
