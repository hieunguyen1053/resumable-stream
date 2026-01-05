"""Core runtime implementation for resumable streams."""

import asyncio
import json
import os
import uuid
from typing import (
    AsyncIterator,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Union,
)

from .types import Publisher, Subscriber, CreateResumableStreamContextOptions
from .redis_adapter import RedisPublisher, RedisSubscriber


# Sentinel value to indicate stream is done
DONE_VALUE = "DONE"

# Sentinel message to signal stream completion via pub/sub
DONE_MESSAGE = "\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n"

# Default TTL for sentinel keys (24 hours)
DEFAULT_TTL = 24 * 60 * 60


def _is_debug() -> bool:
    return bool(os.environ.get("DEBUG"))


def _debug_log(*args) -> None:
    if _is_debug():
        print(*args)


class _ResumableStreamContext:
    """Implementation of ResumableStreamContext."""
    
    def __init__(
        self,
        key_prefix: str,
        publisher: Publisher,
        subscriber: Subscriber,
    ):
        self._key_prefix = f"{key_prefix}:rs"
        self._publisher = publisher
        self._subscriber = subscriber
        self._init_done = False
    
    async def _ensure_init(self) -> None:
        """Ensure publisher and subscriber are connected."""
        if not self._init_done:
            await self._publisher.connect()
            await self._subscriber.connect()
            self._init_done = True
    
    def _sentinel_key(self, stream_id: str) -> str:
        return f"{self._key_prefix}:sentinel:{stream_id}"
    
    def _request_channel(self, stream_id: str) -> str:
        return f"{self._key_prefix}:request:{stream_id}"
    
    def _chunk_channel(self, listener_id: str) -> str:
        return f"{self._key_prefix}:chunk:{listener_id}"
    
    async def has_existing_stream(
        self, 
        stream_id: str
    ) -> Union[None, Literal[True], Literal["DONE"]]:
        """Check if a stream exists."""
        await self._ensure_init()
        state = await self._publisher.get(self._sentinel_key(stream_id))
        
        if state is None:
            return None
        if state == DONE_VALUE:
            return "DONE"
        return True
    
    async def resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> Optional[AsyncIterator[str]]:
        """Creates or resumes a resumable stream (idempotent API)."""
        await self._ensure_init()
        
        # Try to increment the sentinel - this is atomic
        try:
            current_count = await self._publisher.incr(self._sentinel_key(stream_id))
        except Exception as e:
            error_str = str(e)
            if "value is not an integer" in error_str.lower():
                # Sentinel is set to "DONE"
                return None
            raise
        
        _debug_log("currentListenerCount", current_count)
        
        if current_count > 1:
            # Stream already exists, resume it
            return await self._resume_stream(stream_id, skip_characters)
        
        # First listener - create the stream
        return await self._create_new_stream(stream_id, make_stream)
    
    async def resume_existing_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> Union[AsyncIterator[str], None, Literal["NOT_FOUND"]]:
        """Resume an existing stream."""
        await self._ensure_init()
        
        state = await self._publisher.get(self._sentinel_key(stream_id))
        
        if state is None:
            return "NOT_FOUND"
        if state == DONE_VALUE:
            return None
        
        return await self._resume_stream(stream_id, skip_characters)
    
    async def create_new_resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
    ) -> Optional[AsyncIterator[str]]:
        """Create a new resumable stream."""
        await self._ensure_init()
        
        # Set sentinel to "1" to indicate stream is active
        await self._publisher.set(
            self._sentinel_key(stream_id), 
            "1", 
            ex=DEFAULT_TTL
        )
        
        return await self._create_new_stream(stream_id, make_stream)
    
    async def _create_new_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
    ) -> AsyncIterator[str]:
        """Internal: Create the producer stream."""
        # Map of listener_id -> remaining chars to skip
        listener_channels: Dict[str, int] = {}
        
        # Queue for the main caller (the one who created the stream)
        primary_queue: asyncio.Queue[Union[str, Exception, None]] = asyncio.Queue()
        
        is_done = False
        
        # We need a shared chunks list
        chunks: List[str] = []
        
        async def handle_request_wrapper(message: str) -> None:
             nonlocal is_done
             parsed = json.loads(message)
             listener_id = parsed["listenerId"]
             skip_chars = parsed.get("skipCharacters", 0) or 0
             
             _debug_log("Connected to listener", listener_id, "skip:", skip_chars)
             
             # Send buffered chunks
             all_content = "".join(chunks)
             
             # Calculate how much history to send and how much to reduce skip_chars
             current_history_len = len(all_content)
             
             if skip_chars <= current_history_len:
                 # We have enough history to satisfy skip, or start sending
                 chunks_to_send = all_content[skip_chars:]
                 
                 # Remaining skip for FUTURE chunks is 0
                 listener_channels[listener_id] = 0
             else:
                 # History is not enough to satisfy skip
                 chunks_to_send = ""
                 # We still need to skip more chars from future chunks
                 listener_channels[listener_id] = skip_chars - current_history_len
             
             # Always publish even if empty - this acts as the ACK for resume_stream
             _debug_log("sending chunks", len(chunks_to_send), "remaining skip:", listener_channels[listener_id])
             await self._publisher.publish(
                 self._chunk_channel(listener_id), 
                 chunks_to_send
             )
             
             if is_done:
                 await self._publisher.publish(
                     self._chunk_channel(listener_id),
                     DONE_MESSAGE
                 )

        await self._subscriber.subscribe(
            self._request_channel(stream_id),
            handle_request_wrapper
        )
        
        async def background_producer() -> None:
            """Produce chunks from the source stream in background."""
            nonlocal is_done
            
            try:
                async for chunk in make_stream():
                    chunks.append(chunk)
                    
                    _debug_log("Enqueuing line", chunk)
                    # Send to primary consumer
                    await primary_queue.put(chunk)
                    
                    # Broadcast to all listeners
                    chunk_len = len(chunk)
                    for listener_id, skip_remaining in list(listener_channels.items()):
                        chunk_to_send = chunk
                        
                        if skip_remaining > 0:
                            if skip_remaining >= chunk_len:
                                # Skip entire chunk
                                listener_channels[listener_id] -= chunk_len
                                chunk_to_send = ""
                            else:
                                # Partial skip
                                chunk_to_send = chunk[skip_remaining:]
                                listener_channels[listener_id] = 0
                        
                        if chunk_to_send:
                            _debug_log("sending line to", listener_id)
                            await self._publisher.publish(
                                self._chunk_channel(listener_id),
                                chunk_to_send
                            )
                
                # Stream finished successfully
                await primary_queue.put(None)
                
            except Exception as e:
                # Forward exception to primary consumer
                await primary_queue.put(e)
            finally:
                # Stream is done (success or failure)
                is_done = True
                _debug_log("Stream done")
                
                try:
                    # Mark sentinel as done
                    await self._publisher.set(
                        self._sentinel_key(stream_id),
                        DONE_VALUE,
                        ex=DEFAULT_TTL
                    )
                    
                    # Unsubscribe from requests
                    await self._subscriber.unsubscribe(self._request_channel(stream_id))
                    
                    # Notify all listeners that stream is done
                    for listener_id in list(listener_channels.keys()):
                        await self._publisher.publish(
                            self._chunk_channel(listener_id),
                            DONE_MESSAGE
                        )
                except Exception as e:
                    # If we fail during cleanup (e.g. loop closed, client closed), just log it
                    _debug_log("Error during cleanup:", e)
                
                _debug_log("Cleanup done")

        # Start producer in background
        asyncio.create_task(background_producer())

        # Return iterator for primary consumer
        async def primary_consumer() -> AsyncIterator[str]:
            while True:
                item = await primary_queue.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item

        return primary_consumer()
    
    async def _resume_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> Optional[AsyncIterator[str]]:
        """Internal: Resume an existing stream as a consumer."""
        listener_id = str(uuid.uuid4())
        queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        connected_event = asyncio.Event()
        timeout_occurred = False
        
        async def handle_chunk(message: str) -> None:
            """Handle incoming chunks from the producer."""
            nonlocal timeout_occurred
            
            _debug_log("Received message", message[:50] if len(message) > 50 else message)
            
            # Signal that we received something (producer acknowledged us)
            connected_event.set()
            
            if message == DONE_MESSAGE:
                await queue.put(None)  # Signal end of stream
                await self._subscriber.unsubscribe(self._chunk_channel(listener_id))
                return
            
            # Ignore empty strings (ACKs) from user stream perspective
            if message:
                await queue.put(message)
        
        # Subscribe to our personal chunk channel
        await self._subscriber.subscribe(
            self._chunk_channel(listener_id),
            handle_chunk
        )
        
        # Request chunks from the producer
        await self._publisher.publish(
            self._request_channel(stream_id),
            json.dumps({
                "listenerId": listener_id,
                "skipCharacters": skip_characters,
            })
        )
        
        # Wait for acknowledgment with timeout
        try:
            await asyncio.wait_for(connected_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            # Timeout - check if stream is done
            await self._subscriber.unsubscribe(self._chunk_channel(listener_id))
            
            state = await self._publisher.get(self._sentinel_key(stream_id))
            if state == DONE_VALUE:
                return None
            
            raise TimeoutError("Timeout waiting for producer acknowledgment")
        
        async def consumer() -> AsyncIterator[str]:
            """Consume chunks from the queue."""
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
        
        return consumer()


def create_resumable_stream_context(
    key_prefix: str = "resumable-stream",
    publisher: Optional[Publisher] = None,
    subscriber: Optional[Subscriber] = None,
    redis_url: Optional[str] = None,
) -> _ResumableStreamContext:
    """
    Create a resumable stream context.
    
    Args:
        key_prefix: Prefix for Redis keys (default: "resumable-stream")
        publisher: Custom Publisher implementation
        subscriber: Custom Subscriber implementation  
        redis_url: Redis URL for default publisher/subscriber
                   Falls back to REDIS_URL or KV_URL environment variables
    
    Returns:
        A ResumableStreamContext for creating and resuming streams.
    
    Example:
        ```python
        ctx = create_resumable_stream_context(redis_url="redis://localhost:6379")
        
        async def my_stream():
            for i in range(10):
                yield f"chunk {i}\\n"
        
        stream = await ctx.resumable_stream("my-id", my_stream)
        async for chunk in stream:
            print(chunk)
        ```
    """
    # Determine Redis URL
    url = redis_url or os.environ.get("REDIS_URL") or os.environ.get("KV_URL")
    
    if publisher is None:
        if url is None:
            raise ValueError(
                "Either provide a publisher or set REDIS_URL/KV_URL environment variable"
            )
        publisher = RedisPublisher(url)
    
    if subscriber is None:
        if url is None:
            raise ValueError(
                "Either provide a subscriber or set REDIS_URL/KV_URL environment variable"  
            )
        subscriber = RedisSubscriber(url)
    
    return _ResumableStreamContext(
        key_prefix=key_prefix,
        publisher=publisher,
        subscriber=subscriber,
    )
