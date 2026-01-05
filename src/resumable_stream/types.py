"""Type definitions for resumable-stream."""

from typing import (
    AsyncIterator,
    Callable,
    Literal,
    Optional,
    Protocol,
    TypedDict,
    Union,
    Awaitable,
)
from dataclasses import dataclass


class Publisher(Protocol):
    """A Redis-like publisher protocol."""
    
    async def connect(self) -> None:
        """Connect to the Redis server."""
        ...
    
    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a channel."""
        ...
    
    async def set(
        self, 
        key: str, 
        value: str, 
        *, 
        ex: Optional[int] = None
    ) -> str:
        """Set a key-value pair with optional expiration in seconds."""
        ...
    
    async def get(self, key: str) -> Optional[str]:
        """Get a value by key."""
        ...
    
    async def incr(self, key: str) -> int:
        """Increment a key's integer value."""
        ...


class Subscriber(Protocol):
    """A Redis-like subscriber protocol."""
    
    async def connect(self) -> None:
        """Connect to the Redis server."""
        ...
    
    async def subscribe(
        self, 
        channel: str, 
        callback: Callable[[str], Awaitable[None]]
    ) -> None:
        """Subscribe to a channel with a callback for messages."""
        ...
    
    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel."""
        ...


@dataclass
class CreateResumableStreamContextOptions:
    """Options for creating a resumable stream context."""
    
    key_prefix: str = "resumable-stream"
    """The prefix for the keys used by the resumable streams."""
    
    publisher: Optional[Publisher] = None
    """A pubsub publisher. If not provided, will be created from redis_url."""
    
    subscriber: Optional[Subscriber] = None
    """A pubsub subscriber. If not provided, will be created from redis_url."""
    
    redis_url: Optional[str] = None
    """Redis URL for creating default publisher/subscriber."""


class ResumableStreamContext(Protocol):
    """Context for managing resumable streams."""
    
    async def resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> Optional[AsyncIterator[str]]:
        """
        Creates or resumes a resumable stream.
        
        This is an idempotent API - it will create a new stream if one doesn't exist,
        or resume an existing stream if one is in progress.
        
        Args:
            stream_id: The ID of the stream. Must be unique for each stream.
            make_stream: A function that returns an async iterator of strings.
                        It's only executed if the stream is not yet in progress.
            skip_characters: Number of characters to skip when resuming.
        
        Returns:
            An async iterator of strings, or None if the stream is already done.
        """
        ...
    
    async def resume_existing_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> Union[AsyncIterator[str], None, Literal["NOT_FOUND"]]:
        """
        Resumes a stream that was previously created.
        
        Args:
            stream_id: The ID of the stream.
            skip_characters: Number of characters to skip when resuming.
        
        Returns:
            An async iterator of strings, None if the stream is done,
            or "NOT_FOUND" if no stream exists with the given ID.
        """
        ...
    
    async def create_new_resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
    ) -> Optional[AsyncIterator[str]]:
        """
        Creates a new resumable stream.
        
        Args:
            stream_id: The ID of the stream. Must be unique for each stream.
            make_stream: A function that returns an async iterator of strings.
        
        Returns:
            An async iterator of strings, or None if a stream with this ID
            already exists and is done.
        """
        ...
    
    async def has_existing_stream(
        self, 
        stream_id: str
    ) -> Union[None, Literal[True], Literal["DONE"]]:
        """
        Checks if a stream with the given ID exists.
        
        Args:
            stream_id: The ID of the stream.
        
        Returns:
            None if no stream exists, True if a stream is active,
            or "DONE" if the stream is finished.
        """
        ...
