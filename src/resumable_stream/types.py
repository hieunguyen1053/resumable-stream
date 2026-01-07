"""Type definitions for resumable-stream."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Protocol, Union

# Type alias for Redis client (stub)
Redis = Any


class Subscriber(ABC):
    """
    A Redis-like subscriber.
    Designed to be compatible with clients from both the `redis` and `ioredis` packages.
    """

    @abstractmethod
    def connect(self) -> Awaitable[Any]: ...

    @abstractmethod
    def subscribe(
        self, channel: str, callback: Callable[[str], Any]
    ) -> Awaitable[Union[None, int]]: ...

    @abstractmethod
    def unsubscribe(self, channel: str) -> Awaitable[Any]: ...


class Publisher(ABC):
    """
    A Redis-like publisher.
    Designed to be compatible with clients from both the `redis` and `ioredis` packages.
    """

    @abstractmethod
    def connect(self) -> Awaitable[Any]: ...

    @abstractmethod
    def publish(self, channel: str, message: str) -> Awaitable[Union[int, Any]]: ...

    @abstractmethod
    def set(
        self,
        key: str,
        value: str,
        options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Awaitable[Union[str, Any]]: ...

    @abstractmethod
    def get(self, key: str) -> Awaitable[Union[str, int, None]]: ...

    @abstractmethod
    def incr(self, key: str) -> Awaitable[int]: ...


@dataclass
class CreateResumableStreamContextOptions:
    """
    Options for creating a ResumableStreamContext.
    """

    key_prefix: str = "resumable-stream"
    wait_until: Optional[Callable[[Any], None]] = None
    subscriber: Optional[Union[Subscriber, Redis]] = None
    publisher: Optional[Union[Publisher, Redis]] = None


class ResumableStreamContext(Protocol):
    """
    Protocol for ResumableStreamContext to ensure type safety.
    """

    def resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], Any],
        skip_characters: Optional[int] = None,
    ) -> Awaitable[Optional[Any]]: ...

    def resume_existing_stream(
        self, stream_id: str, skip_characters: Optional[int] = None
    ) -> Awaitable[Optional[Any]]: ...

    def create_new_resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], Any],
        skip_characters: Optional[int] = None,
    ) -> Awaitable[Optional[Any]]: ...

    def has_existing_stream(
        self, stream_id: str
    ) -> Awaitable[Optional[Union[bool, Literal["DONE"]]]]: ...


class RedisDefaults:
    """
    Configuration for default Redis clients.
    """

    def __init__(
        self, subscriber: Callable[[], Subscriber], publisher: Callable[[], Publisher]
    ):
        self.subscriber = subscriber
        self.publisher = publisher
