"""In-memory pub/sub implementation for testing without Redis."""

import asyncio
from typing import Any, Callable, Dict, List, Optional, Union


async def _sleep(ms: float) -> None:
    """Sleep for the given number of milliseconds."""
    await asyncio.sleep(ms / 1000)


class InMemoryPubSub:
    """
    In-memory implementation of Publisher and Subscriber protocols.
    Designed for testing resumable-stream without requiring Redis.
    """

    def __init__(self) -> None:
        self._subscriptions: Dict[str, List[Callable[[str], Any]]] = {}
        self._data: Dict[str, Union[str, int]] = {}

    async def connect(self) -> None:
        """No-op for in-memory implementation."""
        pass

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a channel."""
        callbacks = self._subscriptions.get(channel, [])
        await _sleep(1)
        for callback in callbacks:
            try:
                result = callback(message)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                print(f"Error invoking callback: {e}")
        return len(callbacks)

    async def subscribe(self, channel: str, callback: Callable[[str], Any]) -> None:
        """Subscribe to a channel with a callback."""
        await _sleep(1)
        if channel not in self._subscriptions:
            self._subscriptions[channel] = []
        self._subscriptions[channel].append(callback)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel."""
        await _sleep(1)
        if channel in self._subscriptions:
            del self._subscriptions[channel]

    async def set(
        self,
        key: str,
        value: str,
        options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> str:
        """Set a key-value pair."""
        await _sleep(1)
        self._data[key] = value
        return "OK"

    async def get(self, key: str) -> Union[str, int, None]:
        """Get a value by key."""
        await _sleep(1)
        return self._data.get(key)

    async def incr(self, key: str) -> int:
        """Increment a key's value."""
        await _sleep(1)
        raw_value = self._data.get(key, 0)

        if isinstance(raw_value, str):
            try:
                value = int(raw_value)
            except ValueError:
                raise ValueError("ERR value is not an integer or out of range")
        else:
            value = raw_value

        new_value = value + 1
        self._data[key] = new_value
        return new_value


def create_in_memory_pubsub_for_testing() -> Dict[str, InMemoryPubSub]:
    """
    Create in-memory pub/sub instances for testing.

    Returns:
        A dict with 'subscriber' and 'publisher' keys, both pointing
        to the same InMemoryPubSub instance (since it implements both protocols).
    """
    pubsub = InMemoryPubSub()
    return {
        "subscriber": pubsub,
        "publisher": pubsub,
    }
