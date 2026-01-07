"""Pytest configuration and fixtures for resumable-stream tests."""

import uuid
import pytest

from resumable_stream import create_resumable_stream_context
from testing_utils import create_in_memory_pubsub_for_testing


@pytest.fixture
def pubsub_factory():
    """Create an in-memory pub/sub factory for testing."""
    return create_in_memory_pubsub_for_testing


@pytest.fixture
def resume_context(pubsub_factory):
    """Create a ResumableStreamContext with in-memory pub/sub."""
    pubsub = pubsub_factory()
    return create_resumable_stream_context(
        publisher=pubsub["publisher"],
        subscriber=pubsub["subscriber"],
        key_prefix=f"test-resumable-stream-{uuid.uuid4()}",
        wait_until=lambda p: None,  # No-op for tests
    )
