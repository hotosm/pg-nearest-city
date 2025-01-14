"""Test fixtures."""

import pytest


@pytest.fixture(scope="session")
def db():
    """Database URI."""
    return "postgresql://cities:dummycipassword@db:5432/cities"
