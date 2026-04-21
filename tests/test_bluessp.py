import pytest
from hypothesis import assume, given, strategies

from blues.blues_async_client import BluesAsyncClient
from blues.bluessp import BluesStanzaProtocolAsync


@pytest.fixture(autouse=True)
async def bluessp():
    bluessp = BluesStanzaProtocolAsync()
