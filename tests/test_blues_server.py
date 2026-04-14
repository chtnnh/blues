import asyncio

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from blues.main import CRLF, ENCODING, MSG_LIMIT, BluesServer
from tests.utils import encode_command


class BluesClient:
    def __init__(self, host: str = "localhost", port: int = 6379) -> None:
        self.ready = False
        self.host = host
        self.port = port

    async def create(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.ready = True

    async def read(self) -> str:
        msg = await self.reader.read(MSG_LIMIT)
        return msg.decode(ENCODING)

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()


@pytest.fixture(autouse=True)
async def blues_server():
    blues_server = BluesServer()
    asyncio.create_task(blues_server.start())
    yield blues_server
    await blues_server.stop()


@pytest.fixture(autouse=True)
async def blues_client(blues_server: BluesServer):
    blues_client = BluesClient("localhost", 6379)
    await blues_client.create()
    yield blues_client
    await blues_client.close()


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.text(), st.text())
async def test_set_and_get_with_string(blues_client: BluesClient, key: str, value: str):
    assume(key != "")
    assume(value != "")
    blues_client.writer.write(encode_command(["set", key, value]))
    await blues_client.writer.drain()
    res = await blues_client.read()
    assert res == f"+OK{CRLF}", (
        f"Error returned for SET string with key: {key}, value: {value}"
    )
    blues_client.writer.write(encode_command(["get", key]))
    await blues_client.writer.drain()
    res = await blues_client.read()
    assert res == encode_command(value).decode(ENCODING)
    print(f"SET and GET tested with key: {key}, value: {value}")
