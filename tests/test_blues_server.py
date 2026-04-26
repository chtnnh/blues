import asyncio

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from blues.blues_async_client import BluesAsyncClient
from blues.blues_server_config import BluesServerConfig
from blues.bluessp import BluesStanzaProtocolAsync
from blues.constants import WRONG_NUMBER_OF_ARGS
from blues.main import BluesServer


@pytest.fixture(autouse=True)
async def blues_server():
    config = BluesServerConfig()
    blues_server = BluesServer(config)
    asyncio.create_task(blues_server.start())
    yield blues_server
    await blues_server.stop()


@pytest.fixture(autouse=True)
async def blues_async_client(blues_server: BluesServer):
    blues_async_client = BluesAsyncClient("localhost", 6379)
    await blues_async_client.create()
    yield blues_async_client
    await blues_async_client.close()


@pytest.fixture()
def bluessp():
    bluessp = BluesStanzaProtocolAsync()
    return bluessp


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.text() | st.none())
async def test_echo(
    blues_async_client: BluesAsyncClient,
    msg: str | None,
):
    if msg == "" or msg is None:
        await blues_async_client.write(["echo"])
    else:
        await blues_async_client.write(["echo", msg])

    res = await blues_async_client.read()

    if msg == "" or msg is None:
        assert res == WRONG_NUMBER_OF_ARGS.replace("*", "echo"), (
            "Echo executed without arguments"
        )
    else:
        assert res == msg


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.text() | st.none())
async def test_ping(
    blues_async_client: BluesAsyncClient,
    msg: str | None,
):
    if msg == "" or msg is None:
        await blues_async_client.write(["ping"])
    else:
        await blues_async_client.write(["ping", msg])

    res = await blues_async_client.read()
    if msg == "" or msg is None:
        assert res == "PONG"
    else:
        assert res == msg


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.text(), st.text())
async def test_set_and_get_with_string(
    blues_async_client: BluesAsyncClient,
    key: str,
    value: str,
):
    assume(key != "")
    assume(value != "")

    await blues_async_client.write(["set", key, value])
    res = await blues_async_client.read()
    assert res == "OK", (
        f"{res} returned for SET string with key: {key}, {[hex(ord(val)) for val in key]}, value: {value} {[hex(ord(val)) for val in value]}"
    )

    await blues_async_client.write(["get", key])
    res = await blues_async_client.read()
    assert res == value, (
        f"{res} returned for GET string with key: {key}, {[hex(ord(val)) for val in key]}, value: {value}, {[hex(ord(val)) for val in value]}"
    )


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.text(), st.lists(st.text(), min_size=1))
async def test_rpush_and_get_with_string(
    blues_async_client: BluesAsyncClient,
    key: str,
    values: list[str],
):
    assume(key != "")
    assume("" not in values)

    await blues_async_client.write(["rpush", key].extend(values))
    res = await blues_async_client.read()
    assert res == len(values), (
        f"{res} returned for RPUSH with key: {key}, {[hex(ord(val)) for val in key]}, values: {values}, {[[hex(ord(val)) for val in value] for value in values]}"
    )

    await blues_async_client.write("".join(["get", key]))
    res = await blues_async_client.read()
    assert res == values, (
        f"{res} returned for GET with key: {key}, {[hex(ord(val)) for val in key]}, value: {values}, {[[hex(ord(val)) for val in value] for value in values]}"
    )
