import asyncio
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import typer

from blues.blues_server import BluesServer
from blues.blues_server_config import BluesServerConfig
from blues.constants import DEFAULT_TZ, ENCODING, HOST, MSG_LIMIT, PORT

app = typer.Typer()


async def main(
    host: str = HOST,
    port: int = PORT,
    msg_limit: int = MSG_LIMIT,
    encoding: str = ENCODING,
    tz_str: str = "",
    replicaof: str = "",
):
    try:
        tz = ZoneInfo(tz_str)
    except ValueError, ZoneInfoNotFoundError:
        tz = DEFAULT_TZ

    config = BluesServerConfig(host, port, msg_limit, encoding, tz, replicaof)
    blues_server = await BluesServer.create(config)

    print(f"Starting blues server on {host}:{port}")
    await blues_server.start()


@app.command()
def run(
    host: str = HOST,
    port: int = PORT,
    msg_limit: int = MSG_LIMIT,
    encoding: str = ENCODING,
    tz_str: str = "",
    replicaof: str = "",
):
    asyncio.run(main(host, port, msg_limit, encoding, tz_str, replicaof))


if __name__ == "__main__":
    app()
