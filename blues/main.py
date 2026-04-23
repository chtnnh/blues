import asyncio
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import typer

from blues.blues_server import BluesServer
from blues.constants import DEFAULT_TZ, ENCODING, HOST, MSG_LIMIT, PORT

app = typer.Typer()


@app.command()
def run(
    host: str = HOST,
    port: int = PORT,
    msg_limit: int = MSG_LIMIT,
    encoding: str = ENCODING,
    tz_str: str = "",
    master: str = "",
):
    try:
        tz = ZoneInfo(tz_str)
    except ValueError, ZoneInfoNotFoundError:
        tz = DEFAULT_TZ
    blues_server = BluesServer(host, port, msg_limit, encoding, tz, master)
    print(f"Starting blues server on {host}:{port}")
    asyncio.run(blues_server.start())


if __name__ == "__main__":
    app()
