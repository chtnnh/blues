import asyncio

import typer

from blues.blues_cli_client import BluesCliClient
from blues.constants import ENCODING, HOST, PORT, TIMEOUT

app = typer.Typer()


async def main(
    host: str = HOST,
    port: int = PORT,
    encoding: str = ENCODING,
    timeout: float = TIMEOUT,
):
    client = BluesCliClient(host, port, encoding, timeout)
    await client.run()


@app.command()
def run(
    host: str = HOST,
    port: int = PORT,
    encoding: str = ENCODING,
    timeout: float = TIMEOUT,
):
    asyncio.run(main(host, port, encoding, timeout))


if __name__ == "__main__":
    app()
