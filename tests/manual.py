import asyncio

import typer

from blues.blues_cli_client import BluesCliClient
from blues.blues_client_config import BluesClientConfig
from blues.constants import ENCODING, HOST, PORT, TIMEOUT

app = typer.Typer()


async def main(
    host: str = HOST,
    port: int = PORT,
    encoding: str = ENCODING,
    timeout: float = TIMEOUT,
):
    config = BluesClientConfig(host, port, encoding, timeout)
    client = await BluesCliClient.create(config)
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
