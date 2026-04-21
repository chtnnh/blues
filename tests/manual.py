import asyncio

from blues.blues_cli_client import BluesCliClient


async def main():
    client = BluesCliClient()
    await client.run()


if __name__ == "__main__":
    asyncio.run(main())
