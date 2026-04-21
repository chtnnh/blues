import asyncio

from blues.bluessp import BluesStanzaProtocolAsync
from blues.constants import ENCODING, HOST, PORT, TIMEOUT, AcceptedMessageTypes


class BluesAsyncClient:
    def __init__(
        self,
        host: str = HOST,
        port: int = PORT,
        encoding: str = ENCODING,
        timeout: float = TIMEOUT,
    ) -> None:
        self.ready = False
        self.host = host
        self.port = port
        self.encoding = encoding
        self.timeout = timeout
        self.bluessp = BluesStanzaProtocolAsync(self.encoding)

    async def create(self) -> None:
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            self.ready = True
        except OSError as e:
            print(f"Could not connect to {self.host}:{self.port}, error: {e}")

    async def read(self) -> AcceptedMessageTypes:
        if not self.ready:
            print("Client is not connected to server, run BluesAsyncClient.create()")
            return
        try:
            async with asyncio.timeout(self.timeout):
                return await self.bluessp.decode(self.reader)
        except TimeoutError:
            print("Timed out while reading from server")
            return []

    async def write(self, msg: AcceptedMessageTypes) -> None:
        if not self.ready:
            print("Client is not connected to server, run BluesAsyncClient.create()")
            return
        command = self.bluessp.encode(msg)
        self.writer.write(command)
        await self.writer.drain()

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()
