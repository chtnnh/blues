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

    async def read_rdb(self) -> bytes:
        if not self.ready:
            print("Client is not connected to server, run BluesAsyncClient.create()")
            return b""

        try:
            async with asyncio.timeout(self.timeout):
                res = await self.reader.readline()
                msg_type, msg = res[:1], res[1:-2]

                if msg_type.decode(self.encoding) == "$":
                    len = int(msg.decode(self.encoding))
                    return await self.reader.read(len)
                else:
                    raise ValueError

        except TimeoutError:
            print("Timed out while reading from server")
            return b""

        except ValueError:
            print("Error while decoding RDB from server")
            return b""

    async def read(self) -> AcceptedMessageTypes:
        if not self.ready:
            print("Client is not connected to server, run BluesAsyncClient.create()")
            return

        try:
            async with asyncio.timeout(self.timeout):
                # TODO: handle is_null and is_error
                res, error, *_ = await self.bluessp.decode(self.reader)
                if error:
                    print("Error reading from client")
                    return None

                return res

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
