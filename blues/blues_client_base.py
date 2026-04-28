import asyncio
from abc import ABC, abstractmethod
from typing import Self

from blues.blues_client_config import BluesClientConfig
from blues.bluessp import BluesStanzaProtocolAsync
from blues.constants import ENCODING, HOST, PORT, TIMEOUT, AcceptedMessageTypes


class BluesClientBase(ABC):
    _constructed_via_factory = False

    host: str = HOST
    port: int = PORT
    encoding: str = ENCODING
    timeout: float = TIMEOUT
    bluessp: BluesStanzaProtocolAsync
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def __init__(self, config: BluesClientConfig) -> None:
        if not getattr(self, "_constructed_via_factory", False):
            raise RuntimeError("Use 'await BluesClientProtocol.create(...)'")

        self.host = config.host
        self.port = config.port
        self.encoding = config.encoding
        self.timeout = config.timeout
        self._ready = False
        self.bluessp = BluesStanzaProtocolAsync(self.encoding)

    @classmethod
    async def create(cls, config: BluesClientConfig) -> Self:
        self = cls.__new__(cls)
        self._constructed_via_factory = True
        self.__init__(config)

        if not self._ready:
            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port
                )
                self._ready = True
            except OSError as e:
                print(f"Could not connect to {self.host}:{self.port}, error: {e}")

        return self

    async def read(self) -> AcceptedMessageTypes:
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
        command = self.bluessp.encode(msg)
        self.writer.write(command)
        await self.writer.drain()

    async def close(self) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    @abstractmethod
    async def run(self) -> None: ...
