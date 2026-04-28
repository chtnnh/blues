import asyncio

from blues.blues_client_base import BluesClientBase


class BluesAsyncClient(BluesClientBase):
    async def read_rdb(self) -> bytes:
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
