import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

ENCODING = "utf-8"
PONG = "+PONG\r\n".encode(ENCODING)
OK = "+OK\r\n".encode(ENCODING)
NULL_STR = "$-1\r\n".encode(ENCODING)
WRONG_TYPE = (
    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".encode(
        ENCODING
    )
)
MSG_LIMIT = 1024
CRLF = "\r\n"


class RedisServer:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        msg_size: int = MSG_LIMIT,
        encoding: str = ENCODING,
        timezone: timezone = timezone.utc,
    ) -> None:
        self.host = host
        self.port = port
        self.msg_size = msg_size
        self.encoding = encoding
        self.timezone = timezone
        self.cache: dict[
            str, dict[str, Any]
        ] = {}  # TODO: research common time complexity and actual redis DS

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                data = await reader.read(self.msg_size)
                data = data.decode()
                if not data:
                    break
                command = self.parse_command(data)
                await self.route_command(command, writer)
        except ConnectionError, BrokenPipeError:
            print(
                f"Client {writer.get_extra_info('peername')} disconnected unexpectedly"
            )
        finally:
            await self.disconnect_client(writer)

    def parse_command(self, data: str) -> list[str]:
        command = []
        params = data.split(CRLF)
        for i in range(2, len(params), 2):
            command.append(params[i])
        return command

    def encode_response(self, msg: bytes | int | str | list[str]) -> bytes:
        if type(msg) is bytes:
            return msg
        if type(msg) is int:
            return f":{msg}{CRLF}".encode(self.encoding)
        if type(msg) is list:
            arr = "".join([f"${len(i)}{CRLF}{i}{CRLF}" for i in msg])
            return f"*{len(msg)}{CRLF}{arr}".encode(self.encoding)
        return f"${len(msg)}{CRLF}{msg}{CRLF}".encode(self.encoding)  # type: ignore

    async def route_command(
        self, command: list[str], writer: asyncio.StreamWriter
    ) -> None:
        com = getattr(self, command[0].lower())
        await com(command, writer)

    async def write(
        self, msg: bytes | int | str | list[str], writer: asyncio.StreamWriter
    ) -> None:
        try:
            writer.write(self.encode_response(msg))
            await writer.drain()
        except Exception as e:
            print(f"Error responding to {writer.get_extra_info('peername')}: {e}")

    async def echo(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        await self.write(command[1], writer)

    async def ping(self, _command: list[str], writer: asyncio.StreamWriter) -> None:
        await self.write(PONG, writer)

    async def set(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        try:
            val: dict[str, Any] = {"value": command[2]}
            if len(command) > 3:
                # TODO: implement remaining SET options
                for idx in range(3, len(command)):
                    option = command[idx]
                    match option.lower():
                        case "px":
                            val["expiry"] = datetime.now(self.timezone) + timedelta(
                                milliseconds=int(command[idx + 1])
                            )
                            idx += 1
                        case "pxat":
                            val["expiry"] = datetime.fromtimestamp(
                                int(command[idx + 1]) / 1000, self.timezone
                            )
                            idx += 1
                        case "ex":
                            val["expiry"] = datetime.now(self.timezone) + timedelta(
                                seconds=int(command[idx + 1])
                            )
                            idx += 1
                        case "exat":
                            val["expiry"] = datetime.fromtimestamp(
                                int(command[idx + 1]), self.timezone
                            )
                            idx += 1
            self.cache[command[1]] = val
            await self.write(OK, writer)
        except Exception as e:
            print(
                f"Error setting value {command[2]} for key {command[1]} from {writer.get_extra_info('peername')}: {e}"
            )

    def internal_get(self, key: str) -> Any:
        val = self.cache.get(key)
        # TODO: Find a better way to write that also does not flag type check
        if val is None or (
            (expiry := val.get("expiry")) is not None
            and expiry < datetime.now(self.timezone)
        ):
            return None
        else:
            return val.get("value", "")

    async def get(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        val = self.internal_get(command[1])
        if val is None:
            await self.write(NULL_STR, writer)
            return
        await self.write(val, writer)

    async def rpush(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        value = command[2:]
        if (val := self.internal_get(command[1])) is not None:
            if type(val) is not list:
                await self.write(WRONG_TYPE, writer)
                return
            val.extend(value)
            value = val
        self.cache[command[1]] = {"value": value}
        await self.write(len(value), writer)

    async def lrange(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if (value := self.internal_get(command[1])) is not None:
            if type(value) is not list:
                await self.write(WRONG_TYPE, writer)
                return
            start, end, n = int(command[2]), int(command[3]), len(value)
            if start > n:
                value = []
                await self.write(value, writer)
                return
            start = max(start, -1 * n)
            end = min(end, n - 1)
            if end < 0:
                end = end + n
            if start < 0:
                start = start + n
            value = value[start : end + 1]
        else:
            value = []
        await self.write(value, writer)

    async def lpush(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        value = command[2:]
        value = value[::-1]
        if (val := self.internal_get(command[1])) is not None:
            if type(val) is not list:
                await self.write(WRONG_TYPE, writer)
                return
            value.extend(val)
        self.cache[command[1]] = {"value": value}
        await self.write(len(value), writer)

    async def llen(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        n = 0
        if (value := self.internal_get(command[1])) is not None:
            if type(value) is not list:
                await self.write(WRONG_TYPE, writer)
                return
            n = len(value)
        await self.write(n, writer)

    async def lpop(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # TODO: figure out a way to modularize the below 4 lines
        # Don't Repeat Yourself (this is like the 4th repitition of these lines)
        if (value := self.internal_get(command[1])) is not None:
            if type(value) is not list:
                await self.write(WRONG_TYPE, writer)
                return
            n = 1
            if len(command) == 3:
                n = min(command[3], len(value))
            self.cache[command[1]] = {"value": value[n:]}
            if n == 1:
                value = value[0]
            else:
                value = value[n:]
            await self.write(value, writer)
            return
        await self.write(NULL_STR, writer)

    async def disconnect_client(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    redis_server = RedisServer("localhost", 6379, MSG_LIMIT, ENCODING, timezone.utc)
    asyncio.run(redis_server.start())


if __name__ == "__main__":
    main()
