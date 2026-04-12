import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from app.deps.pygtrie import StringTrie

ENCODING = "utf-8"
NULL_STR = "$-1\r\n".encode(ENCODING)
NULL_ARR = "*-1\r\n".encode(ENCODING)
MIN_STREAM_ID = "0-1"
WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
LOW_STREAM_ID = (
    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
)
LOWER_THAN_MIN_STREAM_ID = "ERR The ID specified in XADD must be greater than 0-0"
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
        self.blpop_queue: dict[
            str, list[asyncio.StreamWriter]
        ] = {}  # TODO: implement key based ordering

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        client = writer.get_extra_info("peername")
        try:
            while True:
                data = await reader.read(self.msg_size)
                data = data.decode()
                if not data:
                    break
                command = self.parse_command(data)
                print(f"Routing command for {client}")
                await self.route_command(command, writer)
        except ConnectionError, BrokenPipeError:
            print(f"Client {client} disconnected unexpectedly")
        finally:
            await self.disconnect_client(writer)

    def parse_command(self, data: str) -> list[str]:
        command = []
        params = data.split(CRLF)
        for i in range(2, len(params), 2):
            command.append(params[i])
        return command

    async def route_command(
        self, command: list[str], writer: asyncio.StreamWriter
    ) -> None:
        com = getattr(self, command[0].lower())
        await com(command, writer)

    def encode_response(
        self,
        msg: bytes | int | str | list[str],
        isSimple: bool = False,
        isError: bool = False,
    ) -> bytes:
        if isSimple:
            if isError:
                return f"-{msg}{CRLF}".encode(self.encoding)
            return f"+{msg}{CRLF}".encode(self.encoding)
        if type(msg) is bytes:
            return msg
        if type(msg) is int:
            return f":{msg}{CRLF}".encode(self.encoding)
        if type(msg) is list:
            arr = "".join([f"${len(i)}{CRLF}{i}{CRLF}" for i in msg])
            return f"*{len(msg)}{CRLF}{arr}".encode(self.encoding)
        return f"${len(msg)}{CRLF}{msg}{CRLF}".encode(self.encoding)  # type: ignore

    async def write(
        self,
        msg: bytes | int | str | list[str],
        writer: asyncio.StreamWriter,
        isSimple: bool = False,
        isError: bool = False,
    ) -> None:
        try:
            writer.write(self.encode_response(msg, isSimple, isError))
            await writer.drain()
        except Exception as e:
            print(f"Error responding to {writer.get_extra_info('peername')}: {e}")
            raise

    async def echo(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        await self.write(command[1], writer)
        print(f"Executed ECHO for {writer.get_extra_info('peername')}")

    async def ping(self, _command: list[str], writer: asyncio.StreamWriter) -> None:
        await self.write("PONG", writer, True)
        print(f"Executed PONG for {writer.get_extra_info('peername')}")

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
            await self.write("OK", writer, True)
            print(f"Executed SET for {writer.get_extra_info('peername')}")
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
            return val.get("value")

    async def get(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        val = self.internal_get(command[1])
        if val is None:
            await self.write(NULL_STR, writer)
            print(f"Executed GET for {writer.get_extra_info('peername')}")
            return
        await self.write(val, writer)
        print(f"Executed GET for {writer.get_extra_info('peername')}")

    async def rpush(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        value = command[2:]
        key = command[1]
        if (val := self.internal_get(key)) is not None:
            if type(val) is not list:
                await self.write(WRONG_TYPE, writer, True, True)
                return
            val.extend(value)
            value = val
        self.cache[key] = {"value": value}
        await self.write(len(value), writer)
        print(f"Executed RPUSH for {writer.get_extra_info('peername')}")
        # TODO: ensure atomic transactions don't execute this
        await self.blpop_helper(key)

    async def lrange(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if (value := self.internal_get(command[1])) is not None:
            if type(value) is not list:
                await self.write(WRONG_TYPE, writer, True, True)
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
        print(f"Executed LRANGE for {writer.get_extra_info('peername')}")

    async def lpush(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        value = command[2:]
        value = value[::-1]
        key = command[1]
        if (val := self.internal_get(key)) is not None:
            if type(val) is not list:
                await self.write(WRONG_TYPE, writer, True, True)
                return
            value.extend(val)
        self.cache[key] = {"value": value}
        await self.write(len(value), writer)
        print(f"Executed LPUSH for {writer.get_extra_info('peername')}")
        # TODO: ensure atomic transactions don't execute this
        await self.blpop_helper(key)

    async def llen(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        n = 0
        if (value := self.internal_get(command[1])) is not None:
            if type(value) is not list:
                await self.write(WRONG_TYPE, writer, True, True)
                return
            n = len(value)
        await self.write(n, writer)
        print(f"Executed LLEN for {writer.get_extra_info('peername')}")

    def internal_lpop(self, command: list[str]) -> list[str] | str | bool:
        # TODO: figure out a way to modularize the below 4 lines
        # Don't Repeat Yourself (this is like the 4th repitition of these lines)
        if (value := self.internal_get(command[1])) is not None:
            if type(value) is not list:
                return False
            n = 1
            if len(command) == 3:
                n = min(int(command[2]), len(value))
            self.cache[command[1]] = {"value": value[n:]}
            if n == 1:
                value = value[0]
            else:
                value = value[:n]
            return value
        return True

    async def lpop(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        pop = self.internal_lpop(command)
        if pop:
            if type(pop) is not bool:
                await self.write(pop, writer)
            else:
                await self.write(NULL_STR, writer)
        else:
            await self.write(WRONG_TYPE, writer, True, True)
        print(f"Executed LPOP for {writer.get_extra_info('peername')}")

    async def blpop(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # BLPOP key [key...] timeout
        entry = datetime.now(self.timezone)
        try:
            timeout = float(command[-1])
            command.pop()
        except ValueError:
            timeout = 0
        # first pass to check if any list is non-empty
        for key in command[1:]:
            if (value := self.internal_get(command[1])) is not None:
                if type(value) is not list:
                    await self.write(WRONG_TYPE, writer, True, True)
                    return
                if len(value) > 0:
                    self.cache[key] = {"value": value[1:]}
                    await self.write([key, value[0]], writer)
                    return
        # second pass to add BLPOPs if all lists are empty
        for key in command[1:]:
            queue = self.blpop_queue.get(key, [])
            queue.append(writer)
            self.blpop_queue[key] = queue
        # timeout
        while timeout == 0 or (
            datetime.now(self.timezone) - entry < timedelta(seconds=timeout)
        ):
            await asyncio.sleep(0)
        else:
            await self.write(NULL_ARR, writer)
            # third pass to remove BLOPs
            for key in command[1:]:
                self.blpop_queue[key].remove(writer)
        print(f"Executed BLPOP for {writer.get_extra_info('peername')}")

    async def blpop_helper(self, key: str) -> None:
        # NOTE: dict.keys() returns a dictview and changes when the underlying dict changes
        queue = self.blpop_queue.get(key, [])
        for writer in queue:
            try:
                await self.write([key, self.cache[key]["value"][0]], writer)
                self.internal_lpop(["lpop", key])
                self.blpop_queue[key].remove(writer)
                break
            except Exception:
                continue

    async def type(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # TODO: set, zset, hash, stream, and vectorset.
        val = self.internal_get(command[1])
        match type(val).__name__:
            case "NoneType":
                await self.write("none", writer, True)
            case "str":
                await self.write("string", writer, True)
            case "list":
                await self.write("list", writer, True)
            case "StringTrie":
                await self.write("stream", writer, True)
        print(f"Executed TYPE for {writer.get_extra_info('peername')}")

    async def xadd(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # TODO: implement options

        value = StringTrie(separator="-")
        value.enable_sorting()
        stream = {}
        for i in range(3, len(command), 2):
            stream[command[i]] = command[i + 1]

        key = command[1]
        stream_id = command[2]
        now = f"{int(datetime.now(self.timezone).timestamp() * 1000)}"
        idx = 0

        if (val := self.internal_get(key)) is not None:
            if type(val) is not type(StringTrie()):
                await self.write(WRONG_TYPE, writer)

            # generate stream id
            if stream_id.__contains__("*"):
                if stream_id == "*":
                    stream_id = f"{now}-{idx}"
                else:
                    # TODO: look into race conditions for same ms id generation
                    # assuming format number-*
                    try:
                        idx = (
                            int(
                                val.keys(prefix=stream_id.split("-")[0])[-1].split("-")[  # type: ignore
                                    1
                                ]
                            )
                            + 1
                        )
                    except KeyError:
                        pass
                    stream_id = stream_id.replace("*", str(idx))

            # validate stream id
            if stream_id < MIN_STREAM_ID:
                await self.write(LOWER_THAN_MIN_STREAM_ID, writer, True, True)
                print(f"Executed XADD for {writer.get_extra_info('peername')}")
                return
            elif stream_id <= val.keys()[-1]:  # type: ignore
                await self.write(LOW_STREAM_ID, writer, True, True)
                print(f"Executed XADD for {writer.get_extra_info('peername')}")
                return
            value = val

        # generate stream id
        if stream_id.__contains__("*"):
            if stream_id == "*":
                stream_id = f"{now}-{idx}"
            else:
                # assuming format number-*
                stream_id = stream_id.replace("*", str(idx))

        value[stream_id] = stream
        self.cache[key] = {"value": value}
        await self.write(stream_id, writer)
        print(f"Executed XADD for {writer.get_extra_info('peername')}")

    async def disconnect_client(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()
        print(f"Client {writer.get_extra_info('peername')} disconnected gracefully")

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
