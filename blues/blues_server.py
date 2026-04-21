import asyncio
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
from operator import itemgetter
from typing import Any

import blues.constants as const
from blues.deps.pygtrie import StringTrie
from blues.utils import flatten


class BluesServer:
    def __init__(
        self,
        host: str = const.HOST,
        port: int = const.PORT,
        msg_size: int = const.MSG_LIMIT,
        encoding: str = const.ENCODING,
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
        params = data.split(const.CRLF)
        for i in range(2, len(params), 2):
            command.append(params[i])
        return command

    async def route_command(
        self, command: list[str], writer: asyncio.StreamWriter
    ) -> None:
        try:
            com = getattr(self, command[0].lower())
            await com(command, writer)
        except AttributeError:
            await self.write(
                const.UNKNOWN_COMMAND.replace("*", command[0].lower()),
                writer,
                True,
                True,
            )

    def encode_response(
        self,
        msg: bytes | int | str | list[Any],
        isSimple: bool = False,
        isError: bool = False,
    ) -> bytes:
        if isSimple:
            if isError:
                return f"-{msg}{const.CRLF}".encode(self.encoding)
            return f"+{msg}{const.CRLF}".encode(self.encoding)
        if type(msg) is bytes:
            return msg
        if type(msg) is int:
            return f":{msg}{const.CRLF}".encode(self.encoding)
        if type(msg) is list:
            arr = "".join([self.encode_response(i).decode(self.encoding) for i in msg])
            return f"*{len(msg)}{const.CRLF}{arr}".encode(self.encoding)
        return f"${len(msg)}{const.CRLF}{msg}{const.CRLF}".encode(self.encoding)  # type: ignore

    async def write(
        self,
        msg: bytes | int | str | list[Any],
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
        if len(command) != 2 or command[1] == "":
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "echo"), writer, True, True
            )
            print(f"Error executing ECHO for {writer.get_extra_info('peername')}")
            return
        await self.write(command[1], writer)
        print(f"Executed ECHO for {writer.get_extra_info('peername')}")

    async def ping(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        n = len(command)
        if n > 2:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "ping"), writer, True, True
            )
            print(f"Error executing PING for {writer.get_extra_info('peername')}")
            return
        elif n == 2 and command[1] != "":
            await self.write(command[1], writer)
            print(f"Executed PING for {writer.get_extra_info('peername')}")
            return
        await self.write("PONG", writer, True)
        print(f"Executed PING for {writer.get_extra_info('peername')}")

    async def set(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        try:
            val: dict[str, Any] = {"value": command[2]}
            if len(command) < 2:
                # TODO: make this more comprehensive to handle all options
                await self.write(
                    const.WRONG_NUMBER_OF_ARGS.replace("*", "set"), writer, True, True
                )
                print(f"Error executing SET for {writer.get_extra_info('peername')}")
                return
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
        if len(command) != 2:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "get"), writer, True, True
            )
            print(f"Error executing GET for {writer.get_extra_info('peername')}")
            return
        val = self.internal_get(command[1])
        if val is None:
            await self.write(const.NULL_STR, writer)
            print(f"Executed GET for {writer.get_extra_info('peername')}")
            return
        await self.write(val, writer)
        print(f"Executed GET for {writer.get_extra_info('peername')}")

    async def rpush(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        value = command[2:]
        key = command[1]
        if (val := self.internal_get(key)) is not None:
            if type(val) is not list:
                await self.write(const.WRONG_TYPE, writer, True, True)
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
                await self.write(const.WRONG_TYPE, writer, True, True)
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
                await self.write(const.WRONG_TYPE, writer, True, True)
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
                await self.write(const.WRONG_TYPE, writer, True, True)
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
                await self.write(const.NULL_STR, writer)
        else:
            await self.write(const.WRONG_TYPE, writer, True, True)
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
                    await self.write(const.WRONG_TYPE, writer, True, True)
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
            await self.write(const.NULL_ARR, writer)
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
                await self.write(const.WRONG_TYPE, writer)

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
                        if stream_id == "0-*":
                            idx = 1
                        pass
                    stream_id = stream_id.replace("*", str(idx))

            # validate stream id
            if stream_id < const.MIN_STREAM_ID:
                await self.write(const.LOWER_THAN_MIN_STREAM_ID, writer, True, True)
                print(f"Executed XADD for {writer.get_extra_info('peername')}")
                return
            elif stream_id <= val.keys()[-1]:  # type: ignore
                await self.write(const.LOW_STREAM_ID, writer, True, True)
                print(f"Executed XADD for {writer.get_extra_info('peername')}")
                return
            value = val

        # generate stream id
        if stream_id.__contains__("*"):
            if stream_id == "*":
                stream_id = f"{now}-{idx}"
            else:
                if stream_id == "0-*":
                    idx = 1
                # assuming format number-*
                stream_id = stream_id.replace("*", str(idx))

        value[stream_id] = stream
        self.cache[key] = {"value": value}
        await self.write(stream_id, writer)
        print(f"Executed XADD for {writer.get_extra_info('peername')}")

    async def xrange(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if (args := len(command)) > 6 or args == 5 or args < 4:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "xrange"), writer, True, True
            )
            print(f"Executed XRANGE for {writer.get_extra_info('peername')}")
            return

        key = command[1]
        val = self.internal_get(key)

        if val is None:
            # return empty array if key not found
            await self.write([], writer)
            print(f"Executed XRANGE for {writer.get_extra_info('peername')}")
            return

        items = val.items()

        if command[2] == "-":
            start = 0
        elif command[2][0] == "(":
            # TODO: figure out why you need + 1 here
            start = bisect_right(items, command[2][1:], key=itemgetter(0)) + 1
        else:
            start = bisect_left(items, command[2], key=itemgetter(0))

        if start == len(items):
            # return empty array if start greater than all stream ids
            await self.write([], writer)
            print(f"Executed XRANGE for {writer.get_extra_info('peername')}")
            return

        # Python slicing is exclusive
        if command[3] == "+":
            end = len(items)
        elif command[3][0] == "(":
            end = bisect_left(items, command[3][1:], key=itemgetter(0))
        else:
            end = bisect_right(items, command[3], key=itemgetter(0))

        items = [[item[0], flatten(item[1].items())] for item in items[start:end]]

        if len(command) == 6:
            items = items[: int(command[5])]

        await self.write(items, writer)
        print(f"Executed XRANGE for {writer.get_extra_info('peername')}")

    async def disconnect_client(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()
        print(f"Client {writer.get_extra_info('peername')} disconnected gracefully")

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        async with self.server:
            try:
                await self.server.serve_forever()
            except asyncio.exceptions.CancelledError:
                # async with server automatically calls server.close() and server.wait_closer()
                print("\nShutting down gracefully")
                self.server.close_clients()

    async def stop(self) -> None:
        self.server.close_clients()
        self.server.close()
        await self.server.wait_closed()
