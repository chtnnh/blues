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
        self.blxread_queue: dict[str, list[asyncio.StreamWriter]] = {}

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
        # TODO: replace with bluessp
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
        # TODO: replace with bluessp
        if isSimple:
            if isError:
                return f"-{msg}{const.CRLF}".encode(self.encoding)
            return f"+{msg}{const.CRLF}".encode(self.encoding)

        match msg:
            case bytes():
                return msg

            case int():
                return f":{msg}{const.CRLF}".encode(self.encoding)

            case list():
                arr = "".join(
                    [self.encode_response(i).decode(self.encoding) for i in msg]
                )
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
        msg = "PONG"

        if n > 2:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "ping"), writer, True, True
            )
            print(f"Error executing PING for {writer.get_extra_info('peername')}")
            return

        elif n == 2 and command[1] != "":
            msg = command[1]

        await self.write(msg, writer, True)
        print(f"Executed PING for {writer.get_extra_info('peername')}")

    async def set(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        try:
            val: dict[str, Any] = {"value": command[2]}

            if (args := len(command)) < 2:
                # TODO: make this more comprehensive to handle all options
                await self.write(
                    const.WRONG_NUMBER_OF_ARGS.replace("*", "set"), writer, True, True
                )
                print(f"Error executing SET for {writer.get_extra_info('peername')}")
                return

            if args > 3:
                # TODO: implement remaining SET options
                for idx in range(3, args):
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

    def _internal_get(self, key: str) -> Any:
        val = self.cache.get(key)

        # TODO: remove expired key value pairs, read how redis handles this
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

        val = self._internal_get(command[1])

        match val:
            case None:
                await self.write(const.NULL_STR, writer)
            case StringTrie():
                await self.write(const.WRONG_TYPE, writer, True, True)
            case _:
                await self.write(val, writer)

        print(f"Executed GET for {writer.get_extra_info('peername')}")

    async def rpush(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        value = command[2:]
        key = command[1]

        if (val := self._internal_get(key)) is not None:
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
        if (value := self._internal_get(command[1])) is not None:
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

        if (val := self._internal_get(key)) is not None:
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

        if (value := self._internal_get(command[1])) is not None:
            if type(value) is not list:
                await self.write(const.WRONG_TYPE, writer, True, True)
                return

            n = len(value)

        await self.write(n, writer)
        print(f"Executed LLEN for {writer.get_extra_info('peername')}")

    def _internal_lpop(self, command: list[str]) -> list[str] | str | bool:
        # TODO: figure out a way to modularize the below 4 lines
        # Don't Repeat Yourself (this is like the 4th repitition of these lines)
        if (value := self._internal_get(command[1])) is not None:
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
        pop = self._internal_lpop(command)

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
            if (value := self._internal_get(command[1])) is not None:
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
            written = False

            # third pass to remove BLPOPs
            for key in command[1:]:
                try:
                    self.blpop_queue[key].remove(writer)
                except ValueError:
                    # set written to True to prevent double-writing to client
                    written = True

            if not written:
                await self.write(const.NULL_ARR, writer)

        print(f"Executed BLPOP for {writer.get_extra_info('peername')}")

    async def blpop_helper(self, key: str) -> None:
        # NOTE: dict.keys() returns a dictview and changes when the underlying dict changes
        queue = self.blpop_queue.get(key, [])

        for writer in queue:
            try:
                await self.write([key, self.cache[key]["value"][0]], writer)
                self._internal_lpop(["lpop", key])
                # queue is a shallow copy (copy by reference)
                queue.remove(writer)
                break
            except Exception:
                continue

    async def type(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # TODO: set, zset, hash, and vectorset.
        val = self._internal_get(command[1])

        match val:
            case None:
                await self.write("none", writer, True)
            case str():
                await self.write("string", writer, True)
            case list():
                await self.write("list", writer, True)
            case StringTrie():
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

        if (val := self._internal_get(key)) is not None:
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
        # TODO: ensure atomic transactions don't execute this
        await self.blxread_helper(key, stream_id, stream)

    async def _internal_stream_get(self, command: list[str]) -> list[Any]:
        key = command[1]
        val: StringTrie | None = self._internal_get(key)

        if val is None:
            # return empty array if key not found
            return []

        items = val.items()

        if command[2] == "-":
            start = 0
        elif command[2][0] == "(":
            match command[2][1]:
                case "+":
                    start = len(items) - 1
                case "-":
                    start = 1
                case _:
                    start = bisect_right(items, command[2][1:], key=itemgetter(0))
        else:
            start = bisect_left(items, command[2], key=itemgetter(0))

        if start == len(items):
            # return empty array if start greater than all stream ids
            return []

        # Python slicing is exclusive
        if command[3] == "+":
            end = len(items)
        elif command[3][0] == "(":
            match command[3][1]:
                case "+":
                    start = len(items) - 1
                case "-":
                    start = 1
                case _:
                    start = bisect_right(items, command[2][1:], key=itemgetter(0))
        else:
            end = bisect_right(items, command[3], key=itemgetter(0))

        items = [[item[0], flatten(item[1].items())] for item in items[start:end]]  # type: ignore
        return items

    async def xrange(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if (args := len(command)) > 6 or args % 2 != 0 or args < 4:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "xrange"), writer, True, True
            )
            print(f"Executed XRANGE for {writer.get_extra_info('peername')}")
            return

        items = await self._internal_stream_get(command)

        if len(items) == 0:
            # XRANGE returns empty array even if key not found
            await self.write([], writer)
            print(f"Executed XRANGE for {writer.get_extra_info('peername')}")
            return

        if len(command) == 6:
            try:
                count = int(command[5])
                if count < len(items):
                    items = items[:count]
            except ValueError:
                await self.write(const.WRONG_COUNT_TYPE, writer, True, True)
                print(f"Executed XRANGE for {writer.get_extra_info('peername')}")
                return

        await self.write(items, writer)
        print(f"Executed XRANGE for {writer.get_extra_info('peername')}")

    async def xread(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # supports incomplete ids because _internal_stream_get uses bisect search on StringTrie
        entry = datetime.now(self.timezone)
        if (args := len(command)) < 4:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "xread"), writer, True, True
            )
            print(f"Executed XREAD for {writer.get_extra_info('peername')}")
            return
        elif args % 2 != 0:
            await self.write(const.INVALID_COMMAND, writer, True, True)
            print(f"Executed XREAD for {writer.get_extra_info('peername')}")
            return

        count = -1
        block = -1

        count_idx = [
            idx for idx, token in enumerate(command) if token.lower() == "count"
        ]
        if (counts := len(count_idx)) > 1:
            await self.write(const.INVALID_COMMAND, writer, True, True)
            print(f"Executed XREAD for {writer.get_extra_info('peername')}")
            return
        elif counts == 1:
            try:
                count = int(command[count_idx[0] + 1])
            except ValueError:
                await self.write(const.WRONG_COUNT_TYPE, writer, True, True)
                print(f"Executed XREAD for {writer.get_extra_info('peername')}")
                return

        block_idx = [
            idx for idx, token in enumerate(command) if token.lower() == "block"
        ]
        if (blocks := len(block_idx)) > 1:
            await self.write(const.INVALID_COMMAND, writer, True, True)
            print(f"Executed XREAD for {writer.get_extra_info('peername')}")
            return
        elif blocks == 1:
            try:
                block = int(command[block_idx[0] + 1])
            except ValueError:
                await self.write(const.WRONG_COUNT_TYPE, writer, True, True)
                print(f"Executed XREAD for {writer.get_extra_info('peername')}")
                return

        stream_idx = [
            idx for idx, token in enumerate(command) if token.lower() == "streams"
        ]
        if len(stream_idx) != 1:
            await self.write(const.INVALID_COMMAND, writer, True, True)
            print(f"Executed XREAD for {writer.get_extra_info('peername')}")
            return

        # assumes streams is the last flag (which is specified in the command syntax)
        keys = int((args - stream_idx[0] - 1) / 2)
        items = []
        tmp_keys = []

        for idx in range(keys):
            key_idx = stream_idx[0] + idx + 1  # idx starts from 0
            start_idx = key_idx + keys

            key = command[key_idx]
            start = command[start_idx]

            if start == "$":
                items.append([key, []])
            else:
                # _internal_stream_get already handles +
                # because (+ returns the last stream id
                com = ["XREAD", key, f"({start}", "+"]
                items.append([key, await self._internal_stream_get(com)])

            tmp_keys.append(key)

        keys = tmp_keys

        if block != -1 and self._are_all_streams_empty(items):
            for key in keys:
                queue = self.blxread_queue.get(key, [])
                queue.append(writer)
                self.blxread_queue[key] = queue

            while block == 0 or (
                datetime.now(self.timezone) - entry < timedelta(milliseconds=block)
            ):
                await asyncio.sleep(0)
            else:
                written = False
                for key in keys:
                    try:
                        self.blxread_queue[key].remove(writer)
                    except ValueError:
                        # mark written as true if a writer is removed by blxread_helper
                        written = True
                if not written:
                    await self.write(const.NULL_ARR, writer)
                print(f"Executed XREAD for {writer.get_extra_info('peername')}")
                return

        if count != -1:
            for idx in range(len(items)):
                if count < len(items[idx][1]):
                    items[idx][1] = items[idx][1][:count]

        if self._are_all_streams_empty(items):
            await self.write(const.NULL_ARR, writer)
            print(f"Executed XREAD for {writer.get_extra_info('peername')}")
            return

        await self.write(items, writer)
        print(f"Executed XREAD for {writer.get_extra_info('peername')}")

    async def blxread_helper(
        self, key: str, stream_id: str, stream: dict[Any, Any]
    ) -> None:
        # NOTE: dict.keys() returns a dictview and changes when the underlying dict changes
        queue = self.blxread_queue.get(key, [])
        for writer in queue:
            try:
                await self.write(
                    [[key, [[stream_id, flatten(list(stream.items()))]]]], writer
                )
                # queue is a shallow copy (copy by reference)
                queue.remove(writer)
            except Exception:
                continue

    def _are_all_streams_empty(self, it_streams: list[Any]) -> bool:
        """
        Expects [[key, [streams]], [key, [streams]]]
        Returns True if all streams have length 0
        """
        count = sum(map(lambda stream: len(stream[1]), it_streams))
        return count == 0

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
