import asyncio
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from operator import itemgetter
from random import choice
from string import ascii_letters, digits
from typing import Any

import blues.constants as const
from blues.blues_async_client import BluesAsyncClient
from blues.blues_server_config import BluesServerConfig
from blues.deps.pygtrie import StringTrie
from blues.utils import flatten


class BluesServer:
    _constructed_via_factory = False

    def __init__(self, config: BluesServerConfig) -> None:
        if not getattr(self, "_constructed_via_factory", False):
            raise RuntimeError("Use 'await BluesServer.create(...)'")

        self.host = config.host
        self.port = config.port
        self.msg_size = config.msg_size
        self.encoding = config.encoding
        self.timezone = config.timezone
        self.replica_of = config.replica_of

        if config.replica_of != "":
            master_host, master_port = config.replica_of.split()
            self.master_host, self.master_port = master_host, int(master_port)
            self.master = BluesAsyncClient(self.master_host, self.master_port)
        else:
            self.master_host = None
            self.master_port = None
            self.master = None

        self.repl_id = "".join(choice(ascii_letters + digits) for _ in range(40))
        self.master_repl_offset = 0
        self.replicas: dict[int, dict[Any, Any]] = {}

        # TODO: explore defaultdict as a replacement for each DS below
        # TODO: research common time complexity and actual redis DS
        self.cache: dict[str, dict[str, Any]] = {}

        # TODO: implement key based ordering
        self.blpop_queue: dict[str, list[asyncio.StreamWriter]] = {}

        self.blxread_queue: dict[str, list[tuple[asyncio.StreamWriter, str]]] = {}
        self.blocked_writers: dict[asyncio.StreamWriter, asyncio.Event] = {}
        self.transactions: dict[asyncio.StreamWriter, list[list[str]]] = {}
        self.internal_queue: dict[asyncio.StreamWriter, list[Any]] = {}
        self.watched_keys: dict[
            str, list[tuple[asyncio.Event, asyncio.StreamWriter]]
        ] = {}
        self.watch_events: dict[
            asyncio.StreamWriter, list[tuple[asyncio.Event, str]]
        ] = {}

    @classmethod
    async def create(cls, config: BluesServerConfig) -> BluesServer:
        self = cls.__new__(cls)
        self._constructed_via_factory = True
        cls.__init__(self, config)

        if self.master is not None:
            await self.master.create()
            await self._connect_to_master()

        return self

    async def _connect_to_master(self) -> None:
        if self.master is None:
            raise RuntimeError("Cannot call _connect_to_master without master")

        # TODO: psync
        commands = [
            (["PING"], "PONG"),
            (["REPLCONF", "listening-port", f"{self.port}"], "OK"),
            (["REPLCONF", "capa", "psync2"], "OK"),
            (["PSYNC", "?", "-1"], "FULLRESYNC"),
        ]
        for command, expected in commands:
            await self.master.write(command)
            res = await self.master.read()

            # TODO: make this more comprehensive
            if "PSYNC" in command:
                res = res.split()[0]  # type: ignore

            if res != expected:
                raise RuntimeError(
                    f"Error connecting to master: expected {expected} for {command}, got {res}"
                )

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
        self,
        command: list[str],
        writer: asyncio.StreamWriter,
        in_transaction: bool = False,
    ) -> None:
        com = command[0].lower()
        try:
            queue = self.transactions.get(writer, None)
            if queue is None or in_transaction:
                command_handler = getattr(self, com)
                await command_handler(command, writer)
            elif com == "multi":
                await self.write(const.ERR_NESTED_MULTI, writer, True, True)
            elif com == "watch":
                await self.write(const.ERR_WATCH_INSIDE_MULTI, writer, True, True)
            elif com == "discard":
                self.transactions.pop(writer, [])
                self.internal_queue.pop(writer, [])
                self._internal_unwatch(writer)
                await self.write("OK", writer, True)
            elif com == "exec":
                await self.exec(command, writer)
            else:
                queue.append(command)
                await self.write(const.QUEUED, writer, True)
        except AttributeError:
            await self.write(
                const.UNKNOWN_COMMAND.replace("*", com),
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

    async def info(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # TODO: make this more comprehensive
        if self.master:
            info_data = [
                "# Replication",
                "role:slave",
                "",
            ]
        else:
            info_data = [
                "# Replication",
                "role:master",
                f"master_replid:{self.repl_id}",
                f"master_repl_offset:{self.master_repl_offset}",
                "",
            ]

        await self.write(const.CRLF.join(info_data), writer)
        print(f"Executed INFO for {writer.get_extra_info('peername')}")

    async def replconf(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        try:
            port = int(command[command.index("listening-port") + 1])
        except ValueError:
            port = writer.get_extra_info("peername")[1]

        capabilities = [
            command[int(idx) + 1] for idx, flag in enumerate(command) if flag == "capa"
        ]
        replica = self.replicas.get(port, {})
        val = replica.get("capabilities", [])
        # update by reference
        val.extend(capabilities)

        await self.write("OK", writer)
        print(f"Executed REPLCONF for {writer.get_extra_info('peername')}")

    async def psync(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if len(command) != 3:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "psync"), writer, True, True
            )
            print(f"Error executing PSYNC for {writer.get_extra_info('peername')}")
            return

        try:
            repl_id, offset = command[1], int(command[2])
        except ValueError:
            await self.write(const.INVALID_COMMAND, writer, True, True)
            print(f"Error executing PSYNC for {writer.get_extra_info('peername')}")
            return

        if repl_id == "?":
            await self.write(f"FULLRESYNC {self.repl_id} 0", writer, True)
        else:
            await self.write(
                f"PSYNC {self.repl_id} {self.master_repl_offset - offset}", writer, True
            )
        print(f"Executed PSYNC for {writer.get_extra_info('peername')}")

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
            key = command[1]
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

            self.cache[key] = val
            await self.write("OK", writer, True)
            print(f"Executed SET for {writer.get_extra_info('peername')}")

            self._notify_watchers(key, writer)

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

        self._notify_watchers(key, writer)

        if self.transactions.get(writer, None) is None:
            await self.blpop_helper(key)
        else:
            queue = self.internal_queue.get(writer, [])
            queue.append(["blpop_helper", [key]])
            self.internal_queue[writer] = queue

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

        self._notify_watchers(key, writer)

        if self.transactions.get(writer, None) is None:
            await self.blpop_helper(key)
        else:
            queue = self.internal_queue.get(writer, [])
            queue.append(["blpop_helper", [key]])
            self.internal_queue[writer] = queue

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
                self._notify_watchers(command[1], writer)
            else:
                await self.write(const.NULL_STR, writer)
        else:
            await self.write(const.WRONG_TYPE, writer, True, True)

        print(f"Executed LPOP for {writer.get_extra_info('peername')}")

    async def blpop(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # BLPOP key [key...] timeout
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
                    self._notify_watchers(key, writer)
                    return

        # second pass to add BLPOPs if all lists are empty
        for key in command[1:]:
            queue = self.blpop_queue.get(key, [])
            queue.append(writer)
            self.blpop_queue[key] = queue

        written = asyncio.Event()
        self.blocked_writers[writer] = written

        # timeout
        timeout = None if timeout == 0 else timeout
        try:
            await asyncio.wait_for(written.wait(), timeout)
        except TimeoutError:
            pass

        self.blocked_writers.pop(writer, None)
        # third pass to remove BLPOPs
        for key in command[1:]:
            try:
                self.blpop_queue[key].remove(writer)
            except ValueError:
                continue

        if not written.is_set():
            await self.write(const.NULL_ARR, writer)

        print(f"Executed BLPOP for {writer.get_extra_info('peername')}")

    async def blpop_helper(self, key: str) -> None:
        # NOTE: dict.keys() returns a dictview and changes when the underlying dict changes
        queue = self.blpop_queue.get(key, [])

        for writer in queue:
            try:
                await self.write([key, self.cache[key]["value"][0]], writer)
                self._internal_lpop(["lpop", key])
                self.blocked_writers[writer].set()
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

        self._notify_watchers(key, writer)

        if self.transactions.get(writer, None) is None:
            await self.blxread_helper(key)
        else:
            queue = self.internal_queue.get(writer, [])
            queue.append(["blxread_helper", [key]])
            self.internal_queue[writer] = queue

    def _internal_stream_get(self, command: list[str]) -> list[Any]:
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

        items = self._internal_stream_get(command)

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
        stream_ids = []
        items = []
        tmp_keys = []

        for idx in range(keys):
            key_idx = stream_idx[0] + idx + 1  # idx starts from 0
            start_idx = key_idx + keys

            key = command[key_idx]
            start = command[start_idx]

            if start == "$":
                items.append([key, []])
                stream_ids.append("+")
            else:
                # _internal_stream_get already handles +
                # because (+ returns the last stream id
                com = ["XREAD", key, f"({start}", "+"]
                items.append([key, self._internal_stream_get(com)])
                stream_ids.append(start)

            tmp_keys.append(key)

        keys = tmp_keys

        if block != -1 and self._are_all_streams_empty(items):
            written = asyncio.Event()
            self.blocked_writers[writer] = written

            for idx, key in enumerate(keys):
                queue = self.blxread_queue.get(key, [])
                queue.append((writer, stream_ids[idx]))
                self.blxread_queue[key] = queue

            timeout = None if block == 0 else block / 1000
            try:
                await asyncio.wait_for(written.wait(), timeout)
            except TimeoutError:
                pass

            self.blocked_writers.pop(writer, None)
            for idx, key in enumerate(keys):
                try:
                    self.blxread_queue[key].remove((writer, stream_ids[idx]))
                except ValueError:
                    continue

            if not written.is_set():
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

    async def blxread_helper(self, key: str) -> None:
        # NOTE: dict.keys() returns a dictview and changes when the underlying dict changes
        queue = self.blxread_queue.get(key, [])
        for writer, stream_id in queue:
            try:
                stream = self._internal_stream_get(["xread", key, f"({stream_id}", "+"])
                if len(stream) != 0:
                    await self.write([[key, stream[:1]]], writer)
                    self.blocked_writers[writer].set()
                    # queue is a shallow copy (copy by reference)
                    queue.remove((writer, stream_id))
            except Exception:
                continue

    def _are_all_streams_empty(self, it_streams: list[Any]) -> bool:
        """
        Expects [[key, [streams]], [key, [streams]]]
        Returns True if all streams have length 0
        """
        count = sum(map(lambda stream: len(stream[1]), it_streams))
        return count == 0

    async def incr(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if len(command) != 2:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "incr"), writer, True, True
            )
            print(f"Executed INCR for {writer.get_extra_info('peername')}")
            return

        key = command[1]
        val = self._internal_get(key)

        try:
            val = 1 if val is None else int(val) + 1
        except ValueError:
            await self.write(const.INTEGER_OUT_OF_RANGE, writer, True, True)
            print(f"Executed INCR for {writer.get_extra_info('peername')}")
            return

        updated_val = self.cache.get(key, {})
        updated_val["value"] = str(val)
        self.cache[key] = updated_val
        await self.write(val, writer)

        print(f"Executed INCR for {writer.get_extra_info('peername')}")

        self._notify_watchers(key, writer)

    async def multi(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if len(command) != 1:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "multi"), writer, True, True
            )
            print(f"Executed MULTI for {writer.get_extra_info('peername')}")
            return
        self.transactions[writer] = []
        await self.write("OK", writer, True)

    async def exec(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if len(command) != 1:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "exec"), writer, True, True
            )
            print(f"Executed EXEC for {writer.get_extra_info('peername')}")
            return

        queue = self.transactions.get(writer, None)
        if queue is None:
            await self.write(
                const.ERR_OUTSIDE_MULTI.replace("*", command[0].upper()),
                writer,
                True,
                True,
            )
            return

        events = self.watch_events.get(writer, [])
        for modified, _ in events:
            if modified.is_set():
                await self.write(const.NULL_ARR, writer)
                await self._exec_cleanup(writer)
                return

        n = len(queue)
        await self.write(f"*{n}{const.CRLF}".encode(self.encoding), writer)

        for com in queue:
            await self.route_command(com, writer, True)

        await self._exec_cleanup(writer)
        print(f"Executed EXEC for {writer.get_extra_info('peername')}")

    async def _exec_cleanup(self, writer: asyncio.StreamWriter) -> None:
        queue = self.internal_queue.get(writer, None)
        if queue is not None:
            for com, args in queue:
                command_handler = getattr(self, com)
                await command_handler(*args)
            del self.internal_queue[writer]

        self._internal_unwatch(writer)
        del self.transactions[writer]

    async def discard(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        # the actual discard logic sits in self.route_command
        await self.write(
            const.ERR_OUTSIDE_MULTI.replace("*", command[0].upper()), writer, True, True
        )

    async def watch(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if len(command) < 2:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "watch"), writer, True, True
            )
            return

        tmp_events = []
        for key in command[1:]:
            modified = asyncio.Event()
            events = self.watched_keys.get(key, [])
            events.append((modified, writer))
            self.watched_keys[key] = events
            tmp_events.append((modified, key))

        events = self.watch_events.get(writer, [])
        events.extend(tmp_events)
        self.watch_events[writer] = events

        await self.write("OK", writer, True)

    def _notify_watchers(self, key: str, writer: asyncio.StreamWriter) -> None:
        events = self.watched_keys.get(key, [])
        for event, watching_writer in events:
            if writer == watching_writer:
                continue
            event.set()

    def _internal_unwatch(self, writer: asyncio.StreamWriter) -> None:
        events = self.watch_events.pop(writer, [])
        for event, key in events:
            watchers = self.watched_keys.get(key, [])
            try:
                watchers.remove((event, writer))
            except ValueError:
                continue

    async def unwatch(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        if len(command) != 1:
            await self.write(
                const.WRONG_NUMBER_OF_ARGS.replace("*", "unwatch"), writer, True, True
            )
            return

        self._internal_unwatch(writer)
        await self.write("OK", writer, True)

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
