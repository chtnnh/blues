from shlex import shlex
from typing import Any

from blues.blues_async_client import BluesAsyncClient
from blues.constants import ENCODING, HOST, PORT, TIMEOUT, AcceptedMessageTypes


class BluesCliClient(BluesAsyncClient):
    def __init__(
        self,
        host: str = HOST,
        port: int = PORT,
        encoding: str = ENCODING,
        timeout: float = TIMEOUT,
    ) -> None:
        super().__init__()

    def _parse_list(self, input: list[str], index: int) -> tuple[list[Any], int]:
        if input[index] != "[":
            print(f"Unexpected char {input[index]}")
            raise ValueError

        res = []
        index += 1

        while index < len(input):
            token = input[index]
            match token:
                case ",":
                    index += 1
                    continue
                case "[":
                    val, index = self._parse_list(input, index)
                    res.append(val)
                case "{":
                    val, index = self._parse_dict(input, index)
                    res.append(val)
                case "]":
                    return res, index + 1
                case ":" | "}":
                    print(f"Unexpected char {token}")
                    raise ValueError
                case _:
                    res.append(token)
            index += 1

        return res, index

    def _parse_dict(self, input: list[str], index: int) -> tuple[dict[Any, Any], int]:
        if input[index] != "{":
            print(f"Unexpected char {input[index]}")
            raise ValueError

        res = {}
        index += 1
        key = None

        while index < len(input):
            token = input[index]
            match token:
                case ":" | ",":
                    index += 1
                    continue
                case "[":
                    if key is None:
                        print(f"Unexpected char {token}")
                        raise ValueError
                    val, index = self._parse_list(input, index)
                    res[key] = val
                case "{":
                    if key is None:
                        print(f"Unexpected char {token}")
                        raise ValueError
                    val, index = self._parse_dict(input, index)
                    res[key] = val
                case "}":
                    return res, index + 1
                case "]":
                    print(f"Unexpected char {token}")
                    raise ValueError
                case _:
                    if key is None:
                        key = token
                    else:
                        res[key] = token
                        key = None
            index += 1

        return res, index

    def command_parser(self, input: list[str]) -> AcceptedMessageTypes:
        command: list[Any] = [input[0]]
        num = len(input)

        if num == 1:
            return command

        index = 1

        while index < num:
            token = input[index]
            match token:
                case "[":
                    val, index = self._parse_list(input, index)
                    command.append(val)
                case "{":
                    val, index = self._parse_dict(input, index)
                    command.append(val)
                case "]" | "}" | ":":
                    print(f"Unexpected char {token}")
                    raise ValueError
                case ",":
                    continue
                case _:
                    command.append(token)
            index += 1

        return command

    async def command(self) -> AcceptedMessageTypes:
        user_input = input("$ ")
        if user_input.lower() == "exit":
            raise KeyboardInterrupt

        lexer = shlex(user_input, punctuation_chars=True)

        # for xrange command
        lexer._punctuation_chars.replace("()", "")  # type: ignore
        # handle list and dict in cli input
        lexer._punctuation_chars += "[]{}:"  # type: ignore

        try:
            com = self.command_parser(list(lexer))
        except ValueError:
            return

        await super().write(com)
        return await super().read()

    async def run(self) -> None:
        await super().create()
        print(f"Current address: {self.writer.get_extra_info('sockname')}")
        while True:
            try:
                output = await self.command()
                print(output)
            except KeyboardInterrupt:
                await super().close()
                break
