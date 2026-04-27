import asyncio

from blues.constants import CRLF, ENCODING, AcceptedMessageTypes


class BluesSPDecodeError(Exception):
    def __init__(self, message: str = "Failed to decode from reader.") -> None:
        super().__init__(message)


class BluesStanzaProtocolAsync:
    def __init__(self, encoding: str = ENCODING) -> None:
        self.encoding = encoding

    def encode(
        self,
        msg: AcceptedMessageTypes,
        isSimple: bool = False,
        isError: bool = False,
    ) -> bytes:
        """
        Encode accepted message types to bytes array using Blues Stanza Protocol.

        Handles None, bool, int, float, str, list[Any], dict[Any, Any].
        Also supports simple string, simple error, bulk error, null string and null array.
        """

        command = ""

        if msg is None:
            command = f"_{CRLF}"

        elif isSimple:
            if isError:
                command = f"-{msg}{CRLF}"
            else:
                command = f"+{msg}{CRLF}"

        else:
            match msg:
                case bytes():
                    return msg

                case bool():
                    if msg:
                        command = f"#t{CRLF}"
                    else:
                        command = f"#f{CRLF}"

                case int():
                    command = f":{msg}{CRLF}"

                case float():
                    command = f",{msg}{CRLF}"

                case str():
                    if isError:
                        command = f"!{len(msg)}{CRLF}{msg}{CRLF}"
                    else:
                        command = f"${len(msg)}{CRLF}{msg}{CRLF}"

                case list():
                    arr = "".join([self.encode(i).decode(self.encoding) for i in msg])
                    command = f"*{len(msg)}{CRLF}{arr}"

                case dict():
                    items = "".join(
                        [
                            self.encode(item).decode(self.encoding)
                            for item_tuple in msg.items()
                            for item in item_tuple
                        ]
                    )
                    command = f"%{len(msg)}{CRLF}{items}"

        return command.encode(self.encoding)

    async def decode(
        self, reader: asyncio.StreamReader
    ) -> tuple[AcceptedMessageTypes, bool, bool, bool]:
        """
        Decode bytes array from reader to accepted message types using Blues Stanza Protocol.

        Handles None, bool, int, float, str, list[Any], dict[Any, Any].
        Also supports simple string, simple error, bulk error, null string and null array.

        Returns: res, error, is_null, is_error
        """

        error = False
        is_null = False
        is_error = False

        data = await reader.readline()
        if not data:
            error = True
            return None, error, is_null, is_error

        data = data.decode(self.encoding)

        msg_type, msg = data[:1], data[1:-2]

        match msg_type:
            case "*":
                res = []
                if msg == "-1":
                    is_null = True
                else:
                    for _ in range(int(msg)):
                        # TODO: handle, is_null and is_error
                        item, err, *_ = await self.decode(reader)
                        if err:
                            raise BluesSPDecodeError
                        res.append(item)

            case "$" | "!":
                if msg_type == "!":
                    is_error = True

                if msg == "-1":
                    is_null = True
                    res = ""

                else:
                    bulk = await reader.read(int(msg) + 2)
                    bulk = bulk.decode(self.encoding)
                    res = bulk[:-2]

            case ":" | "(":
                res = int(msg)

            case ",":
                res = float(msg)

            case "+":
                res = msg

            case "-":
                is_error = True
                res = msg

            case "_":
                is_null = True
                res = None

            case "#":
                res = msg == "t"

            case "%":
                res = {}
                for _ in range(int(msg)):
                    # TODO: handle is_null and is_error
                    key, err, *_ = await self.decode(reader)
                    if err:
                        raise BluesSPDecodeError
                    res[key], err, *_ = await self.decode(reader)
                    if err:
                        raise BluesSPDecodeError

            case _:
                res = None
                error = True

        return res, error, is_null, is_error
