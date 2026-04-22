import asyncio

from blues.constants import CRLF, ENCODING, AcceptedMessageTypes


class BluesStanzaProtocolAsync:
    def __init__(self, encoding: str = ENCODING) -> None:
        self.encoding = encoding
        return

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

    async def decode(self, reader: asyncio.StreamReader) -> AcceptedMessageTypes:
        data = await reader.readline()
        data = data.decode(self.encoding)
        msg_type, msg = data[:1], data[1:-2]
        match msg_type:
            case "*":
                # TODO: return a flag for null arr
                if msg == "-1":
                    return []
                return [await self.decode(reader) for _ in range(int(msg))]
            case "$" | "!":
                # TODO: return a flag for errors
                # don't do reader.read(int(msg)) to avoid having to drain the reader later
                # TODO: return a flag for null str
                if msg == "-1":
                    return ""
                bulk = await reader.readline()
                bulk = bulk.decode(self.encoding)
                return bulk[: int(msg)]
            case ":" | "(":
                return int(msg)
            case ",":
                return float(msg)
            case "+" | "-":
                # TODO: return a flag for errors
                return msg
            case "_":
                return None
            case "#":
                return msg == "t"
            case "%":
                res = {}
                for _ in range(int(msg)):
                    key = await self.decode(reader)
                    res[key] = await self.decode(reader)
                return res
