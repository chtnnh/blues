from typing import Any

from blues.main import CRLF, ENCODING


def encode_command(
    msg: bytes | int | str | list[Any],
    isSimple: bool = False,
    isError: bool = False,
) -> bytes:
    if isSimple:
        if isError:
            return f"-{msg}{CRLF}".encode(ENCODING)
        return f"+{msg}{CRLF}".encode(ENCODING)
    if type(msg) is bytes:
        return msg
    if type(msg) is int:
        return f":{msg}{CRLF}".encode(ENCODING)
    if type(msg) is list:
        arr = "".join([encode_command(i).decode(ENCODING) for i in msg])
        return f"*{len(msg)}{CRLF}{arr}".encode(ENCODING)
    return f"${len(msg)}{CRLF}{msg}{CRLF}".encode(ENCODING)  # type: ignore
