from datetime import timezone
from typing import Any

HOST = "localhost"
PORT = 6379
ENCODING = "utf-8"
TIMEOUT = 5.0
DEFAULT_TZ = timezone.utc
MSG_LIMIT = 1024

CRLF = "\r\n"

NULL_STR = "$-1\r\n".encode(ENCODING)
NULL_ARR = "*-1\r\n".encode(ENCODING)

MIN_STREAM_ID = "0-1"

UNKNOWN_COMMAND = "ERR unknown command '*'"
WRONG_NUMBER_OF_ARGS = "ERR wrong number of arguments for '*' command"
WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
LOW_STREAM_ID = (
    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
)
LOWER_THAN_MIN_STREAM_ID = "ERR The ID specified in XADD must be greater than 0-0"

# requires python >= 3.12
type AcceptedMessageTypes = (
    bytes | None | bool | int | float | str | list[Any] | dict[Any, Any]
)
