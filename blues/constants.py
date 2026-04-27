from typing import Any
from zoneinfo import ZoneInfo

HOST = "localhost"
PORT = 6379
ENCODING = "utf-8"
TIMEOUT = 120.0
DEFAULT_TZ = ZoneInfo("Etc/UTC")
MSG_LIMIT = 1024

CRLF = "\r\n"

NULL_STR = "$-1\r\n".encode(ENCODING)
NULL_ARR = "*-1\r\n".encode(ENCODING)

MIN_STREAM_ID = "0-1"

UNKNOWN_COMMAND = "ERR unknown command '*'"
INVALID_COMMAND = "ResponseError: Invalid command or syntax error"
WRONG_NUMBER_OF_ARGS = "ERR wrong number of arguments for '*' command"
WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
INTEGER_OUT_OF_RANGE = "ERR value is not an integer or out of range"
WRONG_COUNT_TYPE = "ERR value is not an integer or out of range"
LOW_STREAM_ID = (
    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
)
LOWER_THAN_MIN_STREAM_ID = "ERR The ID specified in XADD must be greater than 0-0"
QUEUED = "QUEUED"
ERR_NESTED_MULTI = "ERR MULTI calls can not be nested"
ERR_OUTSIDE_MULTI = "ERR * without MULTI"
ERR_WATCH_INSIDE_MULTI = "ERR WATCH inside MULTI is not allowed"

# temporary: base64 encoding of empty RDB file
EMPTY_RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

# requires python >= 3.12
type AcceptedMessageTypes = (
    bytes | None | bool | int | float | str | list[Any] | dict[Any, Any]
)
