from zoneinfo import ZoneInfo

import blues.constants as const


class BluesServerConfig:
    def __init__(
        self,
        host: str = const.HOST,
        port: int = const.PORT,
        msg_size: int = const.MSG_LIMIT,
        encoding: str = const.ENCODING,
        timezone: ZoneInfo = const.DEFAULT_TZ,
        replica_of: str = "",
    ) -> None:
        self.host = host
        self.port = port
        self.msg_size = msg_size
        self.encoding = encoding
        self.timezone = timezone
        self.replica_of = replica_of
