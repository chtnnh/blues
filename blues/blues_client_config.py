from blues.constants import ENCODING, HOST, PORT, TIMEOUT


class BluesClientConfig:
    def __init__(
        self,
        host: str = HOST,
        port: int = PORT,
        encoding: str = ENCODING,
        timeout: float = TIMEOUT,
    ) -> None:
        self.host = host
        self.port = port
        self.encoding = encoding
        self.timeout = timeout
