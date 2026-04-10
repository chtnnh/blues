import asyncio

PONG = "+PONG\r\n".encode("utf-8")
OK = "+OK\r\n".encode("utf-8")
MSG_LIMIT = 1024
CRLF = "\r\n"
ENCODING = "utf-8"


class RedisServer:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        msg_size: int = MSG_LIMIT,
        encoding: str = ENCODING,
    ) -> None:
        self.host = host
        self.port = port
        self.msg_size = msg_size
        self.encoding = encoding

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                data = await reader.read(self.msg_size)
                data = data.decode()
                if not data:
                    break
                command = self.parse_command(data)
                await self.route_command(command, writer)
        except ConnectionError, BrokenPipeError:
            print(
                f"Client {writer.get_extra_info('peername')} disconnected unexpectedly"
            )
        finally:
            await self.disconnect_client(writer)

    def parse_command(self, data: str) -> list[str]:
        command = []
        params = data.split(CRLF)
        for i in range(2, len(params), 2):
            command.append(params[i])
        return command

    def encode_response(self, msg: bytes | str | list[str]) -> bytes:
        if type(msg) is bytes:
            return msg
        if type(msg) is list:
            msg = CRLF.join(msg)
        return f"${str(len(msg))}{CRLF}{msg}{CRLF}".encode(self.encoding)

    async def route_command(
        self, command: list[str], writer: asyncio.StreamWriter
    ) -> None:
        match command[0].lower():
            case "echo":
                await self.echo(command, writer)
            case "ping":
                await self.ping(writer)

    async def write(self, msg: str, writer: asyncio.StreamWriter) -> None:
        try:
            writer.write(self.encode_response(msg))
            await writer.drain()
        except Exception as e:
            print(f"Error responding to {writer.get_extra_info('peername')}: {e}")

    async def echo(self, command: list[str], writer: asyncio.StreamWriter) -> None:
        await self.write(command[1], writer)

    async def ping(self, writer: asyncio.StreamWriter) -> None:
        await self.write(PONG, writer)

    async def disconnect_client(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        async with server:
            await server.serve_forever()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    redis_server = RedisServer("localhost", 6379, MSG_LIMIT, ENCODING)
    asyncio.run(redis_server.start())


if __name__ == "__main__":
    main()
