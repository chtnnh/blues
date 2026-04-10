import asyncio

pong = "+PONG\r\n".encode("utf-8")


class RedisServer:
    def __init__(self, host: str = "localhost", port: int = 6379) -> None:
        self.host = host
        self.port = port

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                data = await reader.read()
                data = data.decode()
                if not data:
                    break
                await self.execute_command(data, writer)
        except ConnectionError, BrokenPipeError:
            print(
                f"Client {writer.get_extra_info('peername')} disconnected unexpectedly"
            )
        finally:
            await self.disconnect_client(writer)

    async def execute_command(self, command: str, writer: asyncio.StreamWriter) -> None:
        try:
            writer.write(pong)
            await writer.drain()
        except Exception as e:
            print(f"Error responding to {writer.get_extra_info('peername')}: {e}")

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

    redis_server = RedisServer("localhost", 6379)
    asyncio.run(redis_server.start())


if __name__ == "__main__":
    main()
