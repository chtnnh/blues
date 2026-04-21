import asyncio

from blues.blues_server import BluesServer
from blues.constants import DEFAULT_TZ, ENCODING, HOST, MSG_LIMIT, PORT


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    blues_server = BluesServer(HOST, PORT, MSG_LIMIT, ENCODING, DEFAULT_TZ)
    asyncio.run(blues_server.start())


if __name__ == "__main__":
    main()
