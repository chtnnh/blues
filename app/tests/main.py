import asyncio

from app.main import CRLF


def encode_command(msg: bytes | int | str | list[str]) -> bytes:
    if type(msg) is bytes:
        return msg
    if type(msg) is int:
        return f":{msg}{CRLF}".encode("utf-8")
    if type(msg) is list:
        arr = "".join([f"${len(i)}{CRLF}{i}{CRLF}" for i in msg])
        return f"*{len(msg)}{CRLF}{arr}".encode("utf-8")
    return f"${len(msg)}{CRLF}{msg}{CRLF}".encode("utf-8")  # type: ignore


async def main():
    reader, writer = await asyncio.open_connection("localhost", 6379)
    print(f"Current address: {writer.get_extra_info('sockname')}")
    while True:
        msg = input("$ ")
        if msg.lower() == "exit":
            break
        command = msg.split()
        writer.write(encode_command(command))
        await writer.drain()
        res = await reader.read(1024)
        print(str(res))
    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
