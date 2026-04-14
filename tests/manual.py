import asyncio

from tests.utils import encode_command


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
        print(res.decode("utf-8"))
    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
