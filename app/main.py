import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    pong = r"+PONG\r\n".encode("utf-8")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()
    conn, return_address = server_socket.accept()

    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            conn.sendto(pong, return_address)


if __name__ == "__main__":
    main()
