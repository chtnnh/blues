# Redis in Python

### How to implement this with an event loop?

The event loop should be the main thread. This thread will have the server that accepts connections.
Every time a connection receives a command, it's pushed to a stack. We will have a PC like data structure, if it's empty, we pop the stack

### Pointers
- `socket.recv()` blocks until there is a byte available to read or the connection is closed
- There is a flag that can be set to make sockets non-blocking (reddit thinks that complicates things)

### Resources
- [Async + Sockets Notes](https://github.com/xbeat/Machine-Learning/blob/main/Efficient%20Asynchronous%20Socket%20Programming%20with%20asyncio%20in%20Python.md)
