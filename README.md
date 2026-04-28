# blues

_redis_ subset in python3

0 AI. Check out ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis) to do something similar!

### Motivation
1. get some additional practice with async programming in python3
2. get better with pytest-asyncio and hypothesis
3. (some) learning on data structures and redis internals

### What's done
1. Blues Stanza Protocol (RESP2 compatible)
2. Blues Server (async), (strings, lists, streams) TODO: list implemented commands
3. Blues Client (async)
4. Blues CLI Client (async), mostly for manual testing, but pretty functional
5. Transactions
6. Optimistic Locking
7. Replication

### What's coming
1. Persistence
2. More if I can finish the above before the free challenge changes

### Dependencies
1. [Typer](https://typer.tiangolo.com/) - amazing library for Python CLIs

### Dev Tools
1. [uv](https://docs.astral.sh/uv/)
2. [pytest](https://docs.pytest.org/en/stable/)
3. [hypothesis](https://hypothesis.readthedocs.io/en/latest/)
4. [Zed](https://zed.dev/)

### Attribution and Thanks
1. ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis)
2. [StringTrie implementation](https://github.com/mina86/pygtrie/)
3. [Typer](https://typer.tiangolo.com/)

### License
[Apache 2.0](LICENSE)

### Author
© Chaitanya Mittal, 2026

### Notice
Reach out if I have missed out an attribution or something needs to be removed! No harm intended.
