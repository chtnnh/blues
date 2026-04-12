from typing import Any


def flatten(it: list[tuple[Any, Any]]) -> list[Any]:
    # https://stackoverflow.com/a/51291027/9963797
    return [item for sublist in it for item in sublist]
