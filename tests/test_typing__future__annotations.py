from __future__ import annotations

from typing import Any

from tawazi import dag, xn


@xn(unpack_to=2)
def _lcat(item: list[Any]) -> tuple[list[Any], list[list[int]]]:
    return item, item


@dag
def _pipe() -> tuple[list[int], list[int]]:
    return _lcat([1, 2, 3])


assert _pipe() == ([1, 2, 3], [1, 2, 3])
