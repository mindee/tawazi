import os
from typing import Any, Tuple

from tawazi import dag, xn


def test_non_pickalable_args() -> None:
    @xn
    def a(in1: Any = 1) -> Any:
        if isinstance(in1, int):
            return in1 + 1
        return in1

    @xn
    def b(in1: int, in2: int) -> int:
        return in1 + in2

    @dag
    def pipe() -> Tuple[Any, int]:
        return a(os), b(a(10), a())

    assert pipe() == (os, 13)
