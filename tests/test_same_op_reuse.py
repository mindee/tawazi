from typing import Tuple

from tawazi import dag, xn


@xn
def a(v: int) -> int:
    return v + 1


@dag
def pipe() -> Tuple[int, int]:
    a_1 = a(1)
    a_2 = a(2)

    return a_1, a_2


def test_function_reuse() -> None:
    a_1, a_2 = pipe()

    assert (a_1, a_2) == (2, 3)
