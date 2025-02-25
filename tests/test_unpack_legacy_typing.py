# ruff: noqa
from __future__ import annotations

from typing import Tuple, cast

from tawazi import xn, dag


class A(int):
    pass


# can not be parametrized using pytest...
def test_future_typing() -> None:
    @xn(unpack_to=2)
    def return_tuple() -> Tuple[int, int]:
        return 1, 2

    @dag
    def my_dag() -> Tuple[int, int]:
        return return_tuple()

    assert my_dag() == (1, 2)


def test_future_typing_custom_class() -> None:
    @xn(unpack_to=2)
    def return_tuple() -> Tuple[A, A]:
        return cast(A, 1), cast(A, 2)

    @dag
    def my_dag() -> Tuple[int, int]:
        return return_tuple()

    assert my_dag() == (1, 2)


def test_future_typing_int_ellipsis() -> None:
    @xn(unpack_to=2)
    def return_tuple() -> Tuple[int, ...]:
        return 1, 2, 3

    @dag
    def my_dag() -> Tuple[int, ...]:
        return return_tuple()

    assert my_dag() == (1, 2)
