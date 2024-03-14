from __future__ import annotations

import sys
from typing import Tuple, cast

from tawazi import xn


class A(int):
    pass


# can not be parametrized using pytest...
def test_future_typing() -> None:
    if sys.version_info < (3, 9):

        @xn(unpack_to=2)
        def return_tuple() -> Tuple[int, int]:  # noqa: UP006
            return 1, 2

    else:

        @xn(unpack_to=2)
        def return_tuple() -> tuple[int, int]:
            return 1, 2


def test_future_typing_custom_class() -> None:
    if sys.version_info < (3, 9):

        @xn(unpack_to=2)
        def return_tuple() -> Tuple[A, A]:  # noqa: UP006
            return cast(A, 1), cast(A, 2)

    else:

        @xn(unpack_to=2)
        def return_tuple() -> tuple[A, A]:
            return cast(A, 1), cast(A, 2)


def test_future_typing_int_ellipsis() -> None:
    if sys.version_info < (3, 9):

        @xn(unpack_to=2)
        def return_tuple() -> Tuple[int, ...]:  # noqa: UP006
            return 1, 2, 3

    else:

        @xn(unpack_to=2)
        def return_tuple() -> tuple[int, ...]:
            return 1, 2, 3
