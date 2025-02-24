import sys

import pytest
from tawazi import dag, xn


@xn
def incr(a: int) -> int:
    return a + 1


@xn
def generate_dict() -> dict[str, int]:
    return {"1": 1, "2": 2, "3": 3}


@xn
def generate_tuple() -> tuple[int, ...]:
    return 1, 2, 3, 4


@xn
def generate_list() -> list[int]:
    return [1, 2, 3, 4]


@xn
def generate_nested_list() -> tuple[list[int], int, tuple[int, tuple[int]]]:
    return [1], 2, (3, (4,))


@xn
def add(a: int, b: int) -> int:
    return a + b


@xn(unpack_to=3)
def mulreturn() -> tuple[int, int, int]:
    return 1, 2, 3


def test_dict_indexed() -> None:
    @dag
    def pipe() -> tuple[int, int, int]:
        d = generate_dict()
        return incr(d["1"]), incr(d["2"]), incr(d["3"])

    assert (2, 3, 4) == pipe()


def test_tuple_indexed() -> None:
    @dag
    def pipe() -> tuple[int, int, int]:
        d = generate_tuple()
        return incr(d[0]), incr(d[1]), incr(d[2])

    assert (2, 3, 4) == pipe()


def test_list_indexed() -> None:
    @dag
    def pipe() -> list[int]:
        d = generate_list()
        return [incr(d[0]), incr(d[1]), incr(d[2])]

    assert [2, 3, 4] == pipe()


def test_multiple_index() -> None:
    @dag
    def pipe() -> list[int]:
        d = generate_nested_list()
        return [incr(d[0][0]), incr(d[1]), incr(d[2][0]), incr(d[2][1][0])]

    assert [2, 3, 4, 5] == pipe()


def test_multiple_index_reused() -> None:
    @dag
    def pipe() -> tuple[int, ...]:
        d = generate_nested_list()
        d0, d1, d2, d3 = d[0][0], d[1], d[2][0], d[2][1][0]
        d0_, d1_, d2_, d3_ = incr(d0), incr(d1), incr(d2), incr(d3)
        return add(d0_, d0), add(d1_, d1), add(d2_, d2), add(d3_, d3)

    assert (3, 5, 7, 9) == pipe()


def test_mrv() -> None:
    @dag
    def pipe() -> tuple[int, int]:
        r1, r2, _r3 = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_mrv_in_tuple() -> None:
    @dag
    def pipe() -> tuple[int, int]:
        (r1, r2, _r3) = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_mrv_in_list() -> None:
    @dag
    def pipe() -> tuple[int, int]:
        [r1, r2, _r3] = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_mrv_wrong_bigger_unpack_to_number() -> None:
    @xn(unpack_to=4)
    def _mulreturn() -> tuple[int, int, int, int]:
        return 1, 2, 3  # type: ignore[return-value]

    with pytest.raises(ValueError):

        @dag
        def pipe() -> tuple[int, int]:
            r1, r2, _r3 = _mulreturn()  # type: ignore[misc]
            return r1, r2


def test_mrv_wrong_lower_unpack_to_number() -> None:
    @xn(unpack_to=1)
    def _mulreturn() -> tuple[int]:
        return 1, 2, 3  # type: ignore[return-value]

    with pytest.raises(ValueError):

        @dag
        def pipe() -> tuple[int, int]:
            r1, r2, _r3 = _mulreturn()  # type: ignore[misc]
            return r1, r2


# test multiple return values for exec node without typing
def test_mrv_without_typing() -> None:
    @xn(unpack_to=3)
    def _mulreturn():  # type: ignore[no-untyped-def]
        return 1, 2, 3

    @dag
    def pipe() -> tuple[int, int]:
        r1, r2, _r3 = _mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


# test multiple return values for exec node with tuple typed ellipsis
def test_mrv_with_tuple_typed_ellipsis() -> None:
    @xn(unpack_to=3)
    def _mulreturn() -> tuple[int, ...]:
        return 1, 2, 3

    @dag
    def pipe() -> tuple[int, int]:
        r1, r2, _r3 = _mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


# test multiple return values for exec ndoe with list typed ellipsis edge case (unpack_to=1)
def test_mrv_with_tuple_typed_ellipsis_1() -> None:
    @xn(unpack_to=1)
    def _mulreturn() -> tuple[int, ...]:
        return (1,)

    @dag
    def pipe() -> tuple[int, int]:
        (r1,) = _mulreturn()
        return r1, r1

    assert pipe() == (1, 1)


# test multiple return values for exec node wrong unpack_to number in typing
def test_mrv_wrong_unpack_to_number() -> None:
    with pytest.raises(
        ValueError, match="unpack_to must be equal to the number of elements in the type of return"
    ):

        @xn(unpack_to=4)
        def _mulreturn() -> tuple[int, int, int]:
            return 1, 2, 3


def test_mrv_inline_unpack_to_specify() -> None:
    @xn
    def _mulreturn() -> tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> tuple[int, int, int, tuple[int, int, int], tuple[int, int, int]]:
        tuple_v = _mulreturn()
        (r1, r2, r3) = _mulreturn(twz_unpack_to=3)  # type: ignore[call-arg]
        tuple_vv = _mulreturn()
        return r1, r2, r3, tuple_v, tuple_vv

    assert pipe() == (1, 2, 3, (1, 2, 3), (1, 2, 3))


def test_mrv_unpack_to_with_list_type() -> None:
    @xn
    def _mulreturn() -> list[int]:
        return [1, 2, 3]

    @dag
    def pipe() -> tuple[int, int, int, list[int]]:
        r1, r2, r3 = _mulreturn(twz_unpack_to=3)  # type: ignore[call-arg]
        list_v = _mulreturn()
        return r1, r2, r3, list_v

    assert pipe() == (1, 2, 3, [1, 2, 3])


# if python version is 3.10 or higher, include this function
if sys.version_info >= (3, 10):
    from typing import Union

    from tawazi import xn

    union_type = Union[tuple[float, int], tuple[float, float]]
    py311_union_type = tuple[float, int] | tuple[float, float]

    @xn(unpack_to=2)
    def union_py39(x: float, integer: bool = False) -> union_type:
        return x, x / 2 if not integer else int(x / 2)

    @dag
    def dag_union_py39(x):
        return union_py39(x)

    def test_unpacking_union() -> None:
        assert 1, 1 / 2 == dag_union_py39(1)

    @xn(unpack_to=2)
    def union_py310(x: float, integer: bool = False) -> py311_union_type:
        return x, x / 2 if not integer else int(x / 2)

    @dag
    def dag_union_py310(x):
        return union_py310(x)

    def test_unpacking_union_py310() -> None:
        assert 2, 1 == dag_union_py310(2)
