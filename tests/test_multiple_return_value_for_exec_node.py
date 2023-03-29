from typing import Dict, List, Tuple

import pytest
from tawazi import dag, xn


def test_dict_indexed() -> None:
    @xn
    def generate_dict() -> Dict[str, int]:
        return {"1": 1, "2": 2, "3": 3}

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe() -> Tuple[int, int, int]:
        d = generate_dict()
        return incr(d["1"]), incr(d["2"]), incr(d["3"])

    assert (2, 3, 4) == pipe()


def test_tuple_indexed() -> None:
    @xn
    def generate_tuple() -> Tuple[int, ...]:
        return (1, 2, 3, 4)

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe() -> Tuple[int, int, int]:
        d = generate_tuple()
        return incr(d[0]), incr(d[1]), incr(d[2])

    assert (2, 3, 4) == pipe()


def test_list_indexed() -> None:
    @xn
    def generate_list() -> List[int]:
        return [1, 2, 3, 4]

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe() -> List[int]:
        d = generate_list()
        return [incr(d[0]), incr(d[1]), incr(d[2])]

    assert [2, 3, 4] == pipe()


def test_multiple_index() -> None:
    @xn
    def generate_nested_list() -> Tuple[List[int], int, Tuple[int, Tuple[int]]]:
        return ([1], 2, (3, (4,)))

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe() -> List[int]:
        d = generate_nested_list()
        return [incr(d[0][0]), incr(d[1]), incr(d[2][0]), incr(d[2][1][0])]

    assert [2, 3, 4, 5] == pipe()


def test_multiple_index_reused() -> None:
    @xn
    def generate_nested_list() -> Tuple[List[int], int, Tuple[int, Tuple[int]]]:
        return ([1], 2, (3, (4,)))

    @xn
    def incr(a: int) -> int:
        return a + 1

    @xn
    def add(a: int, b: int) -> int:
        return a + b

    @dag
    def pipe() -> Tuple[int, ...]:
        d = generate_nested_list()
        d0, d1, d2, d3 = d[0][0], d[1], d[2][0], d[2][1][0]
        d0_, d1_, d2_, d3_ = incr(d0), incr(d1), incr(d2), incr(d3)
        return add(d0_, d0), add(d1_, d1), add(d2_, d2), add(d3_, d3)

    assert (3, 5, 7, 9) == pipe()


def test_mrv() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        r1, r2, _r3 = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_mrv_in_tuple() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        (r1, r2, _r3) = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_mrv_in_list() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        [r1, r2, _r3] = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_mrv_wrong_bigger_unpack_to_number() -> None:
    @xn(unpack_to=4)
    def mulreturn() -> Tuple[int, int, int, int]:
        return 1, 2, 3  # type: ignore[return-value]

    with pytest.raises(ValueError):

        @dag
        def pipe() -> Tuple[int, int]:
            r1, r2, _r3 = mulreturn()  # type: ignore[misc]
            return r1, r2  # type: ignore[has-type]


def test_mrv_wrong_lower_unpack_to_number() -> None:
    @xn(unpack_to=1)
    def mulreturn() -> Tuple[int]:
        return 1, 2, 3  # type: ignore[return-value]

    with pytest.raises(ValueError):

        @dag
        def pipe() -> Tuple[int, int]:
            r1, r2, _r3 = mulreturn()  # type: ignore[misc]
            return r1, r2  # type: ignore[has-type]


# test multiple return values for exec node without typing
def test_mrv_without_typing() -> None:
    @xn(unpack_to=3)
    def mulreturn():  # type: ignore[no-untyped-def]
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        r1, r2, _r3 = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


# test multiple return values for exec node with tuple typed ellipsis
def test_mrv_with_tuple_typed_ellipsis() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, ...]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        r1, r2, _r3 = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


# test multiple return values for exec ndoe with list typed ellipsis edge case (unpack_to=1)
def test_mrv_with_tuple_typed_ellipsis_1() -> None:
    @xn(unpack_to=1)
    def mulreturn() -> Tuple[int, ...]:
        return (1,)

    @dag
    def pipe() -> Tuple[int, int]:
        (r1,) = mulreturn()
        return r1, r1

    assert pipe() == (1, 1)


# test multiple return values for exec node wrong unpack_to number in typing
def test_mrv_wrong_unpack_to_number() -> None:
    with pytest.raises(
        ValueError, match="unpack_to must be equal to the number of elements in the type of return"
    ):

        @xn(unpack_to=4)
        def mulreturn() -> Tuple[int, int, int]:
            return 1, 2, 3


def test_mrv_inline_unpack_to_specify() -> None:
    @xn
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int, int, Tuple[int, int, int], Tuple[int, int, int]]:
        tuple_v = mulreturn()
        (r1, r2, r3) = mulreturn(twz_unpack_to=3)  # type: ignore[call-arg]
        tuple_vv = mulreturn()
        return r1, r2, r3, tuple_v, tuple_vv

    assert pipe() == (1, 2, 3, (1, 2, 3), (1, 2, 3))


def test_mrv_unpack_to_with_list_type() -> None:
    @xn
    def mulreturn() -> List[int]:
        return [1, 2, 3]

    @dag
    def pipe() -> Tuple[int, int, int, List[int]]:
        r1, r2, r3 = mulreturn(twz_unpack_to=3)  # type: ignore[call-arg]
        list_v = mulreturn()
        return r1, r2, r3, list_v

    assert pipe() == (1, 2, 3, [1, 2, 3])
