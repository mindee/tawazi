from typing import Dict, List, Tuple

import pytest
from tawazi import Cfg, dag, xn
from tawazi.errors import TawaziUsageError


def test_lazy_exec_nodes_return_dict_indexed() -> None:
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


def test_lazy_exec_nodes_return_tuple_indexed() -> None:
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


def test_lazy_exec_nodes_return_list_indexed() -> None:
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


def test_lazy_exec_nodes_return_multiple_index() -> None:
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


def test_lazy_exec_nodes_return_multiple_index_reused() -> None:
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


def test_lazy_exec_nodes_multiple_return_values() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        r1, r2, _r3 = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_lazy_exec_nodes_multiple_return_values_in_tuple() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        (r1, r2, _r3) = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_lazy_exec_nodes_multiple_return_values_in_list() -> None:
    @xn(unpack_to=3)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def pipe() -> Tuple[int, int]:
        [r1, r2, _r3] = mulreturn()
        return r1, r2

    assert pipe() == (1, 2)


def test_lazy_exec_nodes_multiple_return_values_wrong_bigger_unpack_to_number() -> None:
    @xn(unpack_to=4)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    with pytest.raises(ValueError):

        @dag
        def pipe() -> Tuple[int, int]:
            r1, r2, _r3 = mulreturn()
            return r1, r2


def test_lazy_exec_nodes_multiple_return_values_wrong_lower_unpack_to_number() -> None:
    @xn(unpack_to=1)
    def mulreturn() -> Tuple[int, int, int]:
        return 1, 2, 3

    with pytest.raises(ValueError):

        @dag
        def pipe() -> Tuple[int, int]:
            r1, r2, _r3 = mulreturn()
            return r1, r2


# this is an edge case
def test_multiple_return_values_unpack_no_unpack_to_specified_must_be_one() -> None:
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True

    @xn
    def tuple_one() -> Tuple[int]:
        return (1,)

    @dag
    def pipe() -> Tuple[int, int]:
        (v1,) = tuple_one()
        return v1, v1

    assert pipe() == (1, 1)


def test_multiple_return_values_unpack_no_unpack_to_specified_must_be_one_edge_case() -> None:
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True

    @xn
    def tuple_one() -> Tuple[int]:
        return (1,)

    @dag
    def pipe() -> Tuple[Tuple[int], Tuple[int]]:
        v1, v2 = tuple_one(), tuple_one()
        return v1, v2

    assert pipe() == ((1,), (1,))


def test_multiple_return_values_unpack_no_unpack_to_specified_must_be_two() -> None:
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True

    @xn
    def tuple_two() -> Tuple[int, int]:
        return 1, 2

    @dag
    def pipe() -> Tuple[int, int]:
        v1, v2 = tuple_two()
        return v1, v2

    assert pipe() == (1, 2)


def test_multiple_return_values_unpack_no_unpack_to_specified_must_be_three() -> None:
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True

    @xn
    def tuple_three() -> Tuple[int, int, int]:
        return 1, 2, 3

    @xn
    def incr(a: int) -> int:
        return a + 1

    @dag
    def pipe() -> Tuple[int, int, int]:
        v1, v2, v3 = tuple_three()
        v1, v2, v3 = incr(v1), incr(v2), incr(v3)
        return v1, v2, v3

    assert pipe() == (2, 3, 4)


def test_multiple_return_values_unpack_no_unpack_to_specified_must_be_two_left_less_than_right_side() -> (
    None
):
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True

    @xn(unpack_to=3)
    def tuple_three() -> Tuple[int, int, int]:
        return 1, 2, 3

    # ValueError: too many values to unpack (expected 2)
    with pytest.raises(ValueError, match="too many values to unpack \\(expected 2\\)"):

        @dag
        def pipe() -> None:
            v1, v2 = tuple_three()


def test_multiple_return_values_unpack_no_unpack_to_specified_must_be_two_left_more_than_right_side() -> (
    None
):
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True

    @xn(unpack_to=3)
    def tuple_three() -> Tuple[int, int, int]:
        return 1, 2, 3

    # ValueError: too many values to unpack (expected 2)
    with pytest.raises(ValueError, match="not enough values to unpack \\(expected 4, got 3\\)"):

        @dag
        def pipe() -> None:
            v1, v2, v3, v4 = tuple_three()


def test_multiple_return_values_unpack_no_unpack_maximum_trial_expanded() -> None:
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = True
    Cfg.TAWAZI_EXPERIMENTAL_MAX_UNPACK_TRIAL_ITERATIONS = 4

    @xn
    def tuple_three() -> Tuple[int, int, int]:
        return 1, 2, 3

    @dag
    def _pipe() -> None:
        v1, v2, v3 = tuple_three()

    @dag
    def _pipe() -> None:
        v1, v2, v3 = tuple_three()
        v1, v2, v3 = tuple_three()

    # adapt TAWAZI_EXPERIMENTAL_MAX_UNPACK_TRIAL_ITERATIONS
    #  so that this test fails but the previous ones do not fail
    # this is test is important to ensure that there is never an infinite loop
    with pytest.raises(TawaziUsageError):

        @dag
        def _pipe() -> None:
            v1, v2, v3 = tuple_three()
            v1, v2, v3 = tuple_three()
            v1, v2, v3 = tuple_three()


def test_multiple_return_values_unpack_no_unpack_experimental_is_not_activated() -> None:
    Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK = False

    @xn
    def tuple_three() -> Tuple[int, int, int]:
        return 1, 2, 3

    with pytest.raises(ValueError):

        @dag
        def _pipe() -> None:
            v1, v2 = tuple_three()
