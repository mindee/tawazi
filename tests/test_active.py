from typing import Any, Optional

import pytest
from tawazi import and_, dag, xn

from .common import stub


@xn
def my_add(x: Optional[int], y: Optional[int]) -> int:
    if x is not None and y is not None:
        return x + y
    return 0


@dag
def pipe() -> Optional[int]:
    return stub(1, twz_active=True)  # type: ignore[call-arg]


@dag
def pipe2() -> Optional[int]:
    return stub(1, twz_active=False)  # type: ignore[call-arg]


@dag
def pipe3(x: int) -> tuple[int, Optional[int]]:
    # if x is Truthy the value is returned
    y = stub(x, twz_active=x)  # type: ignore[call-arg]
    return x, y


@dag
def pipe4(x: str, y: str) -> tuple[str, str, Optional[str], bool]:
    b = and_(x == "twinkle", y == "toes")
    z = stub(x, twz_active=b)  # type: ignore[call-arg]
    return x, y, z, b


@dag
def pipe5(x: int, y: int) -> tuple[int, int, Optional[int], bool]:
    b = and_(x == 1, y == 2)
    z = stub(x, twz_active=b)  # type: ignore[call-arg]
    return x, y, z, b


@dag
def pipe6(x: int, y: int) -> tuple[Optional[int], Optional[int], Optional[int], Optional[float]]:
    r1 = stub(x + y, twz_active=x + y > 0)  # type: ignore[call-arg]
    r2 = stub(x - y, twz_active=x - y > 0)  # type: ignore[call-arg]
    r3 = stub(x * y, twz_active=x * y > 0)  # type: ignore[call-arg]
    r4 = stub(x / y, twz_active=x / y > 0)  # type: ignore[call-arg]

    return r1, r2, r3, r4


@dag
def pipe7(
    x: int, y: int
) -> tuple[
    Optional[int], Optional[int], Optional[int], Optional[int], Optional[int], Optional[int]
]:
    a = stub(x, twz_active=x < 0)  # type: ignore[call-arg]
    b = stub(y, twz_active=y > 0)  # type: ignore[call-arg]
    c = stub(x + 1, twz_active=x > 0)  # type: ignore[call-arg]
    d = stub(x + y, twz_active=and_(a, b))  # type: ignore[call-arg]

    imposs_1 = my_add(a, c, twz_active=and_(a, c))  # type: ignore[call-arg]

    imposs_2 = my_add(a, b, twz_active=and_(c, d))  # type: ignore[call-arg]

    return a, b, c, d, imposs_1, imposs_2


@dag
def pipe8() -> Optional[int]:
    x = True
    return stub(1, twz_active=x)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "function, input_args, expected_result",
    [
        (pipe, (), 1),  # test_active_cst
        (pipe2, (), None),  # test_active_cst
        (pipe3, (1,), (1, 1)),  # test_active_uxn
        (pipe3, (0,), (0, None)),  # test_active_uxn
        (
            pipe4,
            ("twinkle", "toes"),
            ("twinkle", "toes", "twinkle", True),
        ),  # test_active_uxn_eq_str
        (pipe4, ("hello", "world"), ("hello", "world", None, False)),  # test_active_uxn_eq_str
        (pipe5, (1, 2), (1, 2, 1, True)),  # test_active_uxn_eq_int
        (pipe5, (3, 4), (3, 4, None, False)),  # test_active_uxn_eq_int
        (pipe6, (-1, -1), (None, None, 1, 1.0)),  # test_active_operations_in_graph
        (pipe6, (-1, 1), (None, None, None, None)),  # test_active_operations_in_graph
        (pipe7, (+1, +1), (None, +1, +2, None, None, None)),  # test_active_impossible_cases_graph
        (pipe7, (+1, -1), (None, None, +2, None, None, None)),  # test_active_impossible_cases_graph
        (pipe7, (-1, +1), (-1, +1, None, 0, None, None)),  # test_active_impossible_cases_graph
        (pipe7, (-1, -1), (-1, None, None, None, None, None)),  # test_active_impossible_cases_graph
    ],
)
def test_active(function: Any, input_args: Any, expected_result: Any) -> None:
    assert function(*input_args) == expected_result


def test_active_with_setup_node() -> None:
    setop_var = 0

    @xn(setup=True)
    def setop(k: int = 1234) -> int:
        nonlocal setop_var
        setop_var += 1
        return k + 1

    @dag
    def pipe11() -> Optional[int]:
        x = False
        return setop(twz_active=x)  # type: ignore[call-arg]

    assert pipe11() is None
    assert setop_var == 0

    @dag
    def pipe12() -> Optional[int]:
        x = True
        return setop(twz_active=x)  # type: ignore[call-arg]

    assert pipe12() == 1235
    assert setop_var == 1


def test_active_with_exclude_node() -> None:
    exec_ = pipe8.executor(exclude_nodes=[stub])
    assert exec_() is None


@xn
def xn1() -> int:
    return 1


def test_active_then_deactive() -> None:
    @dag
    def pipeline_no_setup(is_active: bool) -> int:
        return xn1(twz_active=is_active)  # type: ignore[call-arg]

    assert pipeline_no_setup(True) == 1
    assert pipeline_no_setup(False) is None


@xn(setup=True)
def setup_xn1() -> int:
    return 1


@dag
def subpipeline_setup() -> int:
    #
    v = setup_xn1()
    # use stub to force the scheduler to work
    return stub(v)


@dag
def pipeline_setup(is_active: bool) -> int:
    return subpipeline_setup(twz_active=is_active == True)  # type: ignore[call-arg]  # noqa: E712


def test_subdag_with_setup() -> None:
    # assert that setup works
    assert pipeline_setup.setup() is None  # type: ignore[func-returns-value]


def test_subdag_with_setup_active_then_deactive() -> None:
    assert pipeline_setup(True) == 1
    assert pipeline_setup(False) is None
    assert pipeline_setup(True) == 1
