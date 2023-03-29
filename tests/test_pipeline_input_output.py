from typing import Any, List, Tuple

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziArgumentException, TawaziBaseException


@xn
def a(input_img: List[int], cst: int) -> int:
    return sum(input_img) + cst


@xn
def lazy_print(*args: Any) -> None:
    print(*args)  # noqa: T201


@dag
def declare_dag_function(input_img: List[int], cst: int = 0) -> int:
    lazy_print(cst)
    return a(input_img, cst)


@xn
def op1(in1: int) -> int:
    return in1 + 1


def test_pipeline_input_output() -> None:
    assert declare_dag_function([1, 2, 3], 10) == 16


def test_pipeline_input_output_skipping_default_params() -> None:
    assert declare_dag_function([1, 2, 3]) == 6


def test_pipeline_input_output_missing_argument() -> None:
    with pytest.raises(TawaziBaseException):
        declare_dag_function()  # type: ignore[call-arg]


def test_pipeline_default_args_input_not_provided() -> None:
    @dag
    def pipe(in1: int = 1, in2: int = 2, in3: int = 3, in4: int = 4) -> Tuple[int, ...]:
        return op1(in1), op1(in2), op1(in3), op1(in4)

    assert pipe() == (2, 3, 4, 5)


def test_pipeline_args_input_not_provided() -> None:
    # should fail!!
    @dag
    def pipe(in1: int, in2: int, in3: int, in4: int) -> Tuple[int, ...]:
        return op1(in1), op1(in2), op1(in3), op1(in4)

    with pytest.raises(TawaziArgumentException):
        pipe()  # type: ignore[call-arg]
