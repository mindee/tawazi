# this is a file that describes using Tawazi without being protected from mypy!
# it is tested during the formatting process.

from typing import Tuple

import pytest
from tawazi import dag, xn
from tawazi.errors import ErrorStrategy


@xn
def function_no_parameter() -> None:
    return


@xn
def function_with_parameter(x: int) -> None:
    return


@xn
def function_with_multiple_parameters(x: int, y: str) -> None:
    return


@xn
def function_with_multiple_parameters_and_return(x: int = 1, y: str = "2") -> int:
    return x + int(y)


@xn
def function_with_multiple_parameters_and_return2(x: int) -> Tuple[int, str]:
    return x, str(x)


@xn
def pass_in_tuple(x: Tuple[int, str]) -> int:
    return x[0] + int(x[1])


@dag
def pipe(input: int) -> Tuple[int, str]:
    _none = function_no_parameter()
    _none = function_with_parameter(1234)
    _var = function_with_multiple_parameters_and_return(123, "1234")
    tt = function_with_multiple_parameters_and_return2(500)
    _summ = pass_in_tuple(tt)

    function_with_parameter(1234)

    return tt


@dag(behavior=ErrorStrategy.strict)
def pipe_configured(input: int) -> Tuple[int, str]:
    _none = function_no_parameter()
    _none = function_with_parameter(1234)
    _var = function_with_multiple_parameters_and_return(123, "1234")
    tt = function_with_multiple_parameters_and_return2(500)
    _summ = pass_in_tuple(tt)

    function_with_parameter(1234)

    return tt


def test_mypy_() -> None:
    # should fail! in mypy
    # with pytest.raises()
    with pytest.raises(TypeError):
        pipe(1, "str", "fjdkslajfld", "hi")  # type: ignore[call-arg]

    # TODO: use assignements with typing for _var!
    _res = pipe(1)
    _res = pipe("1")  # type: ignore[arg-type]
    pipe.node_dict  # noqa: B018
    pipe.setup()

    exec = pipe.executor()

    exec.dag(1234)
