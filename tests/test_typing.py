import pytest
from tawazi import AsyncDAG, dag, xn


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
def function_with_multiple_parameters_and_return2(x: int) -> tuple[int, str]:
    return x, str(x)


@xn
def pass_in_tuple(x: tuple[int, str]) -> int:
    return x[0] + int(x[1])


@dag
def pipe(input: int) -> tuple[int, str]:
    _none = function_no_parameter()
    _none = function_with_parameter(1234)
    _var = function_with_multiple_parameters_and_return(123, "1234")
    tt = function_with_multiple_parameters_and_return2(500)
    _summ = pass_in_tuple(tt)

    function_with_parameter(1234)

    return tt


@dag
def pipe_configured(input: int) -> tuple[int, str]:
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

    _res = pipe(1)
    _res = pipe("1")  # type: ignore[arg-type]
    _ = pipe.exec_nodes  # noqa: B018
    pipe.setup()

    _exec = pipe.executor()
    _exec.dag(1234)


def test_typing_async() -> None:
    @dag(is_async=True)
    def my_async_dag() -> None:
        pass

    # assert that mypy passes this test
    var: AsyncDAG[[], None] = my_async_dag
    assert isinstance(var, AsyncDAG)
