import inspect
from typing import Any

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziError


@xn
def faulty_function() -> int:
    raise ValueError("This is a ValueError")


def pipe() -> int:
    return faulty_function()


# declare dag separatly from pipe function in order to use inspect correctly
dag_pipe = dag(pipe)


def test_raise_error_location() -> None:
    # path and line no of function pipe
    pipe_path = inspect.getsourcefile(pipe)
    pipe_lineno = inspect.getsourcelines(pipe)[1]

    with pytest.raises(
        TawaziError,
        match=f"Error occurred while executing ExecNode faulty_function at {pipe_path}:{pipe_lineno + 1}",
    ):
        dag_pipe()


class NoneReturner:
    def currentframe(self) -> None:
        return None

    @property
    def f_back(self) -> None:
        return None


# if python's interpreter doesn't provide stack frame support, call_location won't work
def test_raise_error_location_no_stack_frame_support(mocker: Any) -> None:
    # use mock to deactivate stack frame support

    obj = NoneReturner()
    mocker.patch("tawazi.node.node.inspect", obj)

    @xn
    def faulty_function() -> int:
        raise ValueError("This is a ValueError")

    @dag
    def pipe() -> int:
        return faulty_function()

    with pytest.raises(ValueError, match="This is a ValueError"):
        pipe()


def test_raise_error_location_no_stack_frame_support_shallow_call(mocker: Any) -> None:
    class NoneReturnerShallow:
        def currentframe(self) -> Any:
            return NoneReturner()

    mocker.patch("tawazi.node.node.inspect", NoneReturnerShallow())

    @xn
    def faulty_function() -> int:
        raise ValueError("This is a ValueError")

    @dag
    def pipe() -> int:
        return faulty_function()

    with pytest.raises(ValueError, match="This is a ValueError"):
        pipe()
