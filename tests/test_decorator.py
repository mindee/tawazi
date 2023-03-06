from functools import wraps
from typing import Callable, TypeVar

from typing_extensions import ParamSpec

from tawazi import dag, xn

"""Integration test"""
P = ParamSpec("P")
RV = TypeVar("RV")


def my_little_logger(func: Callable[P, RV]) -> Callable[P, RV]:
    @wraps(func)
    def log(*args: P.args, **kwargs: P.kwargs) -> RV:
        print("this should print before execution")
        res = func(*args, **kwargs)
        print("this should print after execution")
        return res

    return log


@xn
@my_little_logger
def a() -> str:
    return "titi"


@xn
@my_little_logger
def b(a: str) -> str:
    return "tata" + a


@dag
def pipe() -> None:
    a_ = a()
    t = b(a_)


def test_decorator() -> None:
    pipe()
