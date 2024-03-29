from functools import wraps
from logging import Logger
from typing import Callable, TypeVar

from tawazi import dag, xn
from typing_extensions import ParamSpec

P = ParamSpec("P")
RV = TypeVar("RV")

logger = Logger(name="mylogger", level="ERROR")


def my_little_logger(func: Callable[P, RV]) -> Callable[P, RV]:
    @wraps(func)
    def log(*args: P.args, **kwargs: P.kwargs) -> RV:
        logger.debug("this should print before execution")
        res = func(*args, **kwargs)
        logger.debug("this should print after execution")
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
    b(a_)


def test_decorator() -> None:
    pipe()
