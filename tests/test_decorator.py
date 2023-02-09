# type: ignore
from functools import wraps

from tawazi import to_dag, xn

"""Integration test"""


def my_little_logger(func):
    @wraps(func)
    def log(*args, **kwargs):
        print("this should print before execution")
        res = func(*args, **kwargs)
        print("this should print after execution")
        return res

    return log


@xn
@my_little_logger
def a():
    return "titi"


@xn
@my_little_logger
def b(a):
    return "tata" + a


@to_dag
def pipe():
    a_ = a()
    t = b(a_)


def test_decorator():
    pipe()
