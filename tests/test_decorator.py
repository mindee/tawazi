# type: ignore
from functools import wraps

from tawazi import op, to_dag

"""Integration test"""


def my_little_logger(func):
    @wraps(func)
    def log(*args, **kwargs):
        print("this should print before execution")
        res = func(*args, **kwargs)
        print("this should print after execution")
        return res

    return log


@op
@my_little_logger
def a():
    return "titi"


@op
@my_little_logger
def b(a):
    return "tata" + a


@to_dag
def pipe():
    a_ = a()
    t = b(a_)


def test_decorator():
    pipe()
