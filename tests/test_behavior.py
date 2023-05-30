from copy import deepcopy
from time import sleep
from typing import Any

from tawazi import ErrorStrategy, dag, xn

T = 0.001
# global behavior_comp_str
behavior_comp_str = ""


@xn
def a() -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "a"


@xn(priority=2)
def b(a: Any) -> None:
    raise NotImplementedError


@xn(priority=2)
def c(b: Any) -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "c"


@xn
def d(a: Any) -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "d"


@dag
def g() -> None:
    a_ = a()
    b_ = b(a_)
    c(b_)
    d(a_)


def test_strict_error_behavior() -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    g_ = deepcopy(g)
    g_.behavior = ErrorStrategy.strict
    try:
        g_()
    except NotImplementedError:
        pass


def test_all_children_behavior() -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    g_ = deepcopy(g)
    g_.behavior = ErrorStrategy.all_children
    g_()
    assert behavior_comp_str == "ad"


def test_permissive_behavior() -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    g_ = deepcopy(g)
    g_.behavior = ErrorStrategy.permissive
    g_()
    assert behavior_comp_str == "acd"


# todo test using argname for ExecNode
