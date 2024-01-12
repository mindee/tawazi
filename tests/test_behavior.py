from copy import deepcopy
from time import sleep
from typing import Any

import pytest
from tawazi import DAG, ErrorStrategy, Resource, dag, xn

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


@xn(resource=Resource.main_thread)
def a_main() -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "a"


@xn(priority=2, resource=Resource.main_thread)
def b_main(a: Any) -> None:
    raise NotImplementedError


@xn(priority=2, resource=Resource.main_thread)
def c_main(b: Any) -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "c"


@xn(resource=Resource.main_thread)
def d_main(a: Any) -> None:
    sleep(T)
    global behavior_comp_str
    behavior_comp_str += "d"


@dag
def g_main() -> None:
    a_ = a_main()
    b_ = b_main(a_)
    c_main(b_)
    d_main(a_)


@pytest.mark.parametrize(
    "graph, behavior, expected_behavior_comp_str",
    [
        (deepcopy(g), ErrorStrategy.strict, ""),  # test_strict_error_behavior
        (deepcopy(g), ErrorStrategy.all_children, "ad"),  # test_all_children_behavior
        (deepcopy(g), ErrorStrategy.permissive, "acd"),  # test_permissive_behavior
        (deepcopy(g_main), ErrorStrategy.strict, ""),  # test_strict_error_behavior_main_thread
        (
            deepcopy(g_main),
            ErrorStrategy.all_children,
            "ad",
        ),  # test_all_children_behavior_main_thread
        (deepcopy(g_main), ErrorStrategy.permissive, "acd"),  # test_permissive_behavior_main_thread
    ],
)
def test_behavior(
    graph: DAG[Any, Any], behavior: ErrorStrategy, expected_behavior_comp_str: str
) -> None:
    global behavior_comp_str
    behavior_comp_str = ""
    graph.behavior = behavior
    try:
        graph()
        assert behavior_comp_str == expected_behavior_comp_str
    except NotImplementedError:
        pass
