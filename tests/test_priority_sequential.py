# type: ignore # noqa: PGH003
from time import sleep
from typing import Any

import pytest
from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode

T = 0.01
# global priority_sequential_comp_str
pytest.priority_sequential_comp_str = ""


# pass *args because different the same function is used in multiple deps
def a(*__args: Any) -> None:
    sleep(T)
    pytest.priority_sequential_comp_str += "a"


def b(*__args: Any) -> None:
    sleep(T)
    pytest.priority_sequential_comp_str += "b"


def c(*__args: Any) -> None:
    sleep(T)
    pytest.priority_sequential_comp_str += "c"


def d(*__args: Any) -> None:
    sleep(T)
    pytest.priority_sequential_comp_str += "d"


def e(*__args: Any) -> None:
    sleep(T)
    pytest.priority_sequential_comp_str += "e"


def test_priority() -> None:
    # tests to run 1000s of time
    # Priority test
    for _i in range(100):
        pytest.priority_sequential_comp_str = ""
        en_a = ExecNode(a.__name__, a, priority=1, is_sequential=False)
        en_b = ExecNode(b.__name__, b, [en_a], priority=2, is_sequential=False)
        en_c = ExecNode(c.__name__, c, [en_b], priority=2, is_sequential=False)
        en_d = ExecNode(d.__name__, d, [en_a], priority=1, is_sequential=False)
        list_execnodes = [en_a, en_b, en_c, en_d]

        g: DAG[Any, Any] = DAG(list_execnodes, 1, behavior=ErrorStrategy.strict)
        g._execute(g._make_subgraph())
        assert pytest.priority_sequential_comp_str == "abcd", f"during {_i}th iteration"


def test_sequentiality() -> None:
    for _i in range(100):
        # Sequentiality test
        pytest.priority_sequential_comp_str = ""
        en_a = ExecNode(a.__name__, a, is_sequential=False)
        en_b = ExecNode(b.__name__, b, [en_a], priority=2, is_sequential=False)
        en_c = ExecNode(c.__name__, c, [en_a], priority=2, is_sequential=False)
        en_d = ExecNode(d.__name__, d, [en_b], priority=2, is_sequential=False)
        en_e = ExecNode(e.__name__, e, [en_a], priority=1, is_sequential=True)
        list_execnodes = [en_a, en_b, en_c, en_d, en_e]

        g: DAG[Any, Any] = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
        g._execute(g._make_subgraph())
        ind_a = pytest.priority_sequential_comp_str.index("a")
        ind_b = pytest.priority_sequential_comp_str.index("b")
        ind_c = pytest.priority_sequential_comp_str.index("c")
        ind_d = pytest.priority_sequential_comp_str.index("d")
        ind_e = pytest.priority_sequential_comp_str.index("e")

        assert ind_e > ind_b, f"during {_i}th iteration"
        assert ind_d > ind_b, f"during {_i}th iteration"
        assert ind_b > ind_a, f"during {_i}th iteration"
        assert ind_c > ind_a, f"during {_i}th iteration"
