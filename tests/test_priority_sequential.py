#  type: ignore
from time import sleep

import pytest

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode

"""internal Unit test"""

T = 0.01
# global priority_sequential_comp_str
pytest.priority_sequential_comp_str = ""


# pass *kwargs because different the same function is used in multiple deps
def a(*args, **kwargs):
    sleep(T)
    pytest.priority_sequential_comp_str += "a"


def b(*args, **kwargs):
    sleep(T)
    pytest.priority_sequential_comp_str += "b"


def c(*args, **kwargs):
    sleep(T)
    pytest.priority_sequential_comp_str += "c"


def d(*args, **kwargs):
    sleep(T)
    pytest.priority_sequential_comp_str += "d"


def e(*args, **kwargs):
    sleep(T)
    pytest.priority_sequential_comp_str += "e"


def test_priority():
    # tests to run 1000s of time
    # Priority test
    for _i in range(100):
        pytest.priority_sequential_comp_str = ""
        en_a = ExecNode(id=a.__name__, exec_function=a, priority=1, is_sequential=False)
        en_b = ExecNode(
            id=b.__name__, exec_function=b, args=[en_a], priority=2, is_sequential=False
        )
        en_c = ExecNode(
            id=c.__name__, exec_function=c, args=[en_b], priority=2, is_sequential=False
        )
        en_d = ExecNode(
            id=d.__name__, exec_function=d, args=[en_a], priority=1, is_sequential=False
        )
        list_execnodes = [en_a, en_b, en_c, en_d]

        g = DAG(exec_nodes=list_execnodes, max_concurrency=1, behavior=ErrorStrategy.strict)
        g._execute(g._make_subgraph())
        assert pytest.priority_sequential_comp_str == "abcd", f"during {_i}th iteration"


def test_sequentiality():
    for _i in range(100):
        # Sequentiality test
        pytest.priority_sequential_comp_str = ""
        en_a = ExecNode(id=a.__name__, exec_function=a, is_sequential=False)
        en_b = ExecNode(
            id=b.__name__, exec_function=b, args=[en_a], priority=2, is_sequential=False
        )
        en_c = ExecNode(
            id=c.__name__, exec_function=c, args=[en_a], priority=2, is_sequential=False
        )
        en_d = ExecNode(
            id=d.__name__, exec_function=d, args=[en_b], priority=2, is_sequential=False
        )
        en_e = ExecNode(id=e.__name__, exec_function=e, args=[en_a], priority=1, is_sequential=True)
        list_execnodes = [en_a, en_b, en_c, en_d, en_e]

        g = DAG(exec_nodes=list_execnodes, max_concurrency=2, behavior=ErrorStrategy.strict)
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
