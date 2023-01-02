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
        list_execnodes = [
            ExecNode(a.__name__, a, priority=1, is_sequential=False),
            ExecNode(b.__name__, b, [(None, a.__name__)], priority=2, is_sequential=False),
            ExecNode(c.__name__, c, [(None, b.__name__)], priority=2, is_sequential=False),
            ExecNode(d.__name__, d, [(None, a.__name__)], priority=1, is_sequential=False),
        ]

        g = DAG(list_execnodes, 1, behavior=ErrorStrategy.strict)
        g.execute()
        assert pytest.priority_sequential_comp_str == "abcd", f"during {_i}th iteration"


def test_sequentiality():
    for _i in range(100):
        # Sequentiality test
        pytest.priority_sequential_comp_str = ""
        list_execnodes = [
            ExecNode(a.__name__, a, is_sequential=False),
            ExecNode(b.__name__, b, [(None, a.__name__)], priority=2, is_sequential=False),
            ExecNode(c.__name__, c, [(None, a.__name__)], priority=2, is_sequential=False),
            ExecNode(d.__name__, d, [(None, b.__name__)], priority=2, is_sequential=False),
            ExecNode(e.__name__, e, [(None, a.__name__)], priority=1, is_sequential=True),
        ]

        g = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
        g.execute()
        ind_a = pytest.priority_sequential_comp_str.index("a")
        ind_b = pytest.priority_sequential_comp_str.index("b")
        ind_c = pytest.priority_sequential_comp_str.index("c")
        ind_d = pytest.priority_sequential_comp_str.index("d")
        ind_e = pytest.priority_sequential_comp_str.index("e")

        assert ind_e > ind_b, f"during {_i}th iteration"
        assert ind_d > ind_b, f"during {_i}th iteration"
        assert ind_b > ind_a, f"during {_i}th iteration"
        assert ind_c > ind_a, f"during {_i}th iteration"
