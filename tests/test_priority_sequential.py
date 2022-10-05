#  type: ignore
import logging
from time import sleep

import pytest

from tawazi import DAG, ErrorStrategy, ExecNode

T = 0.001
# global comp_str
pytest.comp_str = ""


# pass *kwargs because different the same function is used in multiple deps
def a(**results_dict):
    sleep(T)
    pytest.comp_str += "a"


def b(**results_dict):
    sleep(T)
    pytest.comp_str += "b"


def c(**results_dict):
    sleep(T)
    pytest.comp_str += "c"


def d(**results_dict):
    sleep(T)
    pytest.comp_str += "d"


def e(**results_dict):
    sleep(T)
    pytest.comp_str += "e"


def test_priority():
    # tests to run 1000s of time
    # Priority test
    for _i in range(100):
        pytest.comp_str = ""
        list_execnodes = [
            ExecNode(a, a, priority=1, is_sequential=False),
            ExecNode(b, b, [a], priority=2, is_sequential=False),
            ExecNode(c, c, [b], priority=2, is_sequential=False),
            ExecNode(d, d, [a], priority=1, is_sequential=False),
        ]

        g = DAG(list_execnodes, 1, behavior=ErrorStrategy.strict)
        g.execute()
        assert pytest.comp_str == "abcd", f"during {_i}th iteration"


def test_sequentiality():
    for _i in range(100):
        # Sequentiality test
        pytest.comp_str = ""
        list_execnodes = [
            ExecNode(a, a, is_sequential=False),
            ExecNode(b, b, [a], priority=2, is_sequential=False),
            ExecNode(c, c, [a], priority=2, is_sequential=False),
            ExecNode(d, d, [b], priority=2, is_sequential=False),
            ExecNode(e, e, [a], priority=1, is_sequential=True),
        ]

        g = DAG(list_execnodes, 2, behavior=ErrorStrategy.strict)
        g.execute()
        ind_a = pytest.comp_str.index("a")
        ind_b = pytest.comp_str.index("b")
        ind_c = pytest.comp_str.index("c")
        ind_d = pytest.comp_str.index("d")
        ind_e = pytest.comp_str.index("e")

        assert ind_e > ind_b, f"during {_i}th iteration"
        assert ind_d > ind_b, f"during {_i}th iteration"
        assert ind_b > ind_a, f"during {_i}th iteration"
        assert ind_c > ind_a, f"during {_i}th iteration"
