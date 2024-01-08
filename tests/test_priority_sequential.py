from time import sleep
from typing import Any

from tawazi import DAG, ErrorStrategy
from tawazi.node import ExecNode, UsageExecNode

T = 0.01
# global priority_sequential_comp_str
priority_sequential_comp_str = ""


# pass *args because different the same function is used in multiple deps
def a(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "a"


def b(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "b"


def c(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "c"


def d(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "d"


def e(*__args: Any) -> None:
    sleep(T)
    global priority_sequential_comp_str
    priority_sequential_comp_str += "e"


def test_priority() -> None:
    # tests to run 1000s of time
    # Priority test
    global priority_sequential_comp_str
    for _i in range(100):
        priority_sequential_comp_str = ""
        en_a = ExecNode(id_=a.__name__, exec_function=a, priority=1, is_sequential=False)
        en_b = ExecNode(
            id_=b.__name__,
            exec_function=b,
            args=[UsageExecNode(en_a.id)],
            priority=2,
            is_sequential=False,
        )
        en_c = ExecNode(
            id_=c.__name__,
            exec_function=c,
            args=[UsageExecNode(en_b.id)],
            priority=2,
            is_sequential=False,
        )
        en_d = ExecNode(
            id_=d.__name__,
            exec_function=d,
            args=[UsageExecNode(en_a.id)],
            priority=1,
            is_sequential=False,
        )
        list_execnodes = [en_a, en_b, en_c, en_d]
        node_dict = {xn.id: xn for xn in list_execnodes}

        g: DAG[Any, Any] = DAG(node_dict, [], [], 1, behavior=ErrorStrategy.strict)
        g()
        assert priority_sequential_comp_str == "abcd", f"during {_i}th iteration"


def test_sequentiality() -> None:
    global priority_sequential_comp_str

    for _i in range(100):
        # Sequentiality test
        priority_sequential_comp_str = ""
        en_a = ExecNode(id_=a.__name__, exec_function=a, is_sequential=False)
        en_b = ExecNode(
            id_=b.__name__,
            exec_function=b,
            args=[UsageExecNode(en_a.id)],
            priority=2,
            is_sequential=False,
        )
        en_c = ExecNode(
            id_=c.__name__,
            exec_function=c,
            args=[UsageExecNode(en_a.id)],
            priority=2,
            is_sequential=False,
        )
        en_d = ExecNode(
            id_=d.__name__,
            exec_function=d,
            args=[UsageExecNode(en_b.id)],
            priority=2,
            is_sequential=False,
        )
        en_e = ExecNode(
            id_=e.__name__,
            exec_function=e,
            args=[UsageExecNode(en_a.id)],
            priority=1,
            is_sequential=True,
        )
        list_execnodes = [en_a, en_b, en_c, en_d, en_e]
        node_dict = {xn.id: xn for xn in list_execnodes}

        g: DAG[Any, Any] = DAG(node_dict, [], [], 2, behavior=ErrorStrategy.strict)
        g()
        sequential_comp_str = priority_sequential_comp_str
        ind_a = sequential_comp_str.index("a")
        ind_b = sequential_comp_str.index("b")
        ind_c = sequential_comp_str.index("c")
        ind_d = sequential_comp_str.index("d")
        ind_e = sequential_comp_str.index("e")

        assert ind_e > ind_b, f"during {_i}th iteration"
        assert ind_d > ind_b, f"during {_i}th iteration"
        assert ind_b > ind_a, f"during {_i}th iteration"
        assert ind_c > ind_a, f"during {_i}th iteration"
