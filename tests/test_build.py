from time import sleep
from typing import Any, cast

import pytest
from tawazi import DAG, dag
from tawazi._helpers import StrictDict
from tawazi.node import ExecNode, UsageExecNode

T = 0.1


def a() -> str:
    sleep(T)
    return "a"


def b(a: str) -> str:
    sleep(T)
    return a + "b"


def c(a: str) -> str:
    sleep(T)
    return a + "c"


def d(b: str, c: str) -> str:
    sleep(T)
    return b + c + "d"


def e(b: str) -> str:
    sleep(T)
    return b + "e"


def f(e: str) -> str:
    sleep(T)
    return e + "f"


def g(e: str) -> str:
    sleep(T)
    return e + "g"


def fail(g: Any) -> int:
    return cast(int, 10 / 0)


# ExecNodes can be identified using the actual function or an identification string
en_a = ExecNode(id_=a.__name__, exec_function=a, is_sequential=True)
en_b = ExecNode(
    id_=b.__name__, exec_function=b, args=[UsageExecNode(en_a.id)], priority=2, is_sequential=False
)
en_c = ExecNode(
    id_=c.__name__, exec_function=c, args=[UsageExecNode(en_a.id)], priority=1, is_sequential=False
)
en_d = ExecNode(
    id_=d.__name__,
    exec_function=d,
    args=[UsageExecNode(en_b.id), UsageExecNode(en_c.id)],
    priority=1,
    is_sequential=False,
)
en_e = ExecNode(id_=e.__name__, exec_function=e, args=[UsageExecNode(en_b.id)], is_sequential=False)
en_f = ExecNode(id_=f.__name__, exec_function=f, args=[UsageExecNode(en_e.id)], is_sequential=False)
en_g = ExecNode(id_=g.__name__, exec_function=g, args=[UsageExecNode(en_e.id)], is_sequential=False)

list_execnodes = [en_a, en_b, en_c, en_d, en_e, en_f, en_g]
node_dict = StrictDict((xn.id, xn) for xn in list_execnodes)

failing_execnodes = list_execnodes + [
    ExecNode(
        id_=fail.__name__, exec_function=fail, args=[UsageExecNode(en_g.id)], is_sequential=False
    )
]
failing_node_dict = {xn.id: xn for xn in failing_execnodes}


@pytest.fixture
def strict_dag() -> DAG[Any, Any]:
    return DAG("mydag", StrictDict(), node_dict, [], [], 2)


def test_dag_build(strict_dag: DAG[Any, Any]) -> None:
    strict_dag()  # must never fail!


def test_setting_execnode_id_should_fail() -> None:
    with pytest.raises(AttributeError):
        en_a.id = "fdsakfjs"  # type: ignore[misc]


def test_execnodes() -> None:
    with pytest.raises(NameError):

        @dag
        def pipe() -> None:
            a()
            # purposefully an undefined ExecNode
            b_()  # type: ignore[name-defined] # noqa: F821


def test_wrong_type_max_concurrency() -> None:
    with pytest.raises(ValueError, match="max_concurrency must be an int"):
        DAG("mydag", StrictDict(), node_dict, [], [], "2")  # type: ignore[arg-type]


def test_negative_max_concurrency() -> None:
    with pytest.raises(
        ValueError, match="Invalid maximum number of threads! Must be a positive integer"
    ):
        DAG("mydag", StrictDict(), node_dict, [], [], -2)


def test_wrong_type_results() -> None:
    with pytest.raises(ValueError, match="results must be a StrictDict"):
        DAG("mydag", {}, node_dict, [], [], 2)  # type: ignore[arg-type]


def test_wrong_type_exec_nodes() -> None:
    with pytest.raises(ValueError, match="exec_nodes must be a StrictDict"):
        DAG("mydag", StrictDict(), {}, [], [], 2)  # type: ignore[arg-type]
