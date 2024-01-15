from functools import partial
from typing import Any, Tuple

import pytest
from tawazi import DAG, dag, xn
from tawazi._dag.digraph import DiGraphEx
from tawazi._dag.helpers import execute


def shortcut_execute(dag: DAG[Any, Any], graph: DiGraphEx) -> Any:
    return execute(
        node_dict=dag.exec_nodes,
        max_concurrency=dag.max_concurrency,
        behavior=dag.behavior,
        graph=graph,
    )


def func(a: str, b: str) -> str:
    return a + b


def test_same_constant_name_in_two_exec_nodes() -> None:
    @xn
    def a(cst: int) -> int:
        return cst

    @xn
    def b(a: int, cst: str) -> str:
        return str(a) + cst

    @dag
    def my_dag() -> None:
        var_a = a(1234)
        b(var_a, "poulpe")

    exec_nodes = shortcut_execute(my_dag, my_dag.graph_ids.make_subgraph())
    assert len(exec_nodes) == 4
    assert exec_nodes[a.id].result == 1234
    assert exec_nodes[b.id].result == "1234poulpe"


def test_dag_with_weird_nodes() -> None:
    @xn
    def toto(a: str, b: str) -> str:
        return a + "_wow_" + b

    @dag
    def my_dag() -> Tuple[Any, ...]:
        var_a = xn(partial(func, a="x"))(b="y")
        var_b = xn(partial(func, b="y"))(a="x")
        var_c = xn(lambda x: x)("e")
        var_d = toto(var_a, var_b)

        return var_a, var_b, var_c, var_d

    res_a, res_b, res_c, res_d = my_dag()

    assert res_a == "xy"
    assert res_b == "xy"
    assert res_c == "e"
    assert res_d == "xy_wow_xy"


def test_setup_debug_nodes() -> None:
    with pytest.raises(ValueError):

        @xn(debug=True, setup=True)
        def a() -> None:
            ...
