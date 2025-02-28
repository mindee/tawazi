from typing import Any

import pytest
from tawazi import DAG, dag, xn
from tawazi._dag.digraph import DiGraphEx
from tawazi._dag.helpers import sync_execute
from tawazi.errors import TawaziArgumentError

subgraph_comp_str = ""
T = 1e-3


def shortcut_execute(dag: DAG[Any, Any], graph: DiGraphEx) -> Any:
    return sync_execute(
        results=dag.results,
        exec_nodes=dag.exec_nodes,
        max_concurrency=dag.max_concurrency,
        graph=graph,
    )


@xn
def a() -> None:
    global subgraph_comp_str
    subgraph_comp_str += "a"


@xn
def b(a: Any) -> None:
    global subgraph_comp_str
    subgraph_comp_str += "b"


@xn
def c(a: Any) -> None:
    global subgraph_comp_str
    subgraph_comp_str += "c"


@xn
def d(c: Any) -> None:
    global subgraph_comp_str
    subgraph_comp_str += "d"


@xn
def e(c: Any) -> None:
    global subgraph_comp_str
    subgraph_comp_str += "e"


@xn
def f(e: Any) -> None:
    global subgraph_comp_str
    subgraph_comp_str += "f"


@xn
def g() -> None:
    global subgraph_comp_str
    subgraph_comp_str += "g"


@xn
def h() -> None:
    global subgraph_comp_str
    subgraph_comp_str += "h"


@xn
def i(h: Any) -> None:
    global subgraph_comp_str
    subgraph_comp_str += "i"


@dag
def dag_describer() -> None:
    var_a = a()
    b(var_a)
    var_c = c(var_a)
    d(var_c)
    var_e = e(var_c)
    f(var_e)

    g()

    var_h = h()
    i(var_h)


@pytest.mark.parametrize(
    "target_nodes, expected_nodes",
    [(["a"], {"a"}), (["b"], {"a", "b"}), (["c"], {"a", "c"}), (["d"], {"a", "c", "d"})],
)
def test_scheduled_nodes(target_nodes: list[str], expected_nodes: set[str]) -> None:
    executor = dag_describer.executor(target_nodes=target_nodes)
    assert expected_nodes == set(executor.graph.nodes)


@pytest.mark.parametrize(
    "nodes_ids, expected_nodes",
    [
        (["a", "b", "c", "d", "e", "f", "g", "h", "i"], "abcdefghi"),
        (["b", "d", "f", "g", "i"], "abcdefghi"),
        (["b", "c", "e", "h", "g"], "abcegh"),
        (["b", "c", "e", "h", "g"], "abcegh"),
    ],
)
def test_dag_subgraph(nodes_ids: list[str], expected_nodes: str) -> None:
    global subgraph_comp_str
    subgraph_comp_str = ""
    graph = dag_describer.graph_ids.make_subgraph(nodes_ids)
    shortcut_execute(dag_describer, graph)
    assert set(expected_nodes) == set(subgraph_comp_str)


def test_dag_subgraph_non_existing_nodes_ids() -> None:
    with pytest.raises(ValueError):
        graph = dag_describer.graph_ids.make_subgraph(["gibirish"])
        shortcut_execute(dag_describer, graph)


def test_no_nodes_running_in_subgraph() -> None:
    exec_ = dag_describer.executor(target_nodes=[])
    assert exec_() is None


def test_subgraph_return_constant() -> None:
    @dag
    def subgraph() -> int:
        return 1

    @dag
    def graph() -> int:
        return subgraph()

    assert graph() == 1


def sub_dag(v: Any) -> int:
    return 1


# declare dag separately from pipe function in order to use inspect correctly
err_dag = dag(sub_dag)


@dag
def super_dag() -> None:
    err_dag()  # type: ignore[call-arg]


def test_raise_error_location() -> None:
    with pytest.raises(TawaziArgumentError):
        super_dag()
