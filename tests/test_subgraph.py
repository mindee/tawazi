from typing import Any, List

import pytest
from tawazi import DAG, dag, xn
from tawazi._dag.digraph import DiGraphEx
from tawazi._dag.helpers import execute
from tawazi.node import ExecNode

subgraph_comp_str = ""
T = 1e-3


def shortcut_execute(dag: DAG[Any, Any], graph: DiGraphEx) -> Any:
    return execute(
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


def test_scheduled_nodes() -> None:
    executor = dag_describer.executor(target_nodes=["a"])
    assert {"a"} == set(executor.graph.nodes)

    executor = dag_describer.executor(target_nodes=["b"])
    assert {"a", "b"} == set(executor.graph.nodes)

    executor = dag_describer.executor(target_nodes=["c"])
    assert {"a", "c"} == set(executor.graph.nodes)

    executor = dag_describer.executor(target_nodes=["d"])
    assert {"a", "c", "d"} == set(executor.graph.nodes)


def test_dag_subgraph_all_nodes() -> None:
    global subgraph_comp_str
    subgraph_comp_str = ""
    dag = dag_describer
    nodes: List[ExecNode] = [a, b, c, d, e, f, g, h, i]
    nodes_ids = [n.id for n in nodes]

    graph = dag.graph_ids.make_subgraph(nodes_ids)
    shortcut_execute(dag, graph)
    assert set("abcdefghi") == set(subgraph_comp_str)


def test_dag_subgraph_leaf_nodes() -> None:
    global subgraph_comp_str
    subgraph_comp_str = ""
    dag = dag_describer
    nodes: List[ExecNode] = [b, d, f, g, i]
    nodes_ids: List[str] = [n.id for n in nodes]

    graph = dag.graph_ids.make_subgraph(nodes_ids)
    shortcut_execute(dag, graph)
    assert set("abcdefghi") == set(subgraph_comp_str)


def test_dag_subgraph_leaf_nodes_with_extra_nodes() -> None:
    global subgraph_comp_str
    subgraph_comp_str = ""
    dag = dag_describer
    nodes: List[ExecNode] = [b, c, e, h, g]
    nodes_ids = [n.id for n in nodes]

    graph = dag.graph_ids.make_subgraph(nodes_ids)
    shortcut_execute(dag, graph)
    assert set("abcegh") == set(subgraph_comp_str)


def test_dag_subgraph_nodes_ids() -> None:
    global subgraph_comp_str
    subgraph_comp_str = ""
    dag = dag_describer
    graph = dag.graph_ids.make_subgraph([b.id, c.id, e.id, h.id, g.id])
    shortcut_execute(dag, graph)
    assert set("abcegh") == set(subgraph_comp_str)


def test_dag_subgraph_non_existing_nodes_ids() -> None:
    with pytest.raises(ValueError):
        dag = dag_describer
        graph = dag.graph_ids.make_subgraph(["gibirish"])
        shortcut_execute(dag, graph)


@xn
def a1(in1: int) -> int:
    return in1 + 1


@dag
def pipe(in1: int) -> int:
    return a1(in1)


def test_no_nodes_running_in_subgraph() -> None:
    exec_ = pipe.executor(target_nodes=[])
    assert exec_() is None  # type: ignore[call-arg]
