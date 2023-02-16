#  type: ignore
import pytest

from tawazi import dag, xn
from tawazi.errors import TawaziBaseException

"""integration test"""

pytest.subgraph_comp_str = ""
T = 1e-3


@xn
def a():
    pytest.subgraph_comp_str += "a"


@xn
def b(a):
    pytest.subgraph_comp_str += "b"


@xn
def c(a):
    pytest.subgraph_comp_str += "c"


@xn
def d(c):
    pytest.subgraph_comp_str += "d"


@xn
def e(c):
    pytest.subgraph_comp_str += "e"


@xn
def f(e):
    pytest.subgraph_comp_str += "f"


@xn
def g():
    pytest.subgraph_comp_str += "g"


@xn
def h():
    pytest.subgraph_comp_str += "h"


@xn
def i(h):
    pytest.subgraph_comp_str += "i"


@dag
def dag_describer():
    var_a = a()
    var_b = b(var_a)
    var_c = c(var_a)
    var_d = d(var_c)
    var_e = e(var_c)
    var_f = f(var_e)

    var_g = g()

    var_h = h()
    var_i = i(var_h)


def test_dag_subgraph_all_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    nodes = [a, b, c, d, e, f, g, h, i]
    nodes_ids = [n.id for n in nodes]

    results = dag._execute(target_ids=nodes_ids)
    assert set("abcdefghi") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_leaf_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    nodes = [b, d, f, g, i]
    nodes_ids = [n.id for n in nodes]

    results = dag._execute(target_ids=nodes_ids)
    assert set("abcdefghi") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_leaf_nodes_with_extra_nodes():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    nodes = [b, c, e, h, g]
    nodes_ids = [n.id for n in nodes]

    results = dag._execute(target_ids=nodes_ids)
    assert set("abcegh") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_nodes_ids():
    pytest.subgraph_comp_str = ""
    dag = dag_describer
    results = dag._execute(target_ids=[b.id, c.id, e.id, h.id, g.id])
    assert set("abcegh") == set(pytest.subgraph_comp_str)


def test_dag_subgraph_non_existing_nodes_ids():
    with pytest.raises(ValueError, match="nodes are not in the graph"):
        dag = dag_describer
        results = dag._execute(target_ids=["gibirish"])


@xn
def a1(in1):
    return in1 + 1


@dag
def pipe(in1):
    return a1(in1)


def test_no_nodes_running_in_subgraph():
    assert pipe(target_nodes=[]) is None


# TODO: fix this problem!!!
# def test_dag_subgraph_nodes_with_usage():
#     @to_dag
#     def pipe_duplication():
#         a()
#         a()
#     with pytest.raises(TawaziBaseException):
#         pipe_duplication.execute([a])
