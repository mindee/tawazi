# type: ignore
from tawazi import op, to_dag


@op(tag="b")
def a(b: int = 1234):
    return b + 1


@op(tag=("op", "b", "takes argument a"))
def b(a: int = 4321):
    return a + 1


@to_dag
def pipe():
    return b(a())


def test_tag():
    assert pipe() == 1236
    assert pipe.get_nodes_by_tag("b") == [pipe.get_node_by_id("a")]
    assert pipe.get_nodes_by_tag(("op", "b", "takes argument a")) == [pipe.get_node_by_id("b")]
