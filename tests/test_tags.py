# type: ignore
from tawazi import dag, xn


@xn(tag="b")
def a(b: int = 1234):
    return b + 1


@xn(tag=("op", "b", "takes argument a"))
def b(a: int = 4321):
    return a + 1


@dag
def pipe():
    return b(a())


def test_tag():
    assert pipe() == 1236
    assert pipe.get_nodes_by_tag("b") == [pipe.get_node_by_id("a")]
    assert pipe.get_nodes_by_tag(("op", "b", "takes argument a")) == [pipe.get_node_by_id("b")]


def test_call_tag():
    @dag
    def pipe():
        a_ = a(10, twz_tag="another_a_tag")
        b_ = b(a_, twz_tag="another_b_tag")
        return b_

    assert pipe() == 12
    assert pipe.get_nodes_by_tag("another_a_tag") == [pipe.get_node_by_id("a")]
    assert pipe.get_nodes_by_tag("another_b_tag") == [pipe.get_node_by_id("b")]
