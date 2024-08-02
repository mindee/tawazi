import pytest
from tawazi import dag, xn


@xn(tag="b")
def a(b: int = 1234) -> int:
    return b + 1


@xn(tag=("op", "b", "takes argument a"))
def b(a: int = 4321) -> int:
    return a + 1


@dag
def pipe() -> int:
    return b(a())


def test_tag() -> None:
    assert pipe() == 1236
    assert pipe.graph_ids.tags == {"takes argument a", "op", "b"}
    assert pipe.get_nodes_by_tag("b") == [pipe.get_node_by_id("a"), pipe.get_node_by_id("b")]
    assert pipe.get_nodes_by_tag("op") == [pipe.get_node_by_id("b")]
    assert pipe.get_nodes_by_tag("takes argument a") == [pipe.get_node_by_id("b")]


def test_call_tag() -> None:
    @dag
    def pipe() -> int:
        a_ = a(10, twz_tag="another_a_tag")  # type: ignore[call-arg]
        return b(a_, twz_tag="another_b_tag")  # type: ignore[call-arg]

    assert pipe() == 12
    assert pipe.get_nodes_by_tag("another_a_tag") == [pipe.get_node_by_id("a")]
    assert pipe.get_nodes_by_tag("another_b_tag") == [pipe.get_node_by_id("b")]


def test_tag_is_not_registered() -> None:
    @xn(tag="c")
    def c() -> int:
        return 1

    @dag
    def pipe() -> None:
        c()
        c(twz_tag="twinkle")  # type: ignore[call-arg]

    assert pipe.get_nodes_by_tag("c")[0].kwargs == {}
    assert pipe.get_nodes_by_tag("twinkle")[0].kwargs == {}


def test_get_by_id() -> None:
    assert pipe.get_node_by_id("a").tag == "b"
    assert pipe.get_node_by_id("b").tag == ("op", "b", "takes argument a")


def test_failing_get_by_id() -> None:
    with pytest.raises(ValueError, match="node c doesn't exist in the DAG"):
        pipe.get_node_by_id("c")
