import pytest
from tawazi import _node, dag, xn


def test_execnodes() -> None:
    @xn
    def a() -> None:
        pass

    with pytest.raises(NameError):

        @dag
        def pipe() -> None:
            a()
            # purposefully an undefined ExecNode
            b()  # type: ignore[name-defined] # noqa: F821

    assert _node.node.exec_nodes == {}
