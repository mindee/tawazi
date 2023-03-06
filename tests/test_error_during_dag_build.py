import pytest

from tawazi import dag, node, xn


def test_execnodes() -> None:
    @xn
    def a() -> None:
        pass

    with pytest.raises(NameError):

        @dag
        def pipe() -> None:
            a()
            # an undefined ExecNode
            b()  # type: ignore[name-defined]

    assert node.exec_nodes == []
