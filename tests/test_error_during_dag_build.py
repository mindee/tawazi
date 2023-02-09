# type: ignore
import pytest

from tawazi import dag, node, xn


def test_execnodes():
    @xn
    def a():
        pass

    with pytest.raises(NameError):

        @dag
        def pipe():
            a()
            b()  # an undefined ExecNode

    assert node.exec_nodes == []
