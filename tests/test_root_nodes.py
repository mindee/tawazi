import pytest
from tawazi import dag, xn


@xn
def add(ar: str, br: str) -> str:
    return ar + br


@xn
def v() -> int:
    return 1


@xn
def v1(a: int) -> int:
    return 1


@xn
def v2(e: int) -> int:
    return 1


@xn
def v3(f: int) -> int:
    return 1


@xn
def a() -> str:
    return "q"


@xn
def c() -> int:
    return 3


@dag
def d() -> int:
    titi = c()
    _a = a()
    add(_a, "b")
    va = v()
    vb = v1(va)
    vc = v2(vb)
    v3(vc)
    return titi


def test_with_root_nodes() -> None:
    dx = d.executor(root_nodes=["v"])
    assert set(dx.graph.nodes) == {"v", "v1", "v2", "v3"}


def test_failing_roots() -> None:
    with pytest.raises(ValueError):
        d.executor(root_nodes=["v2"])
