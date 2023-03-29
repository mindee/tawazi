from copy import deepcopy
from typing import Tuple

import pytest
from tawazi import dag, xn
from tawazi.config import cfg
from tawazi.errors import TawaziUsageError

test_exclude_nodes = ""


@xn
def a(s: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "a"
    return s + "a"


@xn
def b(s: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "b"
    return s + "b"


@xn
def c(s: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "c"
    return s + "c"


@xn
def d(s: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "d"
    return s + "d"


@xn
def e(s: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "e"
    return s + "e"


@xn
def f(s: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "f"
    return s + "f"


@xn
def g(s1: str, s2: str) -> str:
    global test_exclude_nodes
    test_exclude_nodes += "g"
    return s1 + s2 + "g"


@dag
def pipe() -> Tuple[str, str, str, str]:
    f_ = f(e(a("")))
    b_ = b("")
    c_ = c(b_)
    d_ = d(b_)
    g_ = g(c_, d_)
    return f_, c_, d_, g_


def test_excludenodes_basic() -> None:
    global test_exclude_nodes
    test_exclude_nodes = ""
    pipe_ = pipe.executor(exclude_nodes=[g])
    assert ("aef", "bc", "bd", None) == pipe_()  # type: ignore[comparison-overlap]
    assert set("abcdef") == set(test_exclude_nodes)


def test_exclude_main_node() -> None:
    global test_exclude_nodes
    test_exclude_nodes = ""
    pipe_ = pipe.executor(exclude_nodes=[b])
    assert ("aef", None, None, None) == pipe_()  # type: ignore[comparison-overlap]
    assert set("aef") == set(test_exclude_nodes)


def test_excludenodes_execute_all_nodes_without_return() -> None:
    global test_exclude_nodes
    test_exclude_nodes = ""
    pipe_ = pipe.executor(exclude_nodes=[f, g, c, d])
    assert (None, None, None, None) == pipe_()  # type: ignore[comparison-overlap]
    assert set("abe") == set(test_exclude_nodes)


def test_with_setup_nodes() -> None:
    @xn(setup=True)
    def z_setup(s: str) -> str:
        global test_exclude_nodes
        test_exclude_nodes += "z"
        return s + "z"

    @dag
    def pipe() -> Tuple[str, str, str, str]:
        f_ = f(e(z_setup("")))
        b_ = b("")
        c_ = c(b_)
        d_ = d(b_)
        g_ = g(c_, d_)
        return f_, c_, d_, g_

    global test_exclude_nodes
    test_exclude_nodes = ""
    pipe_ = deepcopy(pipe)
    pipe_exec = pipe_.executor(exclude_nodes=[e])
    assert (None, "bc", "bd", "bcbdg") == pipe_exec()  # type: ignore[comparison-overlap]
    assert set("zbcdg") == set(test_exclude_nodes)

    pipe_.setup(exclude_nodes=[e])
    assert test_exclude_nodes.count("z") == 1

    pipe_exec = pipe_.executor(exclude_nodes=[e])
    assert (None, "bc", "bd", "bcbdg") == pipe_exec()  # type: ignore[comparison-overlap]
    assert test_exclude_nodes.count("z") == 1

    pipe_exec = pipe_.executor(exclude_nodes=[e])
    assert ("zef", "bc", "bd", "bcbdg") == pipe_()
    assert test_exclude_nodes.count("z") == 1


def test_with_debug_nodes() -> None:
    c_debug = xn(debug=True)(c.exec_function)
    d_debug = xn(debug=True)(d.exec_function)
    g_debug = xn(debug=True)(g.exec_function)

    @dag
    def pipe() -> Tuple[str, str, str, str]:
        f_ = f(e(a("")))
        b_ = b("")
        c_ = c_debug(b_)
        d_ = d_debug(b_)
        g_ = g_debug(c_, d_)
        return f_, c_, d_, g_

    cfg.RUN_DEBUG_NODES = True
    assert ("aef", "bc", "bd", "bcbdg") == pipe()
    # TODO: write clear documentation about priority of choosing exclude_nodes vs debug_nodes
    #  (debug_nodes are more prioritized!)
    exec_ = pipe.executor(exclude_nodes=[g])
    assert ("aef", "bc", "bd", "bcbdg") == exec_()
    exec_ = pipe.executor(exclude_nodes=[b])
    assert ("aef", None, None, None) == exec_()  # type: ignore[comparison-overlap]


# TODO: maybe write a test case with cache ?


def test_impossible_situation() -> None:
    with pytest.raises(TawaziUsageError):
        _ = pipe.executor(target_nodes=[g], exclude_nodes=[d])
