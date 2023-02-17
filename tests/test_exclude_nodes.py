from copy import deepcopy

import pytest

from tawazi import dag, xn
from tawazi.config import Cfg
from tawazi.errors import TawaziUsageError


@xn
def a(s):
    pytest.test_exclude_nodes += "a"
    return s + "a"


@xn
def b(s):
    pytest.test_exclude_nodes += "b"
    return s + "b"


@xn
def c(s):
    pytest.test_exclude_nodes += "c"
    return s + "c"


@xn
def d(s):
    pytest.test_exclude_nodes += "d"
    return s + "d"


@xn
def e(s):
    pytest.test_exclude_nodes += "e"
    return s + "e"


@xn
def f(s):
    pytest.test_exclude_nodes += "f"
    return s + "f"


@xn
def g(s1, s2):
    pytest.test_exclude_nodes += "g"
    return s1 + s2 + "g"


@dag
def pipe():
    f_ = f(e(a("")))
    b_ = b("")
    c_ = c(b_)
    d_ = d(b_)
    g_ = g(c_, d_)
    return f_, c_, d_, g_


def test_excludenodes_basic():
    pytest.test_exclude_nodes = ""
    pipe_ = pipe.executor(exclude_nodes=[g])
    assert ("aef", "bc", "bd", None) == pipe_()
    assert set("abcdef") == set(pytest.test_exclude_nodes)


def test_exclude_main_node():
    pytest.test_exclude_nodes = ""
    pipe_ = pipe.executor(exclude_nodes=[b])
    assert ("aef", None, None, None) == pipe_()
    assert set("aef") == set(pytest.test_exclude_nodes)


def test_excludenodes_execute_all_nodes_without_return():
    pytest.test_exclude_nodes = ""
    pipe_ = pipe.executor(exclude_nodes=[f, g, c, d])
    assert (None, None, None, None) == pipe_()
    assert set("abe") == set(pytest.test_exclude_nodes)


def test_with_setup_nodes():
    @xn(setup=True)
    def z_setup(s):
        pytest.test_exclude_nodes += "z"
        return s + "z"

    @dag
    def pipe():
        f_ = f(e(z_setup("")))
        b_ = b("")
        c_ = c(b_)
        d_ = d(b_)
        g_ = g(c_, d_)
        return f_, c_, d_, g_

    pytest.test_exclude_nodes = ""
    pipe_ = deepcopy(pipe)
    pipe_exec = pipe_.executor(exclude_nodes=[e])
    assert (None, "bc", "bd", "bcbdg") == pipe_exec()
    assert set("zbcdg") == set(pytest.test_exclude_nodes)

    pipe_.setup(exclude_nodes=[e])
    assert pytest.test_exclude_nodes.count("z") == 1

    assert (None, "bc", "bd", "bcbdg") == pipe_exec()
    assert pytest.test_exclude_nodes.count("z") == 1

    assert ("zef", "bc", "bd", "bcbdg") == pipe_()
    assert pytest.test_exclude_nodes.count("z") == 1


def test_with_debug_nodes():
    c_debug = xn(debug=True)(c.exec_function)
    d_debug = xn(debug=True)(d.exec_function)
    g_debug = xn(debug=True)(g.exec_function)

    @dag
    def pipe():
        f_ = f(e(a("")))
        b_ = b("")
        c_ = c_debug(b_)
        d_ = d_debug(b_)
        g_ = g_debug(c_, d_)
        return f_, c_, d_, g_

    Cfg.RUN_DEBUG_NODES = True
    assert ("aef", "bc", "bd", "bcbdg") == pipe()
    # TODO: write clear documentation about priority of choosing exclude_nodes vs debug_nodes
    #  (debug_nodes are more prioritized!)
    assert ("aef", "bc", "bd", "bcbdg") == pipe(exclude_nodes=[g])
    assert ("aef", None, None, None) == pipe(exclude_nodes=[b])


# TODO: maybe write a test case with cache ?


def test_impossible_situation():
    pipe_exec = pipe.executor(target_nodes=[g], exclude_nodes=[d])
    with pytest.raises(TawaziUsageError):
        pipe_exec()


# test_excludenodes_basic()