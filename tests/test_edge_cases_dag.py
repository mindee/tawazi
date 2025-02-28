import asyncio
import os
from contextvars import ContextVar
from functools import partial
from typing import Any

import pytest
from networkx import NetworkXUnfeasible
from tawazi import DAG, Resource, dag, xn
from tawazi._dag.digraph import DiGraphEx
from tawazi._dag.helpers import sync_execute
from tawazi._helpers import StrictDict
from tawazi.errors import TawaziUsageError
from tawazi.node import ExecNode, UsageExecNode


def shortcut_execute(dag: DAG[Any, Any], graph: DiGraphEx) -> Any:
    return sync_execute(
        results=dag.results,
        exec_nodes=dag.exec_nodes,
        max_concurrency=dag.max_concurrency,
        graph=graph,
    )


def a(c: str) -> str:
    return "a"


def b(a: str) -> str:
    return a + "b"


def c(b: str) -> str:
    return b + "c"


def func(a: str, b: str) -> str:
    return a + b


def test_same_constant_name_in_two_exec_nodes() -> None:
    @xn
    def a(cst: int) -> int:
        return cst

    @xn
    def b(a: int, cst: str) -> str:
        return str(a) + cst

    @dag
    def my_dag() -> None:
        var_a = a(1234)
        b(var_a, "poulpe")

    exec_nodes, results, _ = shortcut_execute(my_dag, my_dag.graph_ids.make_subgraph())
    assert len(exec_nodes) == 4
    assert results[a.id] == 1234
    assert results[b.id] == "1234poulpe"


def test_dag_with_weird_nodes() -> None:
    @xn
    def toto(a: str, b: str) -> str:
        return a + "_wow_" + b

    @dag
    def my_dag() -> tuple[Any, ...]:
        var_a = xn(partial(func, a="x"))(b="y")
        var_b = xn(partial(func, b="y"))(a="x")
        var_c = xn(lambda x: x)("e")
        var_d = toto(var_a, var_b)

        return var_a, var_b, var_c, var_d

    res_a, res_b, res_c, res_d = my_dag()

    assert res_a == "xy"
    assert res_b == "xy"
    assert res_c == "e"
    assert res_d == "xy_wow_xy"


def test_setup_debug_nodes() -> None:
    with pytest.raises(ValueError):

        @xn(debug=True, setup=True)
        def a() -> None: ...


def test_circular_deps() -> None:
    en_a = ExecNode(id_="a", exec_function=a, args=[], is_sequential=True)
    en_b = ExecNode(
        id_="b", exec_function=b, args=[UsageExecNode(en_a.id)], priority=2, is_sequential=False
    )
    en_c = ExecNode(
        id_="c", exec_function=c, args=[UsageExecNode(en_a.id)], priority=1, is_sequential=False
    )
    object.__setattr__(en_a, "args", [UsageExecNode(en_c.id)])  # typing: ignore[misc]

    with pytest.raises(NetworkXUnfeasible):
        DAG(
            qualname="test_circular_deps",
            results=StrictDict({}),
            exec_nodes=StrictDict({xn.id: xn for xn in [en_a, en_b, en_c]}),
            input_uxns=[],
            return_uxns=[],
            max_concurrency=2,
        )  # typing: ignore[misc]


def test_non_pickalable_args() -> None:
    @xn
    def _a(in1: Any = 1) -> Any:
        if isinstance(in1, int):
            return in1 + 1
        return in1

    @xn
    def _b(in1: int, in2: int) -> int:
        return in1 + in2

    @dag
    def pipe() -> tuple[Any, int]:
        return _a(os), _b(_a(10), _a())

    assert pipe() == (os, 13)


def test_kwargs_in_local_defined_dag() -> None:
    @dag
    def my_dag() -> str:
        return a(c="twinkle toes")

    assert my_dag() == "a"


@pytest.mark.parametrize("is_async", [True, False])
def test_kwargs_passed_to_dag(is_async: bool) -> None:
    @dag(is_async=is_async)
    def my_dag(c: str) -> str:
        return a(c)

    with pytest.raises(TawaziUsageError, match="currently DAG does not support keyword arguments"):
        if is_async:
            asyncio.run(my_dag(c="twinkle toes"))  # type: ignore[arg-type]
        else:
            my_dag(c="twinkle toes")


ctx_var: ContextVar[str] = ContextVar("ctx_var", default="twinkle")


@pytest.mark.parametrize("resource", [Resource.main_thread, Resource.thread, Resource.async_thread])
@pytest.mark.parametrize("is_async", [True, False])
def test_context(resource: Resource, is_async: bool) -> None:
    ctx_var.set("toes")

    @xn(resource=resource)
    def validate_context() -> None:
        assert ctx_var.get() == "toes"

    @dag(is_async=is_async)
    def validate_context_dag() -> None:
        validate_context()

    if is_async:
        asyncio.run(validate_context_dag())  # type: ignore[arg-type]
    else:
        validate_context_dag()
