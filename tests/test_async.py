import asyncio
from time import sleep, time
from typing import Literal

import pytest
from tawazi import Resource, dag, xn


@pytest.mark.asyncio
async def test_sync_in_async() -> None:
    @xn
    def sync_xn() -> Literal["sync"]:
        return "sync"

    @dag
    def pipeline() -> str:
        return sync_xn()

    with pytest.raises(RuntimeError, match="cannot be called from a running event loop"):
        assert pipeline() == "sync"


@xn
def a() -> Literal["a"]:
    return "a"


@xn
def b() -> Literal["b"]:
    return "b"


@dag
async def pipeline() -> str:
    return a() + b()


@pytest.mark.asyncio
async def test_async_in_async() -> None:
    assert await pipeline() == "ab"


def test_async_in_sync_without_await() -> None:
    assert asyncio.iscoroutine(pipeline())


@xn(resource=Resource.thread_async)
def threaded_async_sleep(t: float) -> str:
    sleep(t)
    return "slept"


@dag
async def pipeline_sleep(t: float) -> str:
    return threaded_async_sleep(t)


@pytest.mark.asyncio
async def test_async_call_next_to_async_pipeline() -> None:
    t0 = time()
    assert await asyncio.gather(asyncio.sleep(0.1, result="toes"), pipeline_sleep(0.1)) == [  # type: ignore[comparison-overlap]
        "toes",
        "slept",
    ]
    duration = time() - t0
    assert duration < 0.2

    t0 = time()
    assert await asyncio.gather(pipeline_sleep(0.1), asyncio.sleep(0.1, result="toes")) == [  # type: ignore[comparison-overlap]
        "slept",
        "toes",
    ]
    duration = time() - t0
    assert duration < 0.2


@pytest.mark.asyncio
async def test_make_async_executor() -> None:
    stateful_pipeline = pipeline_sleep.executor()
    assert await stateful_pipeline(0.1) == "slept"
