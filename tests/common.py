import asyncio
from typing import Any, TypeVar

from tawazi import xn

T = TypeVar("T")


@xn
def stub(x: T) -> T:
    return x


def run_pipeline(*args: Any, pipeline: Any, is_async: bool) -> None:
    if is_async:
        asyncio.run(pipeline(*args))
    else:
        pipeline(*args)
