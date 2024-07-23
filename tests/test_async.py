from typing import Literal

import pytest
from tawazi import dag, xn


@xn
def sync_xn() -> Literal["sync"]:
    return "sync"


@dag
def pipeline() -> str:
    return sync_xn()


@pytest.mark.asyncio
async def test_sync_in_async() -> None:
    with pytest.raises(
        RuntimeError, match="Cannot run the synchronous scheduler in an asynchronous context."
    ):
        assert pipeline() == "sync"
