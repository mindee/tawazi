from typing import Any

from tawazi import cfg


class UseDill:
    def __enter__(self) -> None:
        cfg.TAWAZI_DILL_NODES = True

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        cfg.TAWAZI_DILL_NODES = False
