from time import perf_counter, process_time, thread_time
from typing import Any

from tawazi.errors import TawaziTypeError


class Profile:
    def __init__(self) -> None:
        """
        Profile an execution
        >>> p1, p2, = Profile(), Profile()
        >>> p1 == p2
        True
        >>> p2.start()
        >>> p2.finish()
        >>> p1 < p2
        True
        >>> p1 == p2
        False
        >>> p1 > p2
        False
        >>> l = sorted([p2, p1])
        >>> l == [p1, p2]
        True
        """
        self.abs_exec_time = 0.0  # [s.] (perf_counter()) system wide time
        self.process_exec_time = (
            0.0  # [s.] (process_time()) system & user time consumed by the process
        )
        self.thread_exec_time = (
            0.0  # [s.] (thread_time()) system & user time consumed by the thread
        )

    def start(self) -> None:
        self.abs_exec_time = perf_counter()
        self.process_exec_time = process_time()
        self.thread_exec_time = thread_time()

    def finish(self) -> None:
        self.abs_exec_time = perf_counter() - self.abs_exec_time
        self.process_exec_time = process_time() - self.process_exec_time
        self.thread_exec_time = thread_time() - self.thread_exec_time

    def __enter__(self) -> "Profile":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.finish()

    def __repr__(self) -> str:
        return f"{self.abs_exec_time=}\n" f"{self.process_exec_time=}\n" f"{self.thread_exec_time=}"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Profile):
            raise TawaziTypeError(f"{__o} is not an instance of Profile")

        eq = (
            self.abs_exec_time == __o.abs_exec_time
            and self.process_exec_time == __o.process_exec_time
            and self.thread_exec_time == __o.thread_exec_time
        )

        return eq

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, Profile):
            raise TawaziTypeError(f"{__o} is not an instance of Profile")

        return self.abs_exec_time < __o.abs_exec_time
