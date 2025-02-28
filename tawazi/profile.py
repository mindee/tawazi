"""Module helper to profile execution time of Tawazi ExecNodes."""

from time import perf_counter, process_time, thread_time
from typing import Any


class Profile:
    """Profile records execution time of Tawazi ExecNodes."""

    def __init__(self, active: bool = True) -> None:
        """Profile an execution.

        Args:
            active (bool): Whether to activate the profile. Defaults to True.

        >>> p1, p2, = Profile(), Profile()
        >>> p1 == p2
        True
        >>> with p2:
        ...     pass
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
        self.active = active
        # [s.] (perf_counter()) system wide time
        self.abs_exec_time = 0.0
        # [s.] (process_time()) system & user time consumed by the process
        self.process_exec_time = 0.0
        # [s.] (thread_time()) system & user time consumed by the thread
        self.thread_exec_time = 0.0

    def __enter__(self) -> "Profile":
        """Context manager entry point.

        Returns:
            Profile: self
        """
        if self.active:
            self.abs_exec_time = perf_counter()
            self.process_exec_time = process_time()
            self.thread_exec_time = thread_time()

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit point.

        Args:
            exc_type (Any): ...
            exc_val (Any): ...
            exc_tb (Any): ...
        """
        if self.active:
            self.abs_exec_time = perf_counter() - self.abs_exec_time
            self.process_exec_time = process_time() - self.process_exec_time
            self.thread_exec_time = thread_time() - self.thread_exec_time
        return

    def __repr__(self) -> str:
        """Representation of the profile.

        Returns:
            str: profiled execution times in human-readable format
        """
        return f"Profile(abs_exec_time={self.abs_exec_time}, process_exec_time={self.process_exec_time}, thread_exec_time={self.thread_exec_time})"

    def __eq__(self, __o: object) -> bool:
        """Equality operator.

        Args:
            __o (Profile): The other Profile object.

        Raises:
            TawaziTypeError: If the other object is not an instance of Profile.

        Returns:
            bool: Whether both are equal.
        """
        if not isinstance(__o, Profile):
            return NotImplemented

        return (
            self.abs_exec_time == __o.abs_exec_time
            and self.process_exec_time == __o.process_exec_time
            and self.thread_exec_time == __o.thread_exec_time
        )

    def __lt__(self, __o: object) -> bool:
        """Less than operator.

        Args:
            __o (Profile): The other Profile object.

        Raises:
            TawaziTypeError: if the other object is not an instance of Profile.

        Returns:
            bool: Whether this Profile is less than the other.
        """
        if not isinstance(__o, Profile):
            return NotImplemented
        return self.abs_exec_time < __o.abs_exec_time
