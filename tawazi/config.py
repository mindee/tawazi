import functools

from .dag import ExecNode


def change_default_exec_node_sequential_strategy(is_sequential: bool) -> None:
    """
    Change the default strategy of is_sequential according to your needs.
    Changing the default strategy back and forth is not recommended
    """
    # TODO: Modifying the behaviour of the library like this might not be best practice
    ExecNode.__init__ = functools.partialmethod(ExecNode.__init__, is_sequential=is_sequential)  # type: ignore
