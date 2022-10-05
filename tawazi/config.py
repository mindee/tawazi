import functools

from tawazi import ExecNode


def change_default_exec_node_sequential_strategy(is_sequential: bool) -> None:
    """
    Change the default strategy of is_sequential according to your needs.
    Changing the default strategy back and forth is not recommended
    """
    # TODO: Modifying the behavior of the library like this might not be best practice

    func = functools.partialmethod(ExecNode.__init__, is_sequential=is_sequential)

    setattr(ExecNode, "__init__", func)
