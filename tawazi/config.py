from .dag import ExecNode
import functools

def change_default_exec_node_sequential_strategy(is_sequential):
    """
    Change the default startegy of is_sequential according to your needs.
    Changing the default strategy back&forth is not recommended
    """
    # todo review this technique of modifying the behavior of the library !?
    ExecNode.__init__ = functools.partialmethod(ExecNode.__init__, is_sequential=is_sequential)
