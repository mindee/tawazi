import functools
from typing import Any, Callable, Optional

from tawazi import DAG
from tawazi.errors import ErrorStrategy

from . import node
from .config import Cfg
from .node import LazyExecNode, exec_nodes_lock


# TODO: modify is_sequential's default value according to the pre used default
def op(
    func: Optional[Callable[..., Any]] = None,
    *,
    priority: int = 0,
    is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
) -> "LazyExecNode":
    """
    Decorate a function to make it an ExecNode. When the decorated function is called, you are actually calling
    an ExecNode. This way we can record the dependencies in order to build the actual DAG.
    Please check the example in the README for a guide to the usage.
    Args:
        func: a Callable that will be executed in the DAG
        priority: priority of the execution with respect to other ExecNodes
        is_sequential: whether to allow the execution of this ExecNode with others or not

    Returns:
        LazyExecNode
    """

    def my_custom_op(_func: Callable[..., Any]) -> "LazyExecNode":
        lazy_exec_node = LazyExecNode(_func, priority, is_sequential)
        functools.update_wrapper(lazy_exec_node, _func)
        return lazy_exec_node

    # case #1: arguments are provided to the decorator
    if func is None:
        return my_custom_op  # type: ignore
    # case #2: no argument is provided to the decorator
    else:
        return my_custom_op(func)


def to_dag(
    declare_dag_function: Optional[Callable[..., Any]] = None,
    *,
    max_concurrency: int = 1,
    behavior: ErrorStrategy = ErrorStrategy.strict,
) -> Callable[..., DAG]:
    """
    Transform the declared ops into a DAG that can be executed by tawazi.
    The same DAG can be executed multiple times.
    Note: to_dag is thread safe because it uses an internal lock.
        If you need to construct lots of DAGs in multiple threads,
        it is best to construct your dag once and then consume it as much as you like in multiple threads.
    Please check the example in the README for a guide to the usage.
    Args:
        declare_dag_function: a function that contains the execution of the DAG.
        if the functions are decorated with @op decorator they can be executed in parallel.
        Otherwise, they will be executed immediately. Currently mixing the two behaviors isn't supported.
        max_concurrency: the maximum number of concurrent threads to execute in parallel
        behavior: the behavior of the dag when an error occurs during the execution of a function (ExecNode)
    Returns: a DAG instance

    """

    def intermediate_wrapper(_func: Callable[..., Any]) -> Callable[..., DAG]:
        def wrapped(*args: Any, **kwargs: Any) -> DAG:
            # TODO: modify this horrible pattern
            with exec_nodes_lock:
                node.exec_nodes = []
                _func(*args, **kwargs)
                d = DAG(node.exec_nodes, max_concurrency=max_concurrency, behavior=behavior)
                node.exec_nodes = []

            return d

        return wrapped

    # case 1: arguments are provided to the decorator
    if declare_dag_function is None:
        # return a decorator
        return intermediate_wrapper  # type: ignore
    # case 2: arguments aren't provided to the decorator
    else:
        return intermediate_wrapper(declare_dag_function)
