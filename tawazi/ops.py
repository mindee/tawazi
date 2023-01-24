import functools
from typing import Any, Callable, List, Optional, Union

from tawazi import DAG
from tawazi.errors import ErrorStrategy
from tawazi.helpers import get_args_and_default_args

from . import node
from .config import Cfg
from .node import ArgExecNode, ExecNode, IdentityHash, LazyExecNode, exec_nodes_lock


# TODO: modify is_sequential's default value according to the pre used default
def op(
    func: Optional[Callable[..., Any]] = None,
    *,
    priority: int = 0,
    is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[Any] = None,
    setup: bool = False,
) -> LazyExecNode:
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
    # TODO: maybe setup nodes are not threadsafe!
    #   because they are shared between all threads!
    #   I mean shared per pipeline… so if you execute the same pipeline in multiple threads,
    #   it is not thread safe!
    # so we should execute it using a thread  safety of 1 !!

    def my_custom_op(_func: Callable[..., Any]) -> "LazyExecNode":
        lazy_exec_node = LazyExecNode(_func, priority, is_sequential, debug, tag, setup)
        functools.update_wrapper(lazy_exec_node, _func)
        return lazy_exec_node

    # case #1: arguments are provided to the decorator
    if func is None:
        return my_custom_op  # type: ignore
    # case #2: no argument is provided to the decorator
    else:
        return my_custom_op(func)


# TODO: delete!!!
# NOTE: deprecated!!
def _to_dag(
    declare_dag_function: Optional[Callable[..., Any]] = None,
    *,
    max_concurrency: int = 1,
    behavior: ErrorStrategy = ErrorStrategy.strict,
) -> Callable[..., DAG]:
    """
    deprecated!! use to_pipe instead
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


def to_dag(
    declare_dag_function: Optional[Callable[..., Any]] = None,
    *,
    max_concurrency: int = 1,
    behavior: ErrorStrategy = ErrorStrategy.strict,
) -> DAG:
    """
    Transform the declared ops into a DAG that can be executed by tawazi's scheduler.
    The same DAG can be executed multiple times.
    Note: to_dag is thread safe because it uses an internal lock.
        If you need to construct lots of DAGs in multiple threads,
        it is best to construct your dag once and then use it as much as you like.
    Please check the example in the README for a guide to the usage.
    Args:
        declare_dag_function: a function that contains the execution of the DAG.
        Currently Only @op decorated functions can be used inside the decorated function (i.e. declare_dag_function).
        However, you can use some simple python code to generate constants.
        max_concurrency: the maximum number of concurrent threads to execute in parallel.
        behavior: the behavior of the DAG when an error occurs during the execution of a function (ExecNode).
    Returns: a DAG instance
    """

    def intermediate_wrapper(_func: Callable[..., Any]) -> DAG:
        # 0. Protect against multiple threads declaring many DAGs at the same time
        with exec_nodes_lock:
            # 1. node.exec_nodes contains all the ExecNodes that concern the DAG being built at the moment.
            #      make sure it is empty
            node.exec_nodes = []

            # 2. make ExecNodes corresponding to the arguments of the ExecNode
            func_args, func_default_args = get_args_and_default_args(_func)
            # non default parameters must be provided!
            args: List[ExecNode] = [ArgExecNode(_func, arg_name) for arg_name in func_args]

            args.extend(
                [ArgExecNode(_func, arg_name, arg) for arg_name, arg in func_default_args.items()]
            )
            node.exec_nodes.extend(args)

            # Only ordered parameters are supported at the moment
            returned_exec_nodes = _func(*args)

            d = DAG(node.exec_nodes, max_concurrency=max_concurrency, behavior=behavior)
            # node.exec_nodes are deep copied inside the DAG.
            #   we can emtpy the global variable node.exec_nodes
            node.exec_nodes = []

            d.input_ids = [arg.id for arg in args]

            # make the return ids to be fetched at the end of the computation
            return_ids: Optional[Union[List[IdentityHash], IdentityHash]] = []
            if returned_exec_nodes is None:
                return_ids = None
            elif isinstance(returned_exec_nodes, ExecNode):
                return_ids = returned_exec_nodes.id
            elif isinstance(returned_exec_nodes, tuple):
                for ren in returned_exec_nodes:
                    if isinstance(ren, ExecNode):
                        # NOTE: maybe consider dropping the Optional
                        return_ids.append(ren.id)  # type: ignore
                    else:
                        # NOTE: this error shouldn't ever raise during usage.
                        # Please report in https://github.com/mindee/tawazi/issues
                        raise TypeError(
                            "Return type of the pipeline must be either an execNode or a tuple of ExecNode"
                        )
            else:
                raise TypeError(
                    "Return type of the pipeline must be either an execNode or a tuple of ExecNode"
                )

            d.return_ids = return_ids

        functools.update_wrapper(d, _func)
        return d

    # case 1: arguments are provided to the decorator
    if declare_dag_function is None:
        # return a decorator
        return intermediate_wrapper  # type: ignore
    # case 2: arguments aren't provided to the decorator
    else:
        return intermediate_wrapper(declare_dag_function)
