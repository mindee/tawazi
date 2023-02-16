import functools
from typing import Any, Callable, List, Optional

from tawazi.dag import DAG
from tawazi.errors import ErrorStrategy
from tawazi.helpers import get_args_and_default_args

from . import node
from .config import Cfg
from .node import ArgExecNode, ExecNode, LazyExecNode, exec_nodes_lock


def xn(
    func: Optional[Callable[..., Any]] = None,
    *,
    priority: int = 0,
    is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[Any] = None,
    setup: bool = False,
) -> LazyExecNode:
    """
    Decorate a function to make it an ExecNode.
    When the decorated function is called, you are actually calling an ExecNode.
    This way we can record the dependencies in order to build the actual DAG.
    Please check the example in the README for a guide to the usage.

    Args:
        func (Callable[..., Any]): a Callable that will be executed in the DAG
        priority (int): priority of the execution with respect to other ExecNodes
        is_sequential (bool): whether to allow the execution of this ExecNode with others or not
        debug (bool): if True, this node will be executed when the corresponding DAG runs in Debug mode.
            This means that this ExecNode will run if its inputs exists
        tag (Any): Any Hashable / immutable typed variable can be used to identify nodes (str, Tuples, int etc.).
            It is the responsibility of the user to provide this immutability of the tag.
        setup (bool): if True, this node will be executed only once during the lifetime of a DAG instance.
            Setup ExecNodes are meant to be used to load heavy data only once inside the execution pipeline and then be used as if the results were cached.
            This can be useful if you want to load heavy ML models, heavy Data etc.
            Note that you can run all / subset of the setup nodes by invoking the DAG.setup method
            NOTE setup nodes are currently not threadsafe!
                because they are shared between all threads!
                If you execute the same pipeline in multiple threads during the setup phase, the behavior is undefined.
                This is why it is best to invoke the DAG.setup method before using the DAG in a multithreaded environment.
                This problem will be resolved in the future
        tag (Optional[Any]): Any Hashable / immutable typed variable can be used to identify nodes (str, Tuple[str, ...])

    Returns:
        LazyExecNode: The decorated function wrapped in a Callable.
    """

    def intermediate_wrapper(_func: Callable[..., Any]) -> "LazyExecNode":
        lazy_exec_node = LazyExecNode(_func, priority, is_sequential, debug, tag, setup)
        functools.update_wrapper(lazy_exec_node, _func)
        return lazy_exec_node

    # case #1: arguments are provided to the decorator
    if func is None:
        return intermediate_wrapper  # type: ignore
    # case #2: no argument is provided to the decorator
    else:
        return intermediate_wrapper(func)


def dag(
    declare_dag_function: Optional[Callable[..., Any]] = None,
    *,
    max_concurrency: int = 1,
    behavior: ErrorStrategy = ErrorStrategy.strict,
) -> DAG:
    """
    Transform the declared ops into a DAG that can be executed by tawazi's scheduler.
    The same DAG can be executed multiple times.
    Note: dag is thread safe because it uses an internal lock.
        If you need to construct lots of DAGs in multiple threads,
        it is best to construct your dag once and then use it as much as you like.
    Please check the example in the README for a guide to the usage.

    Args:
        declare_dag_function: a function that contains the execution of the DAG.
            Currently Only @op decorated functions can be used inside the decorated function (i.e. declare_dag_function).
            However, you can use some simple python code to generate constants.
            Note However that these constants are computed only once during DAG declaration.
        max_concurrency: the maximum number of concurrent threads to execute in parallel.
        behavior: the behavior of the DAG when an error occurs during the execution of a function (ExecNode).

    Returns:
        a DAG instance
    """

    # wrapper used to support parametrized and non parametrized decorators
    def intermediate_wrapper(_func: Callable[..., Any]) -> DAG:
        # 0. Protect against multiple threads declaring many DAGs at the same time
        with exec_nodes_lock:
            # 1. node.exec_nodes contains all the ExecNodes that concern the DAG being built at the moment.
            #      make sure it is empty
            node.exec_nodes = []
            try:
                # 2. make ExecNodes corresponding to the arguments of the ExecNode
                # 2.1 get the names of the arguments and the default values
                func_args, func_default_args = get_args_and_default_args(_func)

                # 2.2 Construct non default arguments.
                # Corresponding values must be provided during usage
                args: List[ExecNode] = [ArgExecNode(_func, arg_name) for arg_name in func_args]
                # 2.2 Construct Default arguments.
                args.extend(
                    [
                        ArgExecNode(_func, arg_name, arg)
                        for arg_name, arg in func_default_args.items()
                    ]
                )
                # 2.3 Arguments are also ExecNodes that get executed inside the scheduler
                node.exec_nodes.extend(args)

                # 3. Execute the dependency describer function
                # NOTE: Only ordered parameters are supported at the moment!
                #  No **kwargs!! Only positional Arguments
                returned_exec_nodes = _func(*args)

                # 4. Construct the DAG instance
                d = DAG(
                    exec_nodes=node.exec_nodes,
                    max_concurrency=max_concurrency,
                    behavior=behavior,
                    input_ids=[arg.id for arg in args],
                    return_ids=returned_exec_nodes,
                )

            # clean up even in case an error is raised during dag construction
            finally:
                # 5. Clean global variable
                # node.exec_nodes are deep copied inside the DAG.
                #   we can empty the global variable node.exec_nodes
                node.exec_nodes = []

        functools.update_wrapper(d, _func)
        return d

    # case 1: arguments are provided to the decorator
    if declare_dag_function is None:
        # return a decorator
        return intermediate_wrapper  # type: ignore
    # case 2: arguments aren't provided to the decorator
    else:
        return intermediate_wrapper(declare_dag_function)
