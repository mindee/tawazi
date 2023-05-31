"""Decorators of Tawazi.

The user should use the decorators `@dag` and `@xn` to create Tawazi objects `DAG` and `ExecNode`.
"""
import functools
from typing import Any, Callable, List, Optional, Union, overload

from tawazi._dag import DAG
from tawazi._helpers import get_args_and_default_args
from tawazi.errors import ErrorStrategy

from .config import cfg
from .consts import RVDAG, RVXN, P, Resource, TagOrTags
from .node import (
    ArgExecNode,
    ExecNode,
    LazyExecNode,
    ReturnUXNsType,
    UsageExecNode,
    node,
    wrap_in_uxns,
)


@overload
def xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
    ...


@overload
def xn(
    func: Callable[P, RVXN],
    *,
    priority: int = 0,
    is_sequential: bool = cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[Any] = None,
    setup: bool = False,
    unpack_to: Optional[int] = None,
    resource: Resource = cfg.TAWAZI_DEFAULT_RESOURCE,
) -> LazyExecNode[P, RVXN]:
    ...


@overload
def xn(
    *,
    priority: int = 0,
    is_sequential: bool = cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[Any] = None,
    setup: bool = False,
    unpack_to: Optional[int] = None,
    resource: Resource = cfg.TAWAZI_DEFAULT_RESOURCE,
) -> Callable[[Callable[P, RVXN]], LazyExecNode[P, RVXN]]:
    ...


def xn(
    func: Optional[Callable[P, RVXN]] = None,
    *,
    priority: int = 0,
    is_sequential: bool = cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[TagOrTags] = None,
    setup: bool = False,
    unpack_to: Optional[int] = None,
    resource: Resource = cfg.TAWAZI_DEFAULT_RESOURCE,
) -> Union[Callable[[Callable[P, RVXN]], LazyExecNode[P, RVXN]], LazyExecNode[P, RVXN]]:
    """Decorate a normal function to make it an ExecNode.

    When the decorated function is called inside a `DAG`, you are actually calling an `ExecNode`.
    This way we can record the dependencies in order to build the actual DAG.
    Please check the example in the README for a guide to the usage.

    Args:
        func ([Callable[P, RVXN]): a Callable that will be executed in the `DAG`
        priority (int): priority of the execution with respect to other `ExecNode`s
        is_sequential (bool): whether to allow the execution of this `ExecNode` with others or not.
            If `True`, all other `ExecNode` currently running will stop before this one starts executing.
        debug (bool): if `True`, will execute only when Debug mode is active.
            a debug `ExecNode` will run its inputs exists regardless of subgraph choice.
        tag (Optional[TagOrTags]): a str or Tuple[str] to tag this ExecNode.
            If Tuple[str] is given, every value of the tuple is used as tag.
            Notice that multiple ExecNodes can have the same tag.
        setup (bool): if True, will be executed only once during the lifetime of a `DAG` instance.
            Setup `ExecNode`s are meant to be used to load heavy data only once inside the execution pipeline
            and then be used as if the results of their execution were cached.
            This can be useful if you want to load heavy ML models, heavy Data etc.
            Note that you can run all / subset of the setup nodes by invoking the DAG.setup method
            NOTE setup nodes are currently not threadsafe!
                because they are shared between all threads!
                If you execute the same pipeline in multiple threads during the setup phase, the behavior is undefined.
                This is why it is best to invoke the DAG.setup method before using the DAG in a multithreaded environment.
                This problem will be resolved in the future
        unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding to the given value
        resource (str): the resource to use to execute this ExecNode. Defaults to "thread".

    Returns:
        LazyExecNode: The decorated function wrapped in an `ExecNode`.

    Raises:
        TypeError: If the decorated function passed is not a `Callable`.
    """

    def intermediate_wrapper(_func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
        lazy_exec_node = LazyExecNode(
            _func, priority, is_sequential, debug, tag, setup, unpack_to, resource
        )
        functools.update_wrapper(lazy_exec_node, _func)
        return lazy_exec_node

    # case #1: arguments are provided to the decorator
    if func is None:
        return intermediate_wrapper
    # case #2: no argument is provided to the decorator
    if not callable(func):
        raise TypeError(f"{func} is not a callable. Did you use a non-keyword argument?")
    return intermediate_wrapper(func)


@overload
def dag(declare_dag_function: Callable[P, RVDAG]) -> DAG[P, RVDAG]:
    ...


@overload
def dag(
    declare_dag_function: Callable[P, RVDAG],
    *,
    max_concurrency: int = 1,
    behavior: ErrorStrategy = ErrorStrategy.strict,
) -> DAG[P, RVDAG]:
    ...


@overload
def dag(
    *, max_concurrency: int = 1, behavior: ErrorStrategy = ErrorStrategy.strict
) -> Callable[[Callable[P, RVDAG]], DAG[P, RVDAG]]:
    ...


def dag(
    declare_dag_function: Optional[Callable[P, RVDAG]] = None,
    *,
    max_concurrency: int = 1,
    behavior: ErrorStrategy = ErrorStrategy.strict,
) -> Union[Callable[[Callable[P, RVDAG]], DAG[P, RVDAG]], DAG[P, RVDAG]]:
    """Transform the declared `ExecNode`s into a DAG that can be executed by Tawazi's scheduler.

    The same DAG can be executed multiple times.
    Note: dag is thread safe because it uses an internal lock.
        If you need to construct lots of DAGs in multiple threads,
        it is best to construct your dag once and then use it as much as you like.
    Please check the example in the README for a guide to the usage.

    Args:
        declare_dag_function: a function that describes the execution of the DAG.
            This function should only contain calls to `ExecNode`s and data Exchange between them.
            (i.e. You can not use a normal Python function inside it unless decorated with `@xn`.)
            However, you can use some simple python code to generate constants.
            These constants are computed only once during the `DAG` declaration.
        max_concurrency: the maximum number of concurrent threads to execute in parallel.
        behavior: the behavior of the `DAG` when an error occurs during the execution of a function (`ExecNode`).

    Returns:
        a `DAG` instance that can be used just like a normal Python function. However it will be executed by Tawazi's scheduler.

    Raises:
        TypeError: If the decorated object is not a Callable.
    """

    # wrapper used to support parametrized and non parametrized decorators
    def intermediate_wrapper(_func: Callable[P, RVDAG]) -> DAG[P, RVDAG]:
        # 0. Protect against multiple threads declaring many DAGs at the same time
        with node.exec_nodes_lock:
            # 1. node.exec_nodes contains all the ExecNodes that concern the DAG being built at the moment.
            #      make sure it is empty
            node.exec_nodes = {}
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
                node.exec_nodes.update({xn.id: xn for xn in args})
                # 2.4 make UsageExecNodes for input arguments
                uxn_args = [UsageExecNode(xn.id) for xn in args]

                # 3. Execute the dependency describer function
                # NOTE: Only ordered parameters are supported at the moment!
                #  No **kwargs!! Only positional Arguments
                # used to be fetch the results at the end of the computation
                returned_val: Any = _func(*uxn_args)  # type: ignore[arg-type]

                returned_usage_exec_nodes: ReturnUXNsType = wrap_in_uxns(_func, returned_val)

                # 4. Construct the DAG instance
                d: DAG[P, RVDAG] = DAG(
                    node.exec_nodes,
                    input_uxns=uxn_args,
                    return_uxns=returned_usage_exec_nodes,
                    max_concurrency=max_concurrency,
                    behavior=behavior,
                )

            # clean up even in case an error is raised during dag construction
            finally:
                # 5. Clean global variable
                # node.exec_nodes are deep copied inside the DAG.
                #   we can empty the global variable node.exec_nodes
                node.exec_nodes = {}

        functools.update_wrapper(d, _func)
        return d

    # case 1: arguments are provided to the decorator
    if declare_dag_function is None:
        # return a decorator
        return intermediate_wrapper
    # case 2: arguments aren't provided to the decorator
    if not callable(declare_dag_function):
        raise TypeError(
            f"{declare_dag_function} is not a callable. Did you use a non-keyword argument?"
        )
    return intermediate_wrapper(declare_dag_function)
