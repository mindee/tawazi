"""Decorators of Tawazi.

The user should use the decorators `@dag` and `@xn` to create Tawazi objects `DAG` and `ExecNode`.
"""

import functools
from typing import Any, Callable, Optional, Union, overload

from typing_extensions import Literal

from tawazi import AsyncDAG
from tawazi._dag import DAG, threadsafe_make_dag

from .config import cfg
from .consts import RVDAG, RVXN, P, Resource, TagOrTags
from .node import LazyExecNode


@overload
def xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]: ...


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
) -> LazyExecNode[P, RVXN]: ...


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
) -> Callable[[Callable[P, RVXN]], LazyExecNode[P, RVXN]]: ...


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
                It is best to invoke the DAG.setup method before using the DAG in a multithreaded environment.
                This problem will be resolved in the future
        unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding
                                   to the given value
        resource (str): the resource to use to execute this ExecNode. Defaults to "thread".

    Returns:
        LazyExecNode: The decorated function wrapped in an `ExecNode`.

    Raises:
        TypeError: If the decorated function passed is not a `Callable`.
    """

    def intermediate_wrapper(_func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
        lazy_exec_node: LazyExecNode[P, RVXN] = LazyExecNode(
            exec_function=_func,
            priority=priority,
            is_sequential=is_sequential,
            debug=debug,
            tag=tag,
            setup=setup,
            unpack_to=unpack_to,
            resource=resource,
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
def dag(declare_dag_function: Callable[P, RVDAG]) -> DAG[P, RVDAG]: ...


@overload
def dag(
    declare_dag_function: Callable[P, RVDAG],
    *,
    max_concurrency: int = 1,
    is_async: Literal[False] = False,
) -> DAG[P, RVDAG]: ...


@overload
def dag(
    declare_dag_function: Callable[P, RVDAG],
    *,
    max_concurrency: int = 1,
    is_async: Literal[True] = True,
) -> AsyncDAG[P, RVDAG]: ...


@overload
def dag(
    declare_dag_function: Callable[P, RVDAG], *, max_concurrency: int = 1, is_async: bool = False
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]: ...


@overload
def dag(
    *, max_concurrency: int = 1, is_async: Literal[False] = False
) -> Callable[[Callable[P, RVDAG]], DAG[P, RVDAG]]: ...


@overload
def dag(
    *, max_concurrency: int = 1, is_async: Literal[True] = True
) -> Callable[[Callable[P, RVDAG]], AsyncDAG[P, RVDAG]]: ...


@overload
def dag(
    *, max_concurrency: int = 1, is_async: bool = False
) -> Union[
    Callable[[Callable[P, RVDAG]], DAG[P, RVDAG]],
    Callable[[Callable[P, RVDAG]], AsyncDAG[P, RVDAG]],
]: ...


def dag(
    declare_dag_function: Optional[Callable[P, RVDAG]] = None,
    *,
    max_concurrency: int = 1,
    is_async: bool = False,
) -> Union[
    DAG[P, RVDAG],
    AsyncDAG[P, RVDAG],
    Callable[[Callable[P, RVDAG]], Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]],
]:
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
        is_async: if True, the returned object will be an `AsyncDAG` instead of a `DAG`.

    Returns:
        a `DAG` instance that can be used just like a normal Python function. It will be executed by Tawazi's scheduler.

    Raises:
        TypeError: If the decorated object is not a Callable.
    """

    # wrapper used to support parametrized and non parametrized decorators
    def intermediate_wrapper(_func: Callable[P, RVDAG]) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
        # 0. Protect against multiple threads declaring many DAGs at the same time
        d = threadsafe_make_dag(_func, max_concurrency, is_async)
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
