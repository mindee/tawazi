"""Module for decorators used in Tawazi."""
import functools
from typing import Any, Callable, List, Optional, Union, overload

from loguru import logger

from tawazi import node
from tawazi.config import Cfg
from tawazi.consts import RVDAG, RVXN, P
from tawazi.dag import DAG
from tawazi.errors import ErrorStrategy, TawaziUsageError
from tawazi.helpers import get_args_and_default_args
from tawazi.node import (
    ArgExecNode,
    ExecNode,
    LazyExecNode,
    ReturnUXNsType,
    UsageExecNode,
    exec_nodes_lock,
    validate_returned_usage_exec_nodes,
)


@overload
def xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
    ...


@overload
def xn(
    *,
    priority: int = 0,
    is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[Any] = None,
    setup: bool = False,
    unpack_to: Optional[int] = None,
) -> Callable[[Callable[P, RVXN]], LazyExecNode[P, RVXN]]:
    ...


def xn(
    func: Optional[Callable[P, RVXN]] = None,
    *,
    priority: int = 0,
    is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    debug: bool = False,
    tag: Optional[Any] = None,
    setup: bool = False,
    unpack_to: Optional[int] = None,
) -> Union[Callable[[Callable[P, RVXN]], LazyExecNode[P, RVXN]], LazyExecNode[P, RVXN]]:
    """Decorate a function to make it an ExecNode.

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
        unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding to the given value

    Returns:
        LazyExecNode: The decorated function wrapped in a Callable.

    Raises:
        TypeError: If the decorated function passed is not a Callable.
    """

    def intermediate_wrapper(_func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
        lazy_exec_node = LazyExecNode(_func, priority, is_sequential, debug, tag, setup, unpack_to)
        functools.update_wrapper(lazy_exec_node, _func)
        return lazy_exec_node

    # case #1: arguments are provided to the decorator
    if func is None:
        return intermediate_wrapper
    # case #2: no argument is provided to the decorator
    if not callable(func):
        raise TypeError(f"{func} is not a callable. Did you use a non-keyword argument?")
    return intermediate_wrapper(func)


# helper functions
def _run_dag_description(
    _func: Callable[..., ReturnUXNsType], args: List[ExecNode]
) -> ReturnUXNsType:
    node.exec_nodes = {}
    # 2.3 Arguments are also ExecNodes that get executed inside the scheduler
    node.exec_nodes.update({xn.id: xn for xn in args})
    # 2.4 make UsageExecNodes for input arguments
    uxn_args = [UsageExecNode(xn.id) for xn in args]

    return _func(*uxn_args)


def _unpacking_unpackable_value(unp_err: TypeError) -> bool:
    unpack_error_msg = str(unp_err)
    if not unpack_error_msg.startswith("cannot unpack non-iterable"):
        return False
    return True


def _unpacking_with_wrong_number_of_returns(unp_err: ValueError) -> int:
    """Returns the number of expected iteration that must be returned for the unpacking to function properly.

    Args:
        unp_err (ValueError): The received unpacking error.

    Returns:
        int: 0 if the error is not an unpacking error,
            otherwise returns the number of expected value on the left hand side for the unpacking to work properly
    """
    unpack_error_msg = str(unp_err)
    # left hand side < right hand side
    if unpack_error_msg.startswith("too many values to unpack"):
        return int(str(unpack_error_msg).split("expected ")[1].split(")")[0])
    # left hand side > right hand side
    if unpack_error_msg.startswith("not enough values to unpack"):
        return int(str(unpack_error_msg).split("expected ")[1].split(",")[0])
    return 0


def _run_dag_description_automatic_unpacking(
    _func: Callable[..., Any], args: List[ExecNode]
) -> ReturnUXNsType:
    """Run the DAG description with automatic unpacking.

    there are 4 cases of unpacking:
                    left side  |  right side   |  example     |  produced error
    unpacking  |  False     |  non iter     |  a=1         |  None
    unpacking  |  True #1   |  non iter     |  a,=1        |  TypeError: cannot unpack non-iterable <type> object
    unpacking  |  True #2   |  non iter     |  a,b=1       |  TypeError: cannot unpack non-iterable <type> object
    unpacking  |  False     |  iter         |  a=1,2       |  assignment of tuple or iterable...
    unpacking  |  True <    |  True         |  a,=1,2      |  ValueError: too many values to unpack (expected 1)
    unpacking  |  True ==   |  True         |  a,=1,       |  assignment of tuple...
    unpacking  |  True >    |  True         |  a,b=1,      |  ValueError: not enough values to unpack (expected 2, got 1)

    So the logic behind detecting the number of unpacks is as follows:
    1. return a single value which is the default behavior of a = expression
    2.1 if no error is produced then latest assignment is valid, remove latest id from `last_failing_id` continue
    2.2 if error is produced, either it is a TypeError: cannot unpack non-iterable => assign `last_failing_id` should be expanded to know the expected expansion
    2.3     during next iteration, we can get the expected unpacking number from the error message
    2.4     we record it and we re-run the dag describing function
    3. if an unpacking error happens even though the ExecNode.unpack_to is assigned, raise a TawaziUsageError

    There is a limit of 10_000 maximum iterations for describing the DAG.
    """
    described_the_dag = False
    counter = 0
    logger.debug(f"describing {_func}")

    while counter < Cfg.TAWAZI_EXPERIMENTAL_MAX_UNPACK_TRIAL_ITERATIONS and not described_the_dag:
        logger.debug(f"trial number: {counter}")
        logger.debug(f"{node.exec_nodes=}")
        logger.debug(f"{node.unpack_number=}")
        logger.debug(f"{node.last_id=}")

        counter += 1
        try:
            returned_usage_exec_nodes: ReturnUXNsType = _run_dag_description(_func, args)
        except TypeError as unp_err:
            if _unpacking_unpackable_value(unp_err):
                # try unpacking to two variables (this might be the most common case)
                node.unpack_number[node.last_id] = 2
            else:
                raise unp_err from unp_err

        except ValueError as unp_err:
            expected_unpack_num = _unpacking_with_wrong_number_of_returns(unp_err)
            if expected_unpack_num == 0:
                raise unp_err from unp_err

            # if the unpacking error comes from unpack_to, the user made a misake
            if node.exec_nodes[node.last_id].unpack_to is not None:
                raise unp_err from unp_err
                # xn = node.exec_nodes[node.last_id]
                # raise TawaziUsageError(
                #     f"{xn} is expected to unpack its values to {xn.unpack_to}, "
                #     f"But during DAG description, left hand side has {expected_unpack_num} variables.") from unp_err

            # set the correct unpacking number
            node.unpack_number[node.last_id] = expected_unpack_num

        else:
            described_the_dag = True

    if counter == Cfg.TAWAZI_EXPERIMENTAL_MAX_UNPACK_TRIAL_ITERATIONS:
        raise TawaziUsageError(
            "internal problem while detecting the number of unpacks probably. "
            "Try increasing `TAWAZI_EXPERIMENTAL_MAX_UNPACK_TRIAL_ITERATIONS` -> 1000 - 10_000 "
            "if you have an extremely big DAG you are describing, "
            "or make sure that you are unpacking your returned variable correctly"
        )
    return returned_usage_exec_nodes


@overload
def dag(declare_dag_function: Callable[P, RVDAG]) -> DAG[P, RVDAG]:
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
    """Transform the declared ops into a DAG that can be executed by tawazi's scheduler.

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

    Raises:
        TypeError: If the decorated function passed is not a Callable.
    """

    # wrapper used to support parametrized and non parametrized decorators
    def intermediate_wrapper(_func: Callable[P, RVDAG]) -> DAG[P, RVDAG]:
        # 0. Protect against multiple threads declaring many DAGs at the same time
        with exec_nodes_lock:
            # 1. node.exec_nodes contains all the ExecNodes that concern the DAG being built at the moment.
            #      make sure it is empty
            node.unpack_number = {}
            node.last_id = ""
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

                # 3. Execute the dependency describer function
                # NOTE: Only ordered parameters are supported at the moment!
                #  No **kwargs!! Only positional Arguments
                # used to be fetch the results at the end of the computation
                returned_usage_exec_nodes: ReturnUXNsType = (
                    _run_dag_description_automatic_unpacking(_func, args)
                    if Cfg.TAWAZI_EXPERIMENTAL_AUTOMATIC_UNPACK
                    else _run_dag_description(_func, args)
                )

                # TODO: wrap the consts non UsageExecNodes in a UsageExecNode to support returning consts from DAG
                validate_returned_usage_exec_nodes(returned_usage_exec_nodes)

                # The next line is a duplication!
                uxn_args = [UsageExecNode(xn.id) for xn in args]

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
                node.unpack_number = {}
                node.last_id = ""

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
