import inspect
import warnings
from typing import Any, Callable, Union

from tawazi._helpers import StrictDict
from tawazi.consts import RVDAG, P
from tawazi.node import ArgExecNode, ExecNode, ReturnUXNsType, UsageExecNode, node, wrap_in_uxns
from tawazi.node.node import make_axn_id

from .dag import DAG, AsyncDAG


def get_args_and_default_args(func: Callable[..., Any]) -> tuple[list[str], dict[str, Any]]:
    """Retrieves the arguments names and the default arguments of a function.

    Args:
        func: the target function

    Returns:
        A Tuple containing a List of argument names of non default arguments,
         and the mapping between the arguments and their default value for default arguments

    >>> def f(a1, a2, *args, d1=123, d2=None): pass
    >>> get_args_and_default_args(f)
    (['a1', 'a2', 'args'], {'d1': 123, 'd2': None})
    """
    signature = inspect.signature(func)
    args = []
    default_args = {}
    for k, v in signature.parameters.items():
        if v.default is not inspect.Parameter.empty:
            default_args[k] = v.default
        else:
            args.append(k)

    return args, default_args


def make_dag(
    _func: Callable[P, RVDAG], max_concurrency: int, is_async: bool
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
    """Make a DAG or AsyncDAG from the function that describes the DAG."""
    # 2. make ExecNodes corresponding to the arguments of the ExecNode
    # 2.1 get the names of the arguments and the default values
    func_args, func_default_args = get_args_and_default_args(_func)

    # 2.2 Construct non default arguments.
    # Corresponding values must be provided during usage
    args: list[ExecNode] = [
        ArgExecNode(make_axn_id(_func.__qualname__, arg_name)) for arg_name in func_args
    ]
    # 2.2 Construct Default arguments.
    for arg_name, arg in func_default_args.items():
        axn = ArgExecNode(make_axn_id(_func.__qualname__, arg_name))
        args.append(axn)
        node.results[axn.id] = arg

    # 2.3 Arguments are also ExecNodes that get executed inside the scheduler
    node.exec_nodes.update({exec_node.id: exec_node for exec_node in args})
    # 2.4 make UsageExecNodes for input arguments
    uxn_args = [UsageExecNode(exec_node.id) for exec_node in args]

    # 3. Execute the dependency describer function
    # NOTE: Only ordered parameters are supported at the moment!
    #  No **kwargs!! Only positional Arguments
    # used to be fetch the results at the end of the computation
    returned_val: Any = _func(*uxn_args)  # type: ignore[call-arg,arg-type]

    returned_usage_exec_nodes: ReturnUXNsType = wrap_in_uxns(_func, returned_val)

    # 4. Construct the DAG/AsyncDAG instance
    if is_async:
        return AsyncDAG(
            qualname=_func.__qualname__,
            results=node.results,
            exec_nodes=node.exec_nodes,
            input_uxns=uxn_args,
            return_uxns=returned_usage_exec_nodes,
            max_concurrency=max_concurrency,
        )
    return DAG(
        qualname=_func.__qualname__,
        results=node.results,
        exec_nodes=node.exec_nodes,
        input_uxns=uxn_args,
        return_uxns=returned_usage_exec_nodes,
        max_concurrency=max_concurrency,
    )


def wrap_make_dag(
    _func: Callable[P, RVDAG], max_concurrency: int, is_async: bool
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
    """Clean up before and after making the DAG."""
    # 1. node.exec_nodes contains all the ExecNodes that concern the DAG being built at the moment.
    #      make sure it is empty
    node.exec_nodes = StrictDict()
    node.results = StrictDict()
    node.DAG_PREFIX = []

    try:
        return make_dag(_func, max_concurrency, is_async)
    except NameError as e:
        if _func.__name__ in e.args[0]:
            warnings.warn("Recursion is not supported for DAGs", stacklevel=3)
        raise e
    # clean up even in case an error is raised during dag construction
    finally:
        # 5. Clean global variable
        # node.* are global variables, their value is used in the DAG.
        node.exec_nodes = StrictDict()
        node.results = StrictDict()
        node.DAG_PREFIX = []


def threadsafe_make_dag(
    _func: Union[Callable[P, RVDAG]], max_concurrency: int, is_async: bool
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
    """Make DAG or AsyncDAG form the function that describes the DAG.

    Thread safe and cleans after itself.
    """
    with node.exec_nodes_lock:
        return wrap_make_dag(_func, max_concurrency, is_async)
