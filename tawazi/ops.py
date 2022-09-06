import functools
import inspect
import types
from threading import Lock
from typing import Any, Callable, Dict, List, Optional

from tawazi import DAG, ExecNode

# TODO: replace exec_nodes with dict
# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: List[ExecNode] = []
exec_nodes_lock = Lock()


class PreComputedExecNode(ExecNode):
    # todo must change this because two functions in the same DAG can use the same argument name for two constants!
    def __init__(self, argument_name: str, value: Any):
        super().__init__(
            id_=argument_name,
            exec_function=lambda: value,
            depends_on=[],
            argument_name=argument_name,
            is_sequential=False,
        )


def get_default_args(func: Callable[..., Any]) -> Dict[str, Any]:
    """
    retrieves the default arguments of a function
    Args:
        func: the function with unknown defaults

    Returns:
        the mapping between the arguments and their default value
    """
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


class LazyExecNode(ExecNode):
    """
    A lazy function simulator that records the dependencies to build the DAG
    """

    def __init__(
        self,
        func: Callable[..., Any],
        priority: int = 0,
        argument_name: Optional[str] = None,
        is_sequential: bool = True,
    ):
        # TODO: change the id_ of the execNode. Maybe remove it completely
        super().__init__(
            id_=func,
            exec_function=func,
            depends_on=None,
            priority=priority,
            argument_name=argument_name,
            is_sequential=is_sequential,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> "LazyExecNode":
        """
        Record the dependencies in a global variable to be called later in DAG.
        Returns: LazyExecNode
        """

        # 0. if dependencies are already calculated, there is no need to recalculate them
        if self.computed_dependencies and self in exec_nodes:
            return self

        dependencies = []
        provided_arguments_names = set()

        # TODO: refactor this part.
        function_arguments_names = inspect.getfullargspec(self.exec_function)[0]
        for i, arg in enumerate(args):
            if isinstance(arg, LazyExecNode):
                dependencies.append(arg.id)
            else:
                # if the argument is a custom or constant
                prec_exec_node = PreComputedExecNode(function_arguments_names[i], arg)
                exec_nodes.append(prec_exec_node)
                dependencies.append(prec_exec_node.id)

            provided_arguments_names.add(function_arguments_names[i])

        for argument_name, arg in kwargs.items():
            if isinstance(arg, LazyExecNode):
                dependencies.append(arg.id)
            else:
                # if the argument is a custom or constant
                prec_exec_node = PreComputedExecNode(argument_name, arg)
                exec_nodes.append(prec_exec_node)
                dependencies.append(prec_exec_node.id)

            provided_arguments_names.add(argument_name)

        # Fill default valued parameters with default values if they aren't provided by the user
        default_valued_params = get_default_args(self.exec_function)
        for argument_name in set(function_arguments_names) - provided_arguments_names:
            # if the argument is a custom or constant
            prec_exec_node = PreComputedExecNode(
                argument_name, default_valued_params[argument_name]
            )
            exec_nodes.append(prec_exec_node)
            dependencies.append(prec_exec_node.id)

        self.depends_on = dependencies

        # in case the same function is called twice
        if self not in exec_nodes:
            exec_nodes.append(self)
        return self

    def __get__(self, instance: "LazyExecNode", owner_cls: Optional[Any] = None) -> Any:
        """
        Simulate func_descr_get() in Objects/funcobject.c
        Args:
            instance:
            owner_cls:

        Returns:

        """
        if instance is None:
            # this is the case when we call the method on the class instead of an instance of the class
            # In this case, we must return a "function" hence an instance of this class
            # https://stackoverflow.com/questions/3798835/understanding-get-and-set-and-python-descriptors
            return self
        return types.MethodType(self, instance)  # func=self  # obj=instance


# TODO: add the documentation of the replaced function!
# TODO: modify is_sequential's default value according to the pre used default
def op(
    func: Optional[Callable[..., Any]] = None,
    *,
    priority: int = 0,
    argument_name: Optional[str] = None,
    is_sequential: bool = True
) -> "LazyExecNode":
    """
    Decorate a function to make it an ExecNode. When the decorated function is called, you are actually calling
    an ExecNode. This way we can record the dependencies in order to build the actual DAG.
    Please check the example in the README for a guide to the usage.
    Args:
        func: a Callable that will be executed in the DAG
        priority: priority of the execution with respect to other ExecNodes
        argument_name: name of the argument used by other ExecNodes referring to the returned value.
             Explanation:
             This ExecNode will return a value.
             This value will be used by other ExecNodes via their arguments.
             The name of the argument to be used is specified by this value.
        is_sequential: whether to allow the execution of this ExecNode with others or not

    Returns:
        LazyExecNode
    """

    def my_custom_op(_func: Callable[..., Any]) -> "LazyExecNode":
        lazy_exec_node = LazyExecNode(_func, priority, argument_name, is_sequential)
        functools.update_wrapper(lazy_exec_node, _func)
        return lazy_exec_node

    # case #1: arguments are provided to the decorator
    if func is None:
        return my_custom_op  # type: ignore
    # case #2: no argument is provided to the decorator
    else:
        return my_custom_op(func)


def to_dag(declare_dag_function: Callable[..., Any]) -> Callable[..., Any]:
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

    Returns: a DAG instance

    """

    def wrapped(*args: Any, **kwargs: Any) -> DAG:
        # TODO: modify this horrible pattern
        global exec_nodes
        with exec_nodes_lock:
            exec_nodes = []
            declare_dag_function(*args, **kwargs)
            d = DAG(exec_nodes)
            exec_nodes = []

        return d

    return wrapped
