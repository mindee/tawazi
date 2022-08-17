import inspect
import types
from threading import Lock
from typing import Any, Callable

from tawazi import DAG, ExecNode

# todo replace exec_nodes with dict
# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes = []
exec_nodes_lock = Lock()


class PrecalculatedExecNode(ExecNode):
    def __init__(self, argument_name: str, value: Any):
        super().__init__(
            id_=argument_name,
            exec_function=lambda: value,
            depends_on=[],
            argument_name=argument_name,
            is_sequential=False,
        )


def get_default_args(func):
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
        self, func: Callable, priority=0, argument_name=None, is_sequential=True
    ):
        # todo change the id_ of the execNode. Maybe remove it completely!
        super().__init__(
            id_=func,
            exec_function=func,
            depends_on=None,
            priority=priority,
            argument_name=argument_name,
            is_sequential=is_sequential,
        )

    def __call__(self, *args, **kwargs):
        """
        Record the dependencies in a global variable to be called later in DAG.
        Returns: ReplaceExecNode
        """

        # 0. if dependencies are already calculated, there is no need to recalculate them
        if self.calculated_dependencies and self in exec_nodes:
            return self

        dependencies = []
        provided_arguments_names = set()

        # todo refactor this part!!
        function_arguments_names = inspect.getfullargspec(self.exec_function)[0]
        for i, arg in enumerate(args):
            if isinstance(arg, LazyExecNode):
                dependencies.append(arg.id)
            else:
                # if the argument is a custom or constant
                prec_exec_node = PrecalculatedExecNode(function_arguments_names[i], arg)
                exec_nodes.append(prec_exec_node)
                dependencies.append(prec_exec_node.id)

            provided_arguments_names.add(function_arguments_names[i])

        for argument_name, arg in kwargs.items():
            if isinstance(arg, LazyExecNode):
                dependencies.append(arg.id)
            else:
                # if the argument is a custom or constant
                prec_exec_node = PrecalculatedExecNode(argument_name, arg)
                exec_nodes.append(prec_exec_node)
                dependencies.append(prec_exec_node.id)

            provided_arguments_names.add(argument_name)

        # Fill default valued parameters with default values if they aren't provided by the user
        default_valued_params = get_default_args(self.exec_function)
        for argument_name in set(function_arguments_names) - provided_arguments_names:
            # if the argument is a custom or constant
            prec_exec_node = PrecalculatedExecNode(
                argument_name, default_valued_params[argument_name]
            )
            exec_nodes.append(prec_exec_node)
            dependencies.append(prec_exec_node.id)

        self.depends_on = dependencies

        # in case the same function is called twice
        if self not in exec_nodes:
            exec_nodes.append(self)
        return self

    def __get__(self, instance, owner_cls=None):
        "Simulate func_descr_get() in Objects/funcobject.c"
        if instance is None:
            # this is the case when we call the method on the class instead of an instance of the class
            # In this case, we must return a "function" hence an instance of this class
            # https://stackoverflow.com/questions/3798835/understanding-get-and-set-and-python-descriptors
            return self
        return types.MethodType(self, instance)  # func=self  # obj=instance


# todo add the documentation of the replaced function!
# todo modify is_sequential's default value according to the preused default
def op(func=None, *, priority=0, argument_name=None, is_sequential=True):
    """
    Decorate a function to make it an ExecNode. When the decorated function is called, you are actually calling
    an ExecNode. This way we can record the dependencies in order to build the actual DAG. Check the example in the README
    for a guide to the usage.
    Args:
        func: a Callable that will be executed in the DAG
        priority: priority of the execution with respect to other ExecNodes
        argument_name: the name of the argument to be used by other ExecNodes when referring to the returned value of **this** ExecNode
        is_sequential: whether to allow the execution of this ExecNode with others or not

    Returns:
        ReplaceExecNode
    """

    def my_custom_op(_func: Callable):

        return LazyExecNode(_func, priority, argument_name, is_sequential)
        # to have the help and the name of the origianl function
        # rep_exec_node = ReplaceExecNode(func, priority, argument_name, is_sequential)
        # functools.update_wrapper(rep_exec_node, func)
        #
        # return rep_exec_node

    # if args are provided to the decorator
    if func is None:
        return my_custom_op
    # if no argument is provided
    else:
        return my_custom_op(func)


def to_dag(declare_dag_function: Callable):
    """
    Transform the declared ops into a DAG that can be executed.
    The same DAG can be executed multiple times

    Args:
        declare_dag_function: a function that contains functions decorated with @op decorator.
        The execution of this function must be really fast because almost no calculation happens here.
        Note: to_dag is thread safe because it uses an internal lock. If you need to construct lots of DAGs in multiple threads,
        it is best to construct your dag once and then consume it as much as you like in multiple threads.

    Returns: a DAG instance

    """

    def wrapped(*args, **kwargs):
        # todo modify this pattern! this is horrible!
        global exec_nodes
        with exec_nodes_lock:
            exec_nodes = []

            #
            declare_dag_function(*args, **kwargs)

            d = DAG(exec_nodes)
            exec_nodes = []

        return d

    return wrapped
