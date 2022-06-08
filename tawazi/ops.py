from threading import Lock
from typing import Callable, Any
import inspect
import functools

from tawazi import DAG, ExecNode

# todo replace exec_nodes with dict
# a variable used to pass in exec_nodes to the DAG during building
# making the dag isn't thread safe
exec_nodes = []
exec_nodes_lock = Lock()


class PrecalculatedExecNode(ExecNode):
    def __init__(self, argument_name: str, value: Any):
        super().__init__(id_="temporary",
                         exec_function=lambda: value,
                         depends_on=[],
                         argument_name=argument_name,
                         is_sequential=False)
        self.id = f"{argument_name}@{id(self)}"
        self.calculated_dependencies = True


class ReplaceExecNode(ExecNode):
    def __init__(self, func: Callable, priority=0, argument_name=None, is_sequential=True):
        # todo change the id_ of the execNode. Maybe remove it completely!
        super().__init__(
            id_=func,
            exec_function=func,
            depends_on=None,
            priority=priority,
            argument_name=argument_name,
            is_sequential=is_sequential
        )

    @property
    def calculated_dependencies(self):
        return isinstance(self.depends_on, list)

    def __call__(self, *args, **kwargs):
        # record the dependencies in a list of execnodes ? or a dict of execnodes for ex.

        # 0. if dependencies are already calculated, there is no need to recalculate them
        if self.calculated_dependencies and self in exec_nodes:
            return self

        dependencies = []

        # todo refactor this part!!
        function_arguments_names = inspect.getfullargspec(self.exec_function)[0]
        for i, arg in enumerate(args):
            if isinstance(arg, ReplaceExecNode):
                dependencies.append(arg.id)
            else:
                # if the argument is a custom or constant
                prec_exec_node = PrecalculatedExecNode(function_arguments_names[i], arg)
                exec_nodes.append(prec_exec_node)
                dependencies.append(prec_exec_node.id)

        for argument_name, arg in kwargs.items():
            if isinstance(arg, ReplaceExecNode):
                dependencies.append(arg.id)
            else:
                # if the argument is a custom or constant
                prec_exec_node = PrecalculatedExecNode(argument_name, arg)
                exec_nodes.append(prec_exec_node)
                dependencies.append(prec_exec_node.id)

        self.depends_on = dependencies

        # in case the same function is called twice
        if self not in exec_nodes:
            exec_nodes.append(self)
        return self


# todo add the documentation of the replaced function!
# todo modify is_sequential's default value according to the preused default
def op(func=None, *, priority=0, argument_name=None, is_sequential=True):
    def my_custom_op(_func: Callable):

        return ReplaceExecNode(_func, priority, argument_name, is_sequential)
        # to have the help and the name of the origianl function
        # rep_exec_node = ReplaceExecNode(func, priority, argument_name, is_sequential)
        # functools.update_wrapper(rep_exec_node, func)
        #
        # return rep_exec_node

    if func is None:
        # keyworded arguments are provided for the decorators
        return my_custom_op
    else:
        # there is no argument provided
        return my_custom_op(func)


def to_dag(declare_dag_function: Callable):
    """
    Transform the declared ops into a DAG that can be executed.
    The same DAG can be executed multiple times

    Args:
        declare_dag_function: a function that contains the

    Returns: a DAG instance

    """

    def wrapped(*args, **kwargs):
        # todo modify this pattern! this is horrible!
        # temporary List containing ExecNodes for the DAG only
        global exec_nodes
        with exec_nodes_lock:
            exec_nodes = []

            declare_dag_function(*args, **kwargs)

            d = DAG(exec_nodes)
            exec_nodes = []

        return d

    return wrapped
