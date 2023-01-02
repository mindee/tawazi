import inspect
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger

from tawazi.errors import UnvalidExecNodeCall

from .config import Cfg

# TODO: replace exec_nodes with dict
# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: List["ExecNode"] = []
exec_nodes_lock = Lock()
IdentityHash = str


class ExecNode:
    """
    This class is the base executable node of the Directed Acyclic Execution Graph
    """

    # todo deprecate calling the init directly!
    def __init__(
        self,
        id_: IdentityHash,
        exec_function: Callable[..., Any] = lambda **kwargs: None,
        depends_on: Optional[List[Tuple[Optional[str], IdentityHash]]] = None,
        priority: int = 0,
        is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    ):
        """
        Args:
            id_ (IdentityHash): identifier of ExecNode.
            exec_function (Callable, optional): a callable will be executed in the graph.
            TODO: must support *args, **kwargs
            This is useful to make Joining ExecNodes (Nodes that enforce dependencies on the graph)
            depends_on (list): a list of ExecNodes' ids that must be executed beforehand.
            priority (int, optional): priority compared to other ExecNodes;
                the higher the number the higher the priority.
            is_sequential (bool, optional): whether to execute this ExecNode in sequential order with respect to others.
             When this ExecNode must be executed, all other nodes are waited to finish before starting execution.
             Defaults to False.
        """

        self.id = id_
        self.exec_function = exec_function
        self.depends_on = depends_on if depends_on else []
        self.priority: int = priority
        self.compound_priority: int = priority
        self.is_sequential = is_sequential

        # a string that identifies the ExecNode.
        # It is either the name of the identifying function or the identifying string id_
        self.__name__ = self.exec_function.__name__ if not isinstance(id_, str) else id_

        # todo: remove and make ExecNode immutable ?
        self.result: Optional[Dict[str, Any]] = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} {self.id} ~ | <{hex(id(self))}>"

    @property
    def computed_dependencies(self) -> bool:
        return isinstance(self.depends_on, list)

    def execute(self, node_dict: Dict[IdentityHash, "ExecNode"]) -> Optional[Dict[str, Any]]:
        """
        Execute the ExecNode directly or according to an execution graph.
        Args:
            node_dict (Dict[IdentityHash, ExecNode]): A shared dictionary containing the other ExecNodes in the DAG;
                                                the key is the id of the ExecNode.

        Returns: the result of the execution of the current ExecNode
        """
        logger.debug(f"Start executing {self.id} with task {self.exec_function}")

        # 1. fabricate the arguments for this ExecNode
        args = [
            node_dict[dep_hash].result for arg_name, dep_hash in self.depends_on if arg_name is None
        ]
        kwargs = {
            arg_name: node_dict[dep_hash].result
            for arg_name, dep_hash in self.depends_on
            if arg_name is not None
        }

        # 2. write the result
        self.result = self.exec_function(*args, **kwargs)

        # 3. useless return value
        logger.debug(f"Finished executing {self.id} with task {self.exec_function}")
        return self.result


class PreComputedExecNode(ExecNode):
    # TODO: must change this because two functions in the same DAG can use the same argument name for two constants!
    def __init__(self, argument_name: str, func: Callable[..., Any], value: Any):
        super().__init__(
            id_=f"{func.__qualname__} >>> {argument_name}",  # f"{hash(func)}_{argument_name}",
            exec_function=lambda: value,
            depends_on=[],
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
        is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    ):
        # TODO: change the id_ of the execNode. Maybe remove it completely
        # NOTE: this means that a DAG must have a different named functions which is already True!
        super().__init__(
            id_=func.__qualname__,
            exec_function=func,
            depends_on=None,
            priority=priority,
            is_sequential=is_sequential,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> "LazyExecNode":
        """
        Record the dependencies in a global variable to be called later in DAG.
        Returns: LazyExecNode
        """
        # TODO: test
        if not exec_nodes_lock.locked:
            raise UnvalidExecNodeCall(
                "Invoking ExecNode __call__ is only allowed inside a @to_dag decorated function"
            )

        # 0. if dependencies are already calculated, there is no need to recalculate them
        if self.computed_dependencies and self in exec_nodes:
            return self

        # the Tuple contains (name_of_argument_used, execnode_id_of_argument)
        dependencies: List[Tuple[Optional[str], str]] = []
        # provided_arguments_names = set()

        # TODO: refactor this part.
        function_arguments_names = inspect.getfullargspec(self.exec_function)[0]
        for i, arg in enumerate(args):
            if isinstance(arg, LazyExecNode):
                # NOTE: is it possible to pass the arg itself and avoid passing the id as dependency ?
                dependencies.append((None, arg.id))
            else:
                # if the argument is a custom or constant
                pre_c_exec_node = PreComputedExecNode(
                    function_arguments_names[i], self.exec_function, arg
                )
                exec_nodes.append(pre_c_exec_node)
                dependencies.append((None, pre_c_exec_node.id))

            # provided_arguments_names.add(function_arguments_names[i])

        for key, arg in kwargs.items():
            if isinstance(arg, LazyExecNode):
                dependencies.append((key, arg.id))
            else:
                # if the argument is a custom or constant
                pre_c_exec_node = PreComputedExecNode(key, self.exec_function, arg)
                exec_nodes.append(pre_c_exec_node)
                dependencies.append((key, pre_c_exec_node.id))

            # provided_arguments_names.add(argument_name)

        # TODO: support default arguments
        # Fill default valued parameters with default values if they aren't provided by the user
        # default_valued_params = get_default_args(self.exec_function)
        # for argument_name in set(function_arguments_names) - provided_arguments_names:
        #     # if the argument is a custom or constant
        #     pre_c_exec_node = PreComputedExecNode(
        #         self.exec_function, default_valued_params[argument_name]
        #     )
        #     exec_nodes.append(pre_c_exec_node)
        #     dependencies.append(pre_c_exec_node.id)

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
        return MethodType(self, instance)  # func=self  # obj=instance
