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


# TODO: make a helpers module and put this function in it
def ordinal(numb: int) -> str:
    if numb < 20:  # determining suffix for < 20
        if numb == 1:
            suffix = "st"
        elif numb == 2:
            suffix = "nd"
        elif numb == 3:
            suffix = "rd"
        else:
            suffix = "th"
    else:  # determining suffix for > 20
        tens = str(numb)
        tens = tens[-2]
        unit = str(numb)
        unit = unit[-1]
        if tens == "1":
            suffix = "th"
        else:
            if unit == "1":
                suffix = "st"
            elif unit == "2":
                suffix = "nd"
            elif unit == "3":
                suffix = "rd"
            else:
                suffix = "th"
    return str(numb) + suffix


# TODO: transfer to a helpers file
# TODO: change the name!!
def get_args_and_default_args(func: Callable[..., Any]) -> Tuple[List[str], Dict[str, Any]]:
    """
    retrieves the default arguments of a function
    Args:
        func: the function with unknown defaults

    Returns:
        the mapping between the arguments and their default value
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


class ExecNode:
    """
    This class is the base executable node of the Directed Acyclic Execution Graph
    """

    # todo deprecate calling the init directly!
    def __init__(
        self,
        id_: IdentityHash,
        exec_function: Callable[..., Any] = lambda *args, **kwargs: None,
        args: Optional[List["ExecNode"]] = None,
        kwargs: Optional[Dict[str, "ExecNode"]] = None,
        priority: int = 0,
        is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
    ):
        """
        Args:
            id_ (IdentityHash): identifier of ExecNode.
            exec_function (Callable, optional): a callable will be executed in the graph.
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

        self.args = args or []
        self.kwargs = kwargs or {}

        self.priority: int = priority
        self.compound_priority: int = priority
        self.is_sequential = is_sequential

        # a string that identifies the ExecNode.
        # It is either the name of the identifying function or the identifying string id_
        self.__name__ = self.exec_function.__name__ if not isinstance(id_, str) else id_

        # todo: remove and make ExecNode immutable ?
        self.result: Optional[Dict[str, Any]] = None
        # TODO: use the PreComputedExecNode instead ?
        self.executed = False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} {self.id} ~ | <{hex(id(self))}>"

    # TODO: make cached_property ?
    @property
    def depends_on(self) -> List["ExecNode"]:
        # Making the dependencies
        # 1. from args
        deps = self.args.copy()
        # 2. and from kwargs
        deps.extend(self.kwargs.values())

        return deps

    def execute(self, node_dict: Dict[IdentityHash, "ExecNode"]) -> Optional[Dict[str, Any]]:
        """
        Execute the ExecNode directly or according to an execution graph.
        Args:
            node_dict (Dict[IdentityHash, ExecNode]): A shared dictionary containing the other ExecNodes in the DAG;
                                                the key is the id of the ExecNode.

        Returns: the result of the execution of the current ExecNode
        """
        logger.debug(f"Start executing {self.id} with task {self.exec_function}")

        if self.executed:
            logger.debug(f"Skipping execution of a pre-computed node {self.id}")
            return self.result
        # 1. prepare args and kwargs for usage:
        args = [node_dict[node.id].result for node in self.args]
        kwargs = {key: node_dict[node.id].result for key, node in self.kwargs.items()}
        # args = [arg.result for arg in self.args]
        # kwargs = {key: arg.result for key, arg in self.kwargs.items()}

        # 2. write the result
        self.result = self.exec_function(*args, **kwargs)
        self.executed = True

        # 3. useless return value
        logger.debug(f"Finished executing {self.id} with task {self.exec_function}")
        return self.result


class PreComputedExecNode(ExecNode):
    # TODO: documentation!!!
    def __init__(self, argument_name: str, func: Callable[..., Any], value: Any):
        """_summary_

        Args:
            argument_name (str): the name of the argument passed
            func (Callable[..., Any]): the function which contains the default argument
            value (Any): _description_
        """
        super().__init__(id_=f"{func.__qualname__} >>> {argument_name}", is_sequential=False)

        self.result = value
        self.executed = True


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

        self.args = []
        self.kwargs = {}
        for i, arg in enumerate(args):
            if not isinstance(arg, ExecNode):
                # NOTE: maybe use the name of the argument instead ?
                arg = PreComputedExecNode(f"{ordinal(i)} argument", self.exec_function, arg)
                # Create a new ExecNode
                exec_nodes.append(arg)

            self.args.append(arg)

        for arg_name, arg in kwargs.items():
            # encapsulate the argument in PreComputedExecNode
            if not isinstance(arg, ExecNode):
                arg = PreComputedExecNode(arg_name, self.exec_function, arg)
                # Create a new ExecNode
                exec_nodes.append(arg)
            self.kwargs[arg_name] = arg

        # in case the same function is called twice, it is appended twice!
        # but this won't work correctly because we use the id of the function which is unique!
        # TODO: fix it using an additional random number at the end or the memory address of self!
        if self.id in [exec_node.id for exec_node in exec_nodes]:
            raise UnvalidExecNodeCall(
                "Invoking the same function twice is not allowed in the same DAG"
            )

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
