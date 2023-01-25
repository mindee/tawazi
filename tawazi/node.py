from copy import deepcopy
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger

from tawazi.errors import TawaziBaseException, UnvalidExecNodeCall, raise_arg_exc
from tawazi.helpers import ordinal

from .config import Cfg

# TODO: replace exec_nodes with dict
# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: List["ExecNode"] = []
exec_nodes_lock = Lock()
IdentityHash = str
Tag = Union[str, tuple]  # anything immutable

ARG_NAME_TAG = "twz_tag"

RESERVED_KWARGS = [ARG_NAME_TAG]
ARG_NAME_SEP = ">>>"


class NoVal:
    ...


class ExecNode:
    """
    This class is the base executable node of the Directed Acyclic Execution Graph
    """

    def __init__(
        self,
        id_: IdentityHash,
        exec_function: Callable[..., Any] = lambda *args, **kwargs: None,
        args: Optional[List["ExecNode"]] = None,
        kwargs: Optional[Dict[str, "ExecNode"]] = None,
        priority: int = 0,
        is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
        debug: bool = False,
        tag: Optional[Any] = None,
        setup: bool = False,
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
        # TODO: validate attributes using pydantic perhaps!
        # assign attributes
        self.id = id_
        self.exec_function = exec_function
        self.priority = priority
        self.is_sequential = is_sequential
        self.debug = debug  # TODO: do the fix to run debug nodes if their inputs exist
        self.tag = tag
        self.setup = setup

        self.args: List[ExecNode] = args or []
        self.kwargs: Dict[IdentityHash, ExecNode] = kwargs or {}

        # assign the base of compound priority
        self.compound_priority = priority

        # a string that identifies the ExecNode.
        # It is either the name of the identifying function or the identifying string id_
        self.__name__ = self.exec_function.__name__ if not isinstance(id_, str) else id_

        # todo: remove and make ExecNode immutable ?
        # TODO: make a subclass of None called NoValueSet
        self.result: Optional[Dict[str, Any]] = None
        # TODO: use the PreComputedExecNode instead ?
        self.executed = False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} {self.id} ~ | <{hex(id(self))}>"

    # TODO: make cached_property ?
    @property
    def dependencies(self) -> List["ExecNode"]:
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

    def assign_attr(self, arg_name: str, value: Tag) -> None:
        # TODO: make a tag setter
        if arg_name == ARG_NAME_TAG:
            if not isinstance(value, (str, tuple)):
                raise TypeError(f"tag should be of type {Tag} but {value} provided")
            self.tag = value


class ArgExecNode(ExecNode):
    def __init__(
        self,
        xn_or_func: Union[ExecNode, Callable[..., Any]],
        name_or_order: Union[str, int],
        value: Any = NoVal,
    ):
        """
        ExecNode corresponding to an Argument.
        Every Argument is Attached to a Function or an ExecNode (especially a LazyExecNode)
        If a value is not passed to the function call / ExecNode,
        it will raise an error similar to Python's Error

        Args:
            xn_or_func (Union[ExecNode, Callable[..., Any]]): The ExecNode or function that this Argument is rattached to
            name_or_order (Union[str, int]): Argument name or order in the calling function.
              For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6] will be of type ArgExecNode with a name="iterable"
            value (Any): The preassigned value to the corresponding Argument.

        Raises:
            TawaziArgumentException: if this argument is not provided during the Attached ExecNode usage
            TypeError: if type parameter is passed (Internal)
        """
        if isinstance(xn_or_func, ExecNode):
            base_id = xn_or_func.id
            func = xn_or_func.exec_function
        elif isinstance(xn_or_func, Callable):  # type: ignore
            base_id = xn_or_func.__qualname__
            func = xn_or_func
        else:
            raise TypeError("ArgExecNode can only be attached to a LazyExecNode or a Callable")

        if isinstance(name_or_order, str):
            suffix = name_or_order
        elif isinstance(name_or_order, int):
            suffix = f"{ordinal(name_or_order)} argument"
        else:
            raise TypeError(
                f"ArgExecNode needs the argument name (str) or order of call (int), "
                f"but {name_or_order} of type {type(name_or_order)} is provided"
            )

        id_ = f"{base_id}{ARG_NAME_SEP}{suffix}"

        # declare a local function that will raise an error in the scheduler if
        # the user doesn't pass in This ArgExecNode as argument to the Attached LazyExecNode
        def raise_err() -> None:
            raise_arg_exc(func, suffix)

        super().__init__(id_=id_, exec_function=raise_err, is_sequential=False)

        if value is not NoVal:
            self.result = value
            self.executed = True


class LazyExecNode(ExecNode):
    """
    A lazy function simulator that records the dependencies to build the DAG
    """

    def __init__(
        self,
        func: Callable[..., Any],
        priority: int,
        is_sequential: bool,
        debug: bool,
        tag: Any,
        setup: bool,
    ):
        # TODO: make the parameters non default everywhere but inside the @op decorator
        # TODO: change the id_ of the execNode. Maybe remove it completely
        # NOTE: this means that a DAG must have a different named functions which is already True!
        super().__init__(
            id_=func.__qualname__,
            exec_function=func,
            priority=priority,
            is_sequential=is_sequential,
            debug=debug,
            tag=tag,
            setup=setup,
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

        # if self is a debug ExecNode and Tawazi is configured to skip running debug Nodes
        #   then skip registering this node in the list of ExecNodes to be executed
        if self.debug and not Cfg.RUN_DEBUG_NODES:
            # NOTE: is this the best idea ? what if
            return self

        # TODO: maybe change the Type of objects created.
        #  for example: have a LazyExecNode.__call(...) return an ExecNode instead of a deepcopy
        # "<" is a separator for the number of usage
        count_usages = sum(ex_n.id.split("<<")[0] == self.id for ex_n in exec_nodes)
        self_copy = deepcopy(self)
        if count_usages > 0:
            self_copy.id = f"{self.id}<<{count_usages}>>"

        # these assignements are not necessary! because self_copy is deeply copied
        self_copy.args = []
        self_copy.kwargs = {}
        # args can contain either ExecNodes or non ExecNode values
        #  like strings, constants etc.
        for i, arg in enumerate(args):
            if not isinstance(arg, ExecNode):
                # NOTE: maybe use the name of the argument instead ?
                # arg here is definetly not a return value of a LazyExecNode!
                # it must be a default value for example
                arg = ArgExecNode(self_copy, i, arg)
                # Create a new ExecNode
                exec_nodes.append(arg)

            self_copy.args.append(arg)

        for arg_name, arg in kwargs.items():
            if arg_name in RESERVED_KWARGS:
                self_copy.assign_attr(arg_name, arg)
                continue
            # encapsulate the argument in PreComputedExecNode
            if not isinstance(arg, ExecNode):
                arg = ArgExecNode(self_copy, arg_name, arg)
                # Create a new ExecNode
                exec_nodes.append(arg)
            self_copy.kwargs[arg_name] = arg

        # NOTE: duplicate code!, can be abstracted into a function !?
        # if ExecNode is not a debug node, all its dependencies must not be debug node
        if not self_copy.debug:
            for dep in self_copy.dependencies:
                if dep.debug:
                    raise TawaziBaseException(
                        f"Non debug node {self_copy} depends on debug node {dep}"
                    )

        # if ExecNode is a setup node, all its dependencies should be setup nodes or precalculated nodes Or Argument Nodes
        if self_copy.setup:
            for dep in self_copy.dependencies:
                if not dep.setup and not isinstance(dep, ArgExecNode):
                    raise TawaziBaseException(
                        f"Non setup node {self_copy} depends on setup node {dep}"
                    )

        # in case the same function is called twice, it is appended twice!
        # but this won't work correctly because we use the id of the function which is unique!
        # TODO: fix it using an additional random number at the end or the memory address of self!
        # if self.id in [exec_node.id for exec_node in exec_nodes]:
        #     raise UnvalidExecNodeCall(
        #         "Invoking the same function twice is not allowed in the same DAG"
        #     )

        exec_nodes.append(self_copy)
        return self_copy

    def __get__(self, instance: "LazyExecNode", owner_cls: Optional[Any] = None) -> Any:
        """
        Simulate func_descr_get() in Objects/funcobject.c
        Args:
            instance:
            owner_cls:

        Returns:

        """
        # if LazyExecNode is not an attribute of a class, then return self
        if instance is None:
            # this is the case when we call the method on the class instead of an instance of the class
            # In this case, we must return a "function" hence an instance of this class
            # https://stackoverflow.com/questions/3798835/understanding-get-and-set-and-python-descriptors
            return self
        return MethodType(self, instance)  # func=self  # obj=instance
