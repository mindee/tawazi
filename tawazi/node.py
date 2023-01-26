from copy import deepcopy
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger

from tawazi.consts import (
    ARG_NAME_SEP,
    ARG_NAME_TAG,
    RESERVED_KWARGS,
    USE_SEP_START,
    IdentityHash,
    Tag,
)
from tawazi.errors import InvalidExecNodeCall, TawaziBaseException, raise_arg_exc
from tawazi.helpers import lazy_xn_id, ordinal

from .config import Cfg

# TODO: replace exec_nodes with dict
# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: List["ExecNode"] = []
exec_nodes_lock = Lock()


class NoValType:
    """
    Tawazi's special None.
    This class is a singleton similar to None to determine that no value is assigned
    """

    _instance = None

    def __new__(cls):  # type: ignore
        if cls._instance is None:
            cls._instance

    @classmethod
    def __bool__(cls) -> bool:
        return False

    def __repr__(self) -> str:
        return "NoVal"

    def __eq__(self, __o: object) -> bool:
        return False


NoVal = NoValType()


class ExecNode:
    """
    This class is the base executable node of the Directed Acyclic Execution Graph.
    An ExecNode is an Object that can be executed inside a DAG scheduler.
    It basically consists of a function (exec_function) that takes *args and **kwargs and return a Value.
    When the ExecNode is executed in the DAG, the resulting value will be stored in the ExecNode.result instance attribute

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
        tag: Tag = None,
        setup: bool = False,
    ):
        """
        Constructor of ExecNode

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
        # NOTE: validate attributes using pydantic perhaps?
        #   But will be problematic when Pydantic 2 will be released... it will be best to wait for this Feature
        # 1. assign attributes
        self.id = id_
        self.exec_function = exec_function
        self.priority = priority
        self.is_sequential = is_sequential
        self.debug = debug  # TODO: do the fix to run debug nodes if their inputs exist
        self.tag = tag
        self.setup = setup

        self.args: List[ExecNode] = args or []
        self.kwargs: Dict[IdentityHash, ExecNode] = kwargs or {}

        # 2. compound_priority equals priority at the start but will be modified during the build process
        self.compound_priority = priority

        # 3. Assign the name
        # This can be used in the future but is not particularly useful at the moment
        self.__name__ = self.exec_function.__name__ if not isinstance(id_, str) else id_

        # 4. Assign a default NoVal to the result of the execution of this ExecNode,
        #  when this ExecNode will be executed, self.result will be overridden
        # It would be amazing if we can remove self.result and make ExecNode immutable
        self.result: Union[NoValType, Any] = NoVal
        # even though setting result to NoVal is not necessary... it clarifies debugging

        # self.executed can be removed...
        #  it can be a property that checks if self.result is not NoVal
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

    def execute(self, node_dict: Dict[IdentityHash, "ExecNode"]) -> Optional[Any]:
        """
        Execute the ExecNode inside of a DAG.
        Args:
            node_dict (Dict[IdentityHash, ExecNode]): A shared dictionary containing the other ExecNodes in the DAG;
              the key is the id of the ExecNode. This node_dict refers to the current execution

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

    def _assign_reserved_args(self, arg_name: str, value: Tag) -> bool:
        # TODO: change value type to Union[Tag, Setup etc...] when other special attributes are introduced
        if arg_name == ARG_NAME_TAG:
            self.tag = value
            return True

        return False

    @property
    def tag(self) -> Tag:
        return self._tag

    @tag.setter
    def tag(self, value: Tag) -> None:
        if not isinstance(value, (str, tuple)) and value is not None:
            raise TypeError(f"tag should be of type {Tag} but {value} provided")
        self._tag = value


class ArgExecNode(ExecNode):
    """
    ExecNode corresponding to an Argument.
    Every Argument is Attached to a Function or an ExecNode (especially a LazyExecNode)
    If a value is not passed to the function call / ExecNode,
    it will raise an error similar to Python's Error
    """

    def __init__(
        self,
        xn_or_func: Union[ExecNode, Callable[..., Any]],
        name_or_order: Union[str, int],
        value: Any = NoVal,
    ):
        """
        Constructor of ArgExecNode

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
    A lazy function simulator.
    The __call__ behavior of the original function is overridden to record the dependencies to build the DAG.
    The original function is kept to be called during the scheduling phase when calling the DAG.
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
        """
        Constructor of LazyExecNode

        Args:
            look at ExecNode's documentation.
        """

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
        Returns: a deepcopy of the LazyExecNode
        """
        # 0.1 LazyExecNodes cannot be called outside DAG dependency calculation
        #  (i.e. outside a function that is decorated with @to_dag)
        if not exec_nodes_lock.locked():
            raise InvalidExecNodeCall(
                "Invoking ExecNode __call__ is only allowed inside a @to_dag decorated function"
            )

        # 0.2 if self is a debug ExecNode and Tawazi is configured to skip running debug Nodes
        #   then skip registering this node in the list of ExecNodes to be executed
        if self.debug and not Cfg.RUN_DEBUG_NODES:
            # NOTE: is this the best idea ? what if I want to run a pipe with debug nodes then without debug nodes
            return self

        # TODO: maybe change the Type of objects created.
        #  for example: have a LazyExecNode.__call(...) return an ExecNodeCall instead of a deepcopy

        # 1. Assign the id
        count_usages = sum(ex_n.id.split(USE_SEP_START)[0] == self.id for ex_n in exec_nodes)
        self_copy = deepcopy(self)
        # if ExecNode is used multiple times, <<usage_count>> is appended to its ID
        self_copy.id = lazy_xn_id(self.id, count_usages)

        # 2. Make the corresponding ExecNodes that corresponds to the Arguments
        # NOTE: these assignments are unnecessary! because self_copy is deeply copied
        self_copy.args = []
        self_copy.kwargs = {}

        # 2.1 *args can contain either:
        #  1. ExecNodes corresponding to the dependencies that come from predecessors
        #  2. or non ExecNode values which are constants passed directly to the LazyExecNode.__call__ (eg. strings, int, etc.)
        for i, arg in enumerate(args):
            if not isinstance(arg, ExecNode):
                # arg here is definitely not a return value of a LazyExecNode!
                # it must be a default value
                arg = ArgExecNode(self_copy, i, arg)
                exec_nodes.append(arg)

            self_copy.args.append(arg)

        # 2.2 support **kwargs
        for arg_name, arg in kwargs.items():
            # support reserved kwargs for tawazi
            # These are necessary in order to pass information about the call of an ExecNode (the deep copy)
            #  independently of the original LazyExecNode
            if arg_name in RESERVED_KWARGS:
                self_copy._assign_reserved_args(arg_name, arg)
                continue
            if not isinstance(arg, ExecNode):
                # passed in constants
                arg = ArgExecNode(self_copy, arg_name, arg)
                exec_nodes.append(arg)

            self_copy.kwargs[arg_name] = arg

        # if ExecNode is not a debug node, all its dependencies must not be debug node
        if not self_copy.debug:
            for dep in self_copy.dependencies:
                if dep.debug:
                    raise TawaziBaseException(
                        f"Non debug node {self_copy} depends on debug node {dep}"
                    )

        # if ExecNode is a setup node, all its dependencies should be either:
        # 1. setup nodes
        # 2. Constants (ArgExecNode)
        # 3. Arguments passed directly to the PipeLine (ArgExecNode)
        if self_copy.setup:
            for dep in self_copy.dependencies:
                accepted_case = dep.setup or isinstance(dep, ArgExecNode)
                if not accepted_case:
                    raise TawaziBaseException(
                        f"setup node {self_copy} depends on non setup node {dep}"
                    )

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
