from copy import copy
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger
from pydantic import BaseModel, Extra, root_validator, validator

from tawazi.consts import (
    ARG_NAME_SEP,
    ARG_NAME_TAG,
    RESERVED_KWARGS,
    USE_SEP_START,
    IdentityHash,
    NoVal,
    NoValType,
    Tag,
)
from tawazi.profile import Profile

from .config import Cfg
from .errors import InvalidExecNodeCall, TawaziBaseException
from .helpers import lazy_xn_id, make_raise_arg_error, ordinal

# TODO: replace exec_nodes with dict
# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: List["ExecNode"] = []
exec_nodes_lock = Lock()

Alias = Union[Tag, IdentityHash, "ExecNode"]  # multiple ways of identifying an XN


class ExecNode(
    BaseModel,
    arbitrary_types_allowed=True,
    validate_assignment=True,
    smart_union=True,
    copy_on_model_validation="none",
):
    """
    This class is the base executable node of the Directed Acyclic Execution Graph.
    An ExecNode is an Object that can be executed inside a DAG scheduler.
    It's a function (exec_function) that takes *args and **kwargs and return a Value.
    When the ExecNode is executed in the DAG, the resulting value will be stored in the result attribute

    id (IdentityHash): identifier of ExecNode.
    exec_function (Callable): a callable will be executed in the graph.
        This is useful to make Joining ExecNodes (Nodes that enforce dependencies on the graph)
    args (Optional[List[ExecNode]], optional): *args to pass to exec_function.
    kwargs (Optional[Dict[str, ExecNode]], optional): **kwargs to pass to exec_function.
    priority (int): priority compared to other ExecNodes; the higher the number the higher the priority.
    is_sequential (bool): whether to execute this ExecNode in sequential order with respect to others.
        When this ExecNode must be executed, all other nodes are waited to finish before starting execution.
        Defaults to False.
    debug (bool): Make this ExecNode a debug Node. Defaults to False.
    tag (Tag): Attach a Tag of this ExecNode. Defaults to None.
    setup (bool): Make this ExecNode a setup Node. Defaults to False.
    """

    # TODO: change the way that id is implemented in subclasses since it makes pydantic yell
    #  Consider directly setting id in the code rather than indirecting through other args
    id: IdentityHash = None  # type: ignore
    exec_function: Callable[..., Any] = lambda *args, **kwargs: None
    priority: int = 0
    is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL
    debug: bool = False
    tag: Optional[Tag] = None
    setup: bool = False
    args: List["ExecNode"] = []
    kwargs: Dict[IdentityHash, "ExecNode"] = {}

    # compound_priority equals priority at the start but will be modified during the build process
    compound_priority: int = None  # type: ignore

    # Assign a default NoVal to the result of the execution of this ExecNode,
    # when this ExecNode will be executed, self.result will be overridden
    # It would be amazing if we can remove self.result and make ExecNode immutable
    # even though setting result to NoVal is not necessary... it clarifies debugging
    result: Union[NoValType, Any] = NoVal

    @root_validator
    def check_debug_and_setup(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values["debug"] and values["setup"]:
            raise ValueError(
                f"The node {values['id']} can't be a setup and a debug node at the same time."
            )
        return values

    @validator("tag", pre=True)
    def check_tag(cls, tag: Tag) -> Tag:
        if not isinstance(tag, (str, tuple)) and tag is not None:
            raise TypeError(f"tag should be of type {Tag} but {tag} provided")
        return tag

    @validator("priority", pre=True)
    def check_priority(cls, prio: int) -> int:
        if not isinstance(prio, int):
            raise ValueError(f"priority must be an int, provided {type(prio)}")
        return prio

    @validator("compound_priority", always=True, pre=True)
    def set_default_compound_prio(cls, c_prio: int, values: Dict[str, Any]) -> int:
        # at initialization, defaults to priority
        if c_prio is None:
            c_prio = values["priority"]
        return c_prio

    @property
    def _name(self) -> str:
        return self.exec_function.__name__ if not isinstance(self.id, str) else self.id

        self.profile = Profile(Cfg.TAWAZI_PROFILE_ALL_NODES)

    @property
    def executed(self) -> bool:
        return self.result is not NoVal

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

        Returns:
            the result of the execution of the current ExecNode
        """
        logger.debug(f"Start executing {self.id} with task {self.exec_function}")
        self.profile = Profile(Cfg.TAWAZI_PROFILE_ALL_NODES)

        if self.executed:
            logger.debug(f"Skipping execution of a pre-computed node {self.id}")
            return self.result

        # 1. pre-
        # 1.1 prepare the profiling
        with self.profile:
            # 1.2 prepare args and kwargs for usage:
            args = [node_dict[node.id].result for node in self.args]
            kwargs = {key: node_dict[node.id].result for key, node in self.kwargs.items()}
            # args = [arg.result for arg in self.args]
            # kwargs = {key: arg.result for key, arg in self.kwargs.items()}

            # 2 post-
            # 2.1 write the result
            self.result = self.exec_function(*args, **kwargs)

        # 3. useless return value
        logger.debug(f"Finished executing {self.id} with task {self.exec_function}")
        return self.result

    def _assign_reserved_args(self, arg_name: str, value: Tag) -> bool:
        # TODO: change value type to Union[Tag, Setup etc...] when other special attributes are introduced
        if arg_name == ARG_NAME_TAG:
            self.tag = value
            return True

        return False


class ArgExecNode(
    ExecNode,
    arbitrary_types_allowed=True,
    validate_assignment=True,
    smart_union=True,
    copy_on_model_validation="none",
):
    """
    ExecNode corresponding to an Argument.
    Every Argument is Attached to a Function or an ExecNode (especially a LazyExecNode)
    If a value is not passed to the function call / ExecNode,
    it will raise an error similar to Python's Error

    xn_or_func_or_id: The ExecNode or function that this Argument is attached to
    name_or_order: Argument name or order in the calling function.
    For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
        1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
        2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6] will be of type ArgExecNode with a name="iterable"
    value: The preassigned value to the corresponding Argument.
    """

    xn_or_func_or_id: Union[ExecNode, Callable[..., Any], IdentityHash]
    name_or_order: Union[str, int]
    value: Any = NoVal

    @validator("xn_or_func_or_id")
    def build_base_id(
        cls, identifier: Union[ExecNode, Callable[..., Any], IdentityHash]
    ) -> Union[ExecNode, Callable[..., Any], IdentityHash]:
        if isinstance(identifier, ExecNode):
            base_id = identifier.id
        elif callable(identifier):
            base_id = identifier.__qualname__
        elif isinstance(identifier, IdentityHash):
            base_id = identifier
        else:
            raise TypeError("ArgExecNode can only be attached to a LazyExecNode or a Callable")

        return base_id

    @validator("name_or_order")
    def build_suffix(cls, name_or_order: Union[str, int]) -> Union[str, int]:
        if isinstance(name_or_order, str):
            suffix = name_or_order
        elif isinstance(name_or_order, int):
            suffix = f"{ordinal(name_or_order)} argument"
        else:
            raise TypeError(
                f"ArgExecNode needs the argument name (str) or order of call (int), "
                f"but {name_or_order} of type {type(name_or_order)} is provided"
            )

        return suffix

    @validator("value")
    def fill_value(cls, value: Any, values: Dict[str, Any]) -> Any:
        if value is not NoVal:
            values["result"] = value

        return value

    @root_validator
    def build_id(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["id"] = f"{values['xn_or_func_or_id']}{ARG_NAME_SEP}{values['name_or_order']}"
        values["exec_function"] = make_raise_arg_error(
            values["xn_or_func_or_id"], values["name_or_order"]
        )
        values["is_sequential"] = False

        return values


# NOTE: how can we make a LazyExecNode more configurable ?
#  This might not be as important as it seems actually because
#  one can simply create Partial Functions and wrap them in an ExecNode
# TODO: give ExecNode the possibility to expand its result
#  this means that it will return its values as Tuple[LazyExecNode] or Dict[LazyExecNode]
#  Hence ExecNode can return multiple values!
# TODO: create a twz_deps reserved variable to support Nothing dependency
class LazyExecNode(
    ExecNode,
    arbitrary_types_allowed=True,
    validate_assignment=True,
    smart_union=True,
    extra=Extra.allow,
    copy_on_model_validation="none",
):
    """
    A lazy function simulator.
    The __call__ behavior of the original function is overridden to record the dependencies to build the DAG.
    The original function is kept to be called during the scheduling phase when calling the DAG.
    """

    @validator("exec_function")
    def extract_id_from_func(
        cls, func: Callable[..., Any], values: Dict[str, Any]
    ) -> Callable[..., Any]:
        values["id"] = func.__qualname__
        return func

    def __call__(self, *args: Any, **kwargs: Any) -> "LazyExecNode":
        """
        Record the dependencies in a global variable to be called later in DAG.

        Args:
            *args (Any): positional arguments passed to the function during dependency recording
            **kwargs (Any): keyword arguments passed to the function during dependency recording

        Returns:
            a copy of the LazyExecNode. This copy corresponds to a call to the original function.

        Raises:
            InvalidExecNodeCall: if this ExecNode is called outside of a DAG dependency calculation
            TawaziBaseException: if the debug and setup dependencies constraints are violated:
                1. normal ExecNode depends on debug ExecNode
                2. setup ExecNode depends on normal ExecNode
        """
        # 0.1 LazyExecNodes cannot be called outside DAG dependency calculation
        #  (i.e. outside a function that is decorated with @dag)
        if not exec_nodes_lock.locked():
            raise InvalidExecNodeCall(
                "Invoking ExecNode __call__ is only allowed inside a @dag decorated function"
            )

        # # 0.2 if self is a debug ExecNode and Tawazi is configured to skip running debug Nodes
        # #   then skip registering this node in the list of ExecNodes to be executed

        # TODO: maybe change the Type of objects created.
        #  for example: have a LazyExecNode.__call(...) return an ExecNodeCall instead of a copy
        # 1.1 Make a deep copy of self because every Call to an ExecNode corresponds to a new instance
        self_copy = copy(self)
        # 1.2 Assign the id
        count_usages = sum(ex_n.id.split(USE_SEP_START)[0] == self.id for ex_n in exec_nodes)
        # if ExecNode is used multiple times, <<usage_count>> is appended to its ID
        self_copy.id = lazy_xn_id(self.id, count_usages)

        # 2. Make the corresponding ExecNodes that corresponds to the Arguments
        # Make new objects because these should be different between different XN_calls
        self_copy.args = []
        self_copy.kwargs = {}

        # 2.1 *args can contain either:
        #  1. ExecNodes corresponding to the dependencies that come from predecessors
        #  2. or non ExecNode values which are constants passed directly to the LazyExecNode.__call__ (eg. strings, int, etc.)
        for i, arg in enumerate(args):
            if not isinstance(arg, ExecNode):
                # arg here is definitely not a return value of a LazyExecNode!
                # it must be a default value
                arg = ArgExecNode(xn_or_func_or_id=self_copy, name_or_order=i, value=arg)
                exec_nodes.append(arg)

            self_copy.args.append(arg)

        # 2.2 support **kwargs
        for kwarg_name, kwarg in kwargs.items():
            # support reserved kwargs for tawazi
            # These are necessary in order to pass information about the call of an ExecNode (the deep copy)
            # independently of the original LazyExecNode
            if kwarg_name in RESERVED_KWARGS:
                self_copy._assign_reserved_args(kwarg_name, kwarg)
                continue
            if not isinstance(kwarg, ExecNode):
                # passed in constants
                kwarg = ArgExecNode(
                    xn_or_func_or_id=self_copy, name_or_order=kwarg_name, value=kwarg
                )
                exec_nodes.append(kwarg)

            self_copy.kwargs[kwarg_name] = kwarg

        for dep in self_copy.dependencies:
            # if ExecNode is not a debug node, all its dependencies must not be debug node
            if not self_copy.debug and dep.debug:
                raise TawaziBaseException(f"Non debug node {self_copy} depends on debug node {dep}")

            # if ExecNode is a setup node, all its dependencies should be either:
            # 1. setup nodes
            # 2. Constants (ArgExecNode)
            # 3. Arguments passed directly to the PipeLine (ArgExecNode)
            accepted_case = dep.setup or isinstance(dep, ArgExecNode)
            if self_copy.setup and not accepted_case:
                raise TawaziBaseException(f"setup node {self_copy} depends on non setup node {dep}")

        exec_nodes.append(self_copy)
        return self_copy

    def __get__(self, instance: "LazyExecNode", owner_cls: Optional[Any] = None) -> Any:
        """
        Simulate func_descr_get() in Objects/funcobject.c

        Args:
            instance (LazyExecNode): the instance that this attribute should be attached to
            owner_cls: Discriminate between attaching the attribute to the instance of the class and the class itself

        Returns:
            Either self or a MethodType object
        """
        # if LazyExecNode is not an attribute of a class, then return self
        if instance is None:
            # this is the case when we call the method on the class instead of an instance of the class
            # In this case, we must return a "function" hence an instance of this class
            # https://stackoverflow.com/questions/3798835/understanding-get-and-set-and-python-descriptors
            return self
        return MethodType(self, instance)  # func=self  # obj=instance
