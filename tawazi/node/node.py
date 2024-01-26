"""Module describing ExecNode Class and subclasses (The basic building Block of a DAG."""
import dataclasses
import functools
import warnings
from copy import deepcopy
from dataclasses import dataclass, field
from functools import partial
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, Union

from loguru import logger

from tawazi._helpers import make_raise_arg_error
from tawazi.config import cfg
from tawazi.consts import (
    ARG_NAME_ACTIVATE,
    ARG_NAME_SEP,
    ARG_NAME_TAG,
    ARG_NAME_UNPACK_TO,
    RESERVED_KWARGS,
    RETURN_NAME_SEP,
    RVXN,
    USE_SEP_END,
    USE_SEP_START,
    Identifier,
    P,
    Resource,
    Tag,
    TagOrTags,
    XNOutsideDAGCall,
)
from tawazi.errors import TawaziBaseException, TawaziUsageError
from tawazi.node.uxn import UsageExecNode
from tawazi.profile import Profile

from .helpers import _lazy_xn_id, _validate_tuple, make_suffix

# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: Dict[Identifier, "ExecNode"] = {}
# a temporary variable to hold default values concerning the DAG's description
results: Dict[Identifier, Any] = {}
actives: Dict[Identifier, Union[bool, UsageExecNode]] = {}
exec_nodes_lock = Lock()

# multiple ways of identifying an XN
Alias = Union[Tag, Identifier, "ExecNode"]


def count_occurrences(id_: str, exec_nodes: Dict[str, "ExecNode"]) -> int:
    """Count the number of occurrences of an id in exec_nodes.

    Avoids counting the ids of the arguments passed to previously called ExecNodes.
    example: id_ = "a"
    ExecNode a is called five times, hence we should have ids a, a<<1>>, a<<2>>, a<<3>>, a<<4>>
    ExecNode a is called with many arguments:
    we want to avoid counting "a>>>nth argument" and a<<1>>>>nth argument"

    Args:
        id_ (str): the id to count
        exec_nodes (Dict[str, ExecNode]): the dictionary of ExecNodes

    Returns:
        int: the number of occurrences of id_ in exec_nodes
    """
    # only choose the ids that are exactly exactly the same as the original id
    candidate_ids = (xn_id for xn_id in exec_nodes if xn_id.split(USE_SEP_START)[0] == id_)

    # count the number of ids that are exactly the same as the original id
    #  or that end with USE_SEP_END (which means they come from a reuse of the same ExecNode)
    return sum(xn_id == id_ or xn_id.endswith(USE_SEP_END) for xn_id in candidate_ids)


@dataclass(frozen=True)
class ExecNode:
    """Base class for executable node in a DAG.

    An ExecNode is an Object that can be executed inside a DAG scheduler.
    It basically consists of a function (exec_function) that takes args and kwargs and returns a value.
    When the ExecNode is executed in the DAG, the resulting value will be stored in a dictionary.
    Note: This class is not meant to be instantiated directly.
        Please use `@xn` decorator.

    Args:
        id_ (Identifier): Identifier.
        exec_function (Callable): callable to execute in the DAG.
        args (Optional[List[ExecNode]], optional): *args to pass to exec_function during execution.
        kwargs (Optional[Dict[str, ExecNode]], optional): **kwargs to pass to exec_function during execution.
        priority (int): priority compared to other ExecNodes; the higher the number the higher the priority.
        is_sequential (bool): whether to execute this ExecNode in sequential order with respect to others.
            (i.e. When this ExecNode must be executed, all other nodes are waited to finish before starting execution.)
            Defaults to False.
        debug (bool): Make this ExecNode a debug Node. Defaults to False.
        tag (TagOrTags): Attach a Tag or Tags to this ExecNode. Defaults to None.
        setup (bool): Make this ExecNode a setup Node. Defaults to False.
        unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding
            to the given value
        resource (str): The resource to use to execute this ExecNode. Defaults to "thread".

    Raises:
        ValueError: if setup and debug are both True.
    """

    id_: Identifier = field(default="")
    exec_function: Callable[..., Any] = field(default_factory=lambda: lambda *args, **kwargs: None)
    priority: int = 0
    is_sequential: bool = cfg.TAWAZI_IS_SEQUENTIAL
    debug: bool = False
    tag: Optional[TagOrTags] = None
    setup: bool = False
    unpack_to: Optional[int] = None
    resource: Resource = cfg.TAWAZI_DEFAULT_RESOURCE

    args: List[UsageExecNode] = field(default_factory=list)  # args or []
    kwargs: Dict[Identifier, UsageExecNode] = field(default_factory=dict)  # kwargs or {}

    def __post_init__(self) -> None:
        """Post init to validate attributes."""
        if isinstance(self.exec_function, partial):
            object.__setattr__(
                self,
                "exec_function",
                functools.update_wrapper(self.exec_function, self.exec_function.func),
            )

        # if id is not provided, the id is inferred from the exec_function
        if self.id_ == "":
            object.__setattr__(self, "id_", self.exec_function.__qualname__)

        # type verifications
        is_none = self.tag is None
        is_tag = isinstance(self.tag, Tag)
        is_tuple_tag = isinstance(self.tag, tuple) and all(isinstance(v, Tag) for v in self.tag)
        if not (is_none or is_tag or is_tuple_tag):
            raise TypeError(
                f"tag should be of type {TagOrTags} but {self.tag} of type {type(self.tag)} is provided"
            )

        if not isinstance(self.priority, int):
            raise ValueError(f"priority must be an int, provided {type(self.priority)}")

        if not isinstance(self.resource, Resource):
            raise ValueError(f"resource must be of type {Resource}, provided {type(self.resource)}")

        if any(not isinstance(arg, UsageExecNode) for arg in self.args):
            raise ValueError("args must be of type UsageExecNode")

        if any(not isinstance(arg, UsageExecNode) for arg in self.kwargs.values()):
            raise ValueError("kwargs must be of type UsageExecNode")

        # other validations
        if self.debug and self.setup:
            raise ValueError(
                f"The node {self.id} can't be a setup and a debug node at the same time."
            )

        if self.unpack_to is not None:
            if not isinstance(self.unpack_to, int):
                raise ValueError(
                    f"unpack_to must be a positive int or None, provided {type(self.unpack_to)}"
                )
            # empty tuple case
            if self.unpack_to < 0:
                raise ValueError(
                    f"unpack_to must be a positive int or None, provided {self.unpack_to}"
                )

            _validate_tuple(self.exec_function, self.unpack_to)

    def __repr__(self) -> str:
        """Human representation of the ExecNode.

        Returns:
            str: human representation of the ExecNode.
        """
        return f"{self.__class__.__name__} {self.id} ~ | <{hex(id(self))}>"

    def executed(self, results: Dict[Identifier, Any]) -> bool:
        """Returns whether this ExecNode was executed or not."""
        return self.id in results

    @property
    def id(self) -> Identifier:
        """The identifier of this ExecNode."""
        return self.id_

    @property
    def dependencies(self) -> List[UsageExecNode]:
        """The List of ExecNode dependencies of This ExecNode.

        Returns:
            List[UsageExecNode]: the List of ExecNode dependencies of This ExecNode.
        """
        # Making the dependencies
        # 1. from args
        deps = self.args.copy()
        # 2. and from kwargs
        deps.extend(self.kwargs.values())

        return deps

    def execute(self, results: Dict[Identifier, Any], profiles: Dict[Identifier, Profile]) -> Any:
        """Execute the ExecNode inside of a DAG.

        Args:
            results (Dict[Identifier, Any]): A shared dictionary containing the results of other ExecNodes in the DAG;
            profiles (Dict[Identifier, Profile]): A dictionary containing the profiles of the execution of all other ExecNodes in the DAG

        Returns:
            the result of the execution of the current ExecNode
        """
        logger.debug("Start executing {} with task {}", self.id, self.exec_function)
        profiles[self.id] = Profile(cfg.TAWAZI_PROFILE_ALL_NODES)

        if self.executed(results):
            logger.debug("Skipping execution of a pre-computed node {}", self.id)
            return results[self.id]

        # 1. prepare args and kwargs for usage:
        args = [uxn.result(results) for uxn in self.args]
        kwargs = {
            key: uxn.result(results)
            for key, uxn in self.kwargs.items()
            if key not in RESERVED_KWARGS
        }

        # 1. pre-
        # 1.1 prepare the profiling
        with profiles[self.id]:
            # 2 post-
            # 2.1 write the result
            results[self.id] = self.exec_function(*args, **kwargs)

        # 3. useless return value
        logger.debug("Finished executing {} with task {}", self.id, self.exec_function)
        return results[self.id]


class ReturnExecNode(ExecNode):
    """ExecNode corresponding to a constant Return value of a DAG."""

    def __init__(self, func: Callable[..., Any], name_or_order: Union[str, int]):
        """Constructor of ArgExecNode.

        Args:
            func (Callable[..., Any]): The function (DAG describer) that this return is reattached to
            name_or_order (Union[str, int]): key of the dict or order in the return.
                For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                    1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                    2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6]
                       will be an ArgExecNode with a name="iterable"

        Raises:
            TypeError: if type parameter is passed (Internal)
        """
        suffix = make_suffix(name_or_order)
        super().__init__(id_=f"{func}{RETURN_NAME_SEP}{suffix}", is_sequential=False)


class ArgExecNode(ExecNode):
    """ExecNode corresponding to an Argument.

    Every Argument is Attached to the DAG or a LazyExecNode
    NOTE: If a value is not passed to the function call / ExecNode
    and the argument doesn't have a default value
    it will raise an error similar to Python's Error.
    """

    def __init__(self, id: Identifier):
        """Constructor of ArgExecNode."""
        # prefix might contain ARG_NAME_SEP
        # eg. for example a DAG composed from another with an ArgExecNode input``
        *prefix, suffix = id.split(ARG_NAME_SEP)
        base_id = "".join(prefix)

        raise_err = make_raise_arg_error(base_id, suffix)

        super().__init__(id_=id, exec_function=raise_err, is_sequential=False)


def make_axn_id(
    name_or_order: Union[str, int],
    xn: Optional[ExecNode] = None,
    func: Optional[Callable[[Any], Any]] = None,
    id_: Optional[Identifier] = None,
) -> Identifier:
    """Makes ArgExecNode id.

    Args:
        name_or_order (Union[str, int]): name of the Argument in case of KWarg or order of the Argument in case of an arg
            For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6] will be of
                    type ArgExecNode with a name="iterable"
        xn (Optional[ExecNode], optional): ExecNode to which the corresponding ArgExecNode is attached. Defaults to None.
        func (Optional[Callable[[Any], Any]], optional): DAG to which the corresponding ArgExecNode is attached. Defaults to None.
        id_ (Optional[Identifier], optional): id of an ExecNode to which the corresopnding ArgExecNode is attached. Defaults to None.

    Raises:
        TypeError: if type parameter is passed (Internal)

    Returns:
        Identifier: Id of the ArgExecNode
    """
    if isinstance(xn, ExecNode):
        base_id = xn.id
    elif callable(func):
        base_id = func.__qualname__
    elif isinstance(id_, Identifier):
        base_id = id_
    else:
        raise TypeError("ArgExecNode can only be attached to a LazyExecNode or a Callable")

    suffix = make_suffix(name_or_order)
    return f"{base_id}{ARG_NAME_SEP}{suffix}"


class LazyExecNode(ExecNode, Generic[P, RVXN]):
    """A lazy function simulator.

    The __call__ behavior of the original function is overridden to record the dependencies to build the DAG.
    The original function is kept to be called during the scheduling phase when calling the DAG.
    """

    # in reality it returns UsageExecNode:
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVXN:
        """Record the dependencies in a global variable to be called later in DAG.

        Args:
            *args (P.args): positional arguments passed to the function during dependency recording
            **kwargs (P.kwargs): keyword arguments passed to the function during dependency recording

        Returns:
            A copy of the LazyExecNode. This copy corresponds to a call to the original function.

        Raises:
            InvalidExecNodeCall: if this ExecNode is called outside of a DAG dependency calculation
            TawaziBaseException: if the debug and setup dependencies constraints are violated:
                1. normal ExecNode depends on debug ExecNode
                2. setup ExecNode depends on normal ExecNode
        """
        # 0.1 LazyExecNodes calls outside outside DAG dependency calculation is not recommended
        if not exec_nodes_lock.locked():
            msg = f"Invoking {self} outside of a `DAG`. Executing wrapped function instead of describing dependency."
            if cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.error:
                raise TawaziUsageError(msg)
            if cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.warning:
                warnings.warn(RuntimeWarning(msg), stacklevel=2)

            # else cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.ignore:
            return self.exec_function(*args, **kwargs)  # type: ignore[no-any-return]

        # 1.1 if ExecNode is used multiple times, <<usage_count>> is appended to its ID
        id_ = _lazy_xn_id(self.id, count_occurrences(self.id, exec_nodes))
        # 1.1 Construct a new LazyExecNode corresponding to the current call
        values = dataclasses.asdict(self)
        # force deepcopying instead of the default behavior of asdict: recursively apply asdict to dataclasses!
        values["exec_function"] = deepcopy(self.exec_function)
        values["id_"] = id_

        # 2. Make the corresponding ArgExecNodes that corresponds to the Arguments
        values["args"] = make_args(id_, *args)
        values["kwargs"] = make_kwargs(id_, **kwargs)

        # 3. extract reserved arguments for current LazyExecNode call
        values["tag"] = kwargs.get(ARG_NAME_TAG) or self.tag
        values["unpack_to"] = kwargs.get(ARG_NAME_UNPACK_TO) or self.unpack_to

        new_lxn: LazyExecNode[P, RVXN] = LazyExecNode(**values)

        new_lxn._validate_dependencies()

        exec_nodes[new_lxn.id] = new_lxn

        return new_lxn._usage_exec_node  # type: ignore[return-value]

    def _validate_dependencies(self) -> None:
        for dep in self.dependencies:
            # if ExecNode is not a debug node, all its dependencies must not be debug node
            if not self.debug and exec_nodes[dep.id].debug:
                raise TawaziBaseException(f"Non debug node {self} depends on debug node {dep}")

            # if ExecNode is a setup node, all its dependencies should be either:
            # 1. setup nodes
            # 2. Constants (ArgExecNode)
            # 3. Arguments passed directly to the PipeLine (ArgExecNode)
            accepted_case = exec_nodes[dep.id].setup or isinstance(exec_nodes[dep.id], ArgExecNode)
            if self.setup and not accepted_case:
                raise TawaziBaseException(f"setup node {self} depends on non setup node {dep}")

    @property
    def _usage_exec_node(self) -> Union[Tuple[UsageExecNode, ...], UsageExecNode]:
        """Makes the corresponding UsageExecNode(s).

        Note:
        exec_nodes dict contains a single copy of self!
        but multiple UsageExecNode instances hang around in the dag.
        However, they might relate to the same ExecNode.
        """
        if self.unpack_to is None:
            return UsageExecNode(self.id)
        return tuple(UsageExecNode(self.id, key=[i]) for i in range(self.unpack_to))

    def __get__(self, instance: "LazyExecNode[P, RVXN]", owner_cls: Optional[Any] = None) -> Any:
        """Simulate func_descr_get() in Objects/funcobject.c.

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


def make_default_value_uxn(
    id_: Identifier, name_or_order: Union[str, int], default_value: Any
) -> UsageExecNode:
    """Make a default ArgExecNode and its corresponding UsageExecNode."""
    xn = ArgExecNode(make_axn_id(name_or_order=name_or_order, id_=id_))
    exec_nodes[xn.id] = xn
    results[xn.id] = default_value
    return UsageExecNode(xn.id)


def make_args(id_: Identifier, *args: P.args, **kwargs: P.kwargs) -> List[UsageExecNode]:
    """Constructs the positional arguments for an ExecNode."""
    xn_args = []

    # *args can contain either:
    #  1. UsageExecNode corresponding to the dependencies that come from predecessors
    #  2. or non ExecNode values which are constants passed directly to the
    #  LazyExecNode.__call__ (eg. strings, int, etc.)
    for i, arg in enumerate(args):
        if not isinstance(arg, UsageExecNode):
            # arg here is definitely not a return value of a LazyExecNode!
            # it must be a default value
            arg = make_default_value_uxn(id_, i, arg)

        xn_args.append(arg)
    return xn_args


def make_kwargs(
    id_: Identifier, *args: P.args, **kwargs: P.kwargs
) -> Dict[Identifier, UsageExecNode]:
    """Constructs the keyword arguments for an ExecNode."""
    xn_kwargs = {}
    # **kwargs contain either
    #  1. UsageExecNode corresponding to the dependencies that come from predecessors
    #  2. or non ExecNode values which are constants passed directly to the
    #  3. or Reserved Keyword Arguments for Tawazi. These are used to assign different values per LXN call
    for kwarg_name, kwarg in kwargs.items():
        if isinstance(kwarg, str) and kwarg in [ARG_NAME_TAG, ARG_NAME_UNPACK_TO]:
            continue
        if not isinstance(kwarg, UsageExecNode):
            # passed in constants
            kwarg = make_default_value_uxn(id_, kwarg_name, kwarg)

        # TODO: remove this line when fixing self.active
        if kwarg_name == ARG_NAME_ACTIVATE:
            actives[id_] = kwarg

        xn_kwargs[kwarg_name] = kwarg
    return xn_kwargs
