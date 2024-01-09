"""Module describing ExecNode Class and subclasses (The basic building Block of a DAG."""
import functools
import warnings
from copy import copy, deepcopy
from dataclasses import dataclass, field
from functools import partial, reduce
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, Generic, List, NoReturn, Optional, Tuple, Union

from loguru import logger

from tawazi._helpers import _filter_noval, _make_raise_arg_error
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
    NoVal,
    NoValType,
    P,
    Resource,
    Tag,
    TagOrTags,
    XNOutsideDAGCall,
)
from tawazi.errors import TawaziBaseException, TawaziUsageError
from tawazi.profile import Profile

from .helpers import _lazy_xn_id, _validate_tuple, make_suffix

# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: Dict[Identifier, "ExecNode"] = {}
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


@dataclass
class ExecNode:
    """This class is the base executable node of the Directed Acyclic Execution Graph.

    An ExecNode is an Object that can be executed inside a DAG scheduler.
    It basically consists of a function (exec_function) that takes args and kwargs and returns a value.
    When the ExecNode is executed in the DAG, the resulting value will be stored in the `result` instance attribute.
    Note: This class is not meant to be instantiated directly.
        Please use `@xn` decorator.


    Args:
        id_ (Identifier): identifier of ExecNode.
        exec_function (Callable): a callable will be executed in the graph.
        args (Optional[List[ExecNode]], optional): *args to pass to exec_function.
        kwargs (Optional[Dict[str, ExecNode]], optional): **kwargs to pass to exec_function.
        priority (int): priority compared to other ExecNodes; the higher the number the higher the priority.
        is_sequential (bool): whether to execute this ExecNode in sequential order with respect to others.
            When this ExecNode must be executed, all other nodes are waited to finish before starting execution.
            Defaults to False.
        debug (bool): Make this ExecNode a debug Node. Defaults to False.
        tag (TagOrTags): Attach a Tag or Tags to this ExecNode. Defaults to None.
        setup (bool): Make this ExecNode a setup Node. Defaults to False.
        unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding
            to the given value
        resource (str): the resource to use to execute this ExecNode. Defaults to "thread".

    Raises:
        ValueError: if setup and debug are both True.
    """

    # 1. assign attributes
    id_: Identifier = field(default=None)  # type: ignore[assignment]
    exec_function: Callable[..., Any] = field(default_factory=lambda: lambda *args, **kwargs: None)
    priority: int = 0
    is_sequential: bool = cfg.TAWAZI_IS_SEQUENTIAL
    debug: bool = False
    tag: Optional[TagOrTags] = None
    setup: bool = False
    unpack_to: Optional[int] = None
    resource: Resource = cfg.TAWAZI_DEFAULT_RESOURCE

    args: List["UsageExecNode"] = field(default_factory=list)  # args or []
    kwargs: Dict[Identifier, "UsageExecNode"] = field(default_factory=dict)  # kwargs or {}

    # TODO: fix _active behavior!
    _active: Union[bool, "UsageExecNode"] = field(init=False, default=True)

    # 4. Assign a default NoVal to the result of the execution of this ExecNode,
    #  when this ExecNode will be executed, self.result will be overridden
    # It would be amazing if we can remove self.result and make ExecNode immutable
    result: Union[NoValType, Any] = field(init=False, default=NoVal)
    """Internal attribute to store the result of the execution of this ExecNode (Might change!)."""
    # even though setting result to NoVal is not necessary... it clarifies debugging

    profile: Profile = field(default_factory=lambda: Profile(cfg.TAWAZI_PROFILE_ALL_NODES))

    def __post_init__(self) -> None:
        """Post init to validate attributes."""
        if isinstance(self.exec_function, partial):
            self.exec_function = functools.update_wrapper(
                self.exec_function, self.exec_function.func
            )

        # if id is not provided, the id is inferred from the exec_function
        if self.id_ is None:
            self.id_ = self.exec_function.__qualname__

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

    @property
    def executed(self) -> bool:
        """Whether this ExecNode has been executed."""
        return self.result is not NoVal

    def __repr__(self) -> str:
        """Human representation of the ExecNode.

        Returns:
            str: human representation of the ExecNode.
        """
        return f"{self.__class__.__name__} {self.id} ~ | <{hex(id(self))}>"

    @property
    def active(self) -> Union["UsageExecNode", bool]:
        """Whether this ExecNode is active or not."""
        # the value is set during the DAG description
        if isinstance(self._active, UsageExecNode):
            return self._active
        # the value set is a constant value
        return bool(self._active)

    @active.setter
    def active(self, value: Any) -> None:
        self._active = value

    @property
    def id(self) -> Identifier:
        """The identifier of this ExecNode."""
        return self.id_

    # TODO: make cached_property ?
    @property
    def dependencies(self) -> List["UsageExecNode"]:
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

    def execute(self, node_dict: Dict[Identifier, "ExecNode"]) -> Optional[Any]:
        """Execute the ExecNode inside of a DAG.

        Args:
            node_dict (Dict[Identifier, ExecNode]): A shared dictionary containing the other ExecNodes in the DAG;
                the key is the id of the ExecNode. This node_dict refers to the current execution

        Returns:
            the result of the execution of the current ExecNode
        """
        logger.debug("Start executing %s with task %s", self.id, self.exec_function)
        self.profile = Profile(cfg.TAWAZI_PROFILE_ALL_NODES)

        if self.executed:
            logger.debug("Skipping execution of a pre-computed node %s", self.id)
            return self.result

        # 1. prepare args and kwargs for usage:
        args = [xnw.result(node_dict) for xnw in self.args]
        kwargs = {
            key: xnw.result(node_dict)
            for key, xnw in self.kwargs.items()
            if key not in RESERVED_KWARGS
        }

        # 1. pre-
        # 1.1 prepare the profiling
        with self.profile:
            # 2 post-
            # 2.1 write the result
            self.result = self.exec_function(*args, **kwargs)

        # 3. useless return value
        logger.debug("Finished executing %s with task %s", self.id, self.exec_function)
        return self.result


class ReturnExecNode(ExecNode):
    """ExecNode corresponding to a constant Return value of a DAG."""

    def __init__(self, func: Callable[..., Any], name_or_order: Union[str, int], value: Any):
        """Constructor of ArgExecNode.

        Args:
            func (Callable[..., Any]): The function (DAG describer) that this return is reattached to
            name_or_order (Union[str, int]): key of the dict or order in the return.
                For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                    1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                    2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6]
                       will be an ArgExecNode with a name="iterable"
            value (Any): The preassigned value to the corresponding Return value.

        Raises:
            TypeError: if type parameter is passed (Internal)
        """
        suffix = make_suffix(name_or_order)
        super().__init__(id_=f"{func}{RETURN_NAME_SEP}{suffix}", is_sequential=False)
        self.result = value


class ArgExecNode(ExecNode):
    """ExecNode corresponding to an Argument.

    Every Argument is Attached to a Function or an ExecNode (especially a LazyExecNode)
    If a value is not passed to the function call / ExecNode,
    it will raise an error similar to Python's Error.
    """

    def __init__(
        self,
        xn_or_func_or_id: Union[ExecNode, Callable[..., Any], Identifier],
        name_or_order: Union[str, int],
        value: Any = NoVal,
    ):
        """Constructor of ArgExecNode.

        Args:
            xn_or_func_or_id (Union[ExecNode, Callable[..., Any], Identifier]): the corresponding execnode
            name_or_order (Union[str, int]): Argument name or order in the calling function.
                For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                    1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                    2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6] will be of
                       type ArgExecNode with a name="iterable"
            value (Any): The preassigned value to the corresponding Argument.

        Raises:
            TypeError: if type parameter is passed (Internal)
        """
        # raises TawaziArgumentException: if this argument is not provided during the Attached ExecNode usage

        if isinstance(xn_or_func_or_id, ExecNode):
            base_id = xn_or_func_or_id.id
        elif callable(xn_or_func_or_id):
            base_id = xn_or_func_or_id.__qualname__
        elif isinstance(xn_or_func_or_id, Identifier):
            base_id = xn_or_func_or_id
        else:
            raise TypeError("ArgExecNode can only be attached to a LazyExecNode or a Callable")

        suffix = make_suffix(name_or_order)

        raise_err = _make_raise_arg_error(base_id, suffix)

        super().__init__(
            id_=f"{base_id}{ARG_NAME_SEP}{suffix}", exec_function=raise_err, is_sequential=False
        )

        self.result = value


class LazyExecNode(ExecNode, Generic[P, RVXN]):
    """A lazy function simulator.

    The __call__ behavior of the original function is overridden to record the dependencies to build the DAG.
    The original function is kept to be called during the scheduling phase when calling the DAG.
    """

    # in reality it returns "UsageExecNode":
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

        # 1.1 Make a deep copy of self because every Call to an ExecNode corresponds to a new instance
        self_copy = copy(self)
        # 1.2 Assign the id
        # if ExecNode is used multiple times, <<usage_count>> is appended to its ID
        self_copy.id_ = _lazy_xn_id(self.id, count_occurrences(self.id, exec_nodes))

        # 2. Make the corresponding ArgExecNodes that corresponds to the Arguments
        self_copy.args = self_copy._make_args(*args)
        self_copy.kwargs = self_copy._make_kwargs(**kwargs)

        self_copy._validate_dependencies()

        exec_nodes[self_copy.id] = self_copy

        return self_copy._usage_exec_node()  # type: ignore[return-value]

    def _make_args(self, *args: P.args, **kwargs: P.kwargs) -> List["UsageExecNode"]:
        xn_args = []

        # 2.1 *args can contain either:
        #  1. ExecNodes corresponding to the dependencies that come from predecessors
        #  2. or non ExecNode values which are constants passed directly to the
        #  LazyExecNode.__call__ (eg. strings, int, etc.)
        for i, arg in enumerate(args):
            if not isinstance(arg, UsageExecNode):
                # arg here is definitely not a return value of a LazyExecNode!
                # it must be a default value
                xn = ArgExecNode(self, i, arg)
                exec_nodes[xn.id] = xn
                arg = UsageExecNode(xn.id)

            xn_args.append(arg)
        return xn_args

    def _make_kwargs(self, *args: P.args, **kwargs: P.kwargs) -> Dict[Identifier, "UsageExecNode"]:
        xn_kwargs = {}

        # 2.2 support **kwargs
        for kwarg_name, kwarg in kwargs.items():
            # support reserved kwargs for tawazi
            # These are necessary in order to pass information about the call of an ExecNode (the deep copy)
            #  independently of the original LazyExecNode
            if kwarg_name == ARG_NAME_TAG:
                self.tag = kwarg  # type: ignore[assignment]
                continue
            if kwarg_name == ARG_NAME_UNPACK_TO:
                self.unpack_to = kwarg  # type: ignore[assignment]
                continue

            if not isinstance(kwarg, UsageExecNode):
                # passed in constants
                xn = ArgExecNode(self, kwarg_name, kwarg)
                exec_nodes[xn.id] = xn
                kwarg = UsageExecNode(xn.id)

            # TODO: remove this line when fixing self.active
            if kwarg_name == ARG_NAME_ACTIVATE:
                self.active = kwarg

            xn_kwargs[kwarg_name] = kwarg
        return xn_kwargs

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

    def _usage_exec_node(self) -> Union[Tuple["UsageExecNode", ...], "UsageExecNode"]:
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


def _xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
    return LazyExecNode(
        exec_function=func,
        priority=0,
        is_sequential=cfg.TAWAZI_IS_SEQUENTIAL,
        debug=False,
        tag=None,
        setup=False,
        unpack_to=None,
        resource=cfg.TAWAZI_DEFAULT_RESOURCE,
    )


# boolean operators definitions
# "rich comparison" methods definitions
@_xn
def _lt(a: Any, b: Any) -> bool:
    return a < b  # type: ignore[no-any-return]


@_xn
def _le(a: Any, b: Any) -> bool:
    return a <= b  # type: ignore[no-any-return]


@_xn
def _eq(a: Any, b: Any) -> bool:
    return a == b  # type: ignore[no-any-return]


@_xn
def _ne(a: Any, b: Any) -> bool:
    return a != b  # type: ignore[no-any-return]


@_xn
def _gt(a: Any, b: Any) -> bool:
    return a > b  # type: ignore[no-any-return]


@_xn
def _ge(a: Any, b: Any) -> bool:
    return a >= b  # type: ignore[no-any-return]


# "numeric emulation" methods definitions
# https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types

# binary operations
# automatically implement:
# 1. right hand operations (allowing float + UsageExecNode[float])
# 2. augmented arithmetic (+=, -=, etc.)@_xn


@_xn
def _add(a: Any, b: Any) -> Any:
    return a + b


@_xn
def _sub(a: Any, b: Any) -> Any:
    return a - b


@_xn
def _mul(a: Any, b: Any) -> Any:
    return a * b


@_xn
def _matmul(a: Any, b: Any) -> Any:
    return a @ b


@_xn
def _truediv(a: Any, b: Any) -> Any:
    return a / b


@_xn
def _floordiv(a: Any, b: Any) -> Any:
    return a // b


@_xn
def _mod(a: Any, b: Any) -> Any:
    return a % b


@_xn
def _divmod(a: Any, b: Any) -> Any:
    return divmod(a, b)


@_xn
def _pow(a: Any, b: Any) -> Any:
    return pow(a, b)


@_xn
def _lshift(a: Any, b: Any) -> Any:
    return a << b


@_xn
def _rshift(a: Any, b: Any) -> Any:
    return a >> b


@_xn
def _and(a: Any, b: Any) -> Any:
    return a & b


@_xn
def _xor(a: Any, b: Any) -> Any:
    return a ^ b


@_xn
def _or(a: Any, b: Any) -> Any:
    return a | b


# unary operations
@_xn
def _neg(a: Any) -> Any:
    return -a


@_xn
def _pos(a: Any) -> Any:
    return +a


@_xn
def _abs(a: Any) -> Any:
    return abs(a)


@_xn
def _invert(a: Any) -> Any:
    return ~a


# built ins
# when a value is used for indexing of another
# can't be implemented because should return appropriate type
# object.__index__(self)

# can't be implemented because should return appropriate type
# __complex__
# __int__
# __float__

# __round__
# __trunc__
# __floor__
# __ceil__


# NOTE: None is hashable! In theory it can be used as a key in a dict!
KeyType = Union[str, int, Tuple[Any, ...], None, NoValType]


# TODO: transform this logic into the ExecNode itself ?
@dataclass(frozen=True)
class UsageExecNode:
    """The usage of the ExecNode / LazyExecNode inside the function describing the DAG.

    If ExecNode is not indexed with a key or an int, NoVal is used as the key.
    """

    id: Identifier
    key: List[KeyType] = field(default_factory=list)

    # TODO: make type of key immutable or something hashable, that way
    #  we can directly build a nx DAG with nodes instead of node ids
    #  removing almost all of the duplicate graph building logic
    # used in the dag dependency description
    def __getitem__(self, key: KeyType) -> "UsageExecNode":
        """Record the used key in a new UsageExecNode.

        Args:
            key (Union[str, int, Tuple[Any]]): the used key for indexing (whether int like Lists or strings like dicts)

        Returns:
            UsageExecNode: the new UsageExecNode where the key is recorded
        """
        # deepcopy self because UsageExecNode can be reused with different indexing
        new = deepcopy(self)
        new.key.append(key)
        return new

    @property
    def is_indexable(self) -> bool:
        """Whether UsageExecNode is used with an index.

        Returns:
            bool: whether the ExecNode is indexable
        """
        return bool(self.key)

    def result(self, xn_dict: Dict[Identifier, ExecNode]) -> Any:
        """Extract the result of the ExecNode corresponding to used key(s).

        Returns:
            Any: value inside the container
        """
        xn = xn_dict[self.id]
        # ignore typing error because it is the responsibility of the user to ensure the result of the XN is indexable!
        # Will raise the appropriate exception automatically
        #  The user might have specified a subgraph to run => xn contain NoVal
        #  or the user tried to access a non-indexable object
        # NOTE: maybe handle the 3 types of exceptions that might occur properly to help the user through debugging
        # if isinstance(xn.result, NoValType):
        #     raise TawaziTypeError(f"{xn} didn't run, hence its result is not indexable. Check your DAG's config")

        return _filter_noval(reduce(lambda obj, key: obj.__getitem__(key), self.key, xn.result))

    def __bool__(self) -> NoReturn:
        """__bool__ operator."""
        # __bool__ can not be faked because it must return True or False
        raise NotImplementedError

    def __contains__(self, other: Any) -> NoReturn:
        """___contains__ operator."""
        # __contains__ can not be faked because the return value gets automatically casted to bool
        raise NotImplementedError


def reflected(operator: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
    """Reflects an operator."""

    def inner_reflected(a: Any, b: Any) -> Any:
        return operator(b, a)

    return inner_reflected


# binary operations
setattr(UsageExecNode, "__lt__", _lt)  # noqa: B010
setattr(UsageExecNode, "__le__", _le)  # noqa: B010
setattr(UsageExecNode, "__eq__", _eq)  # noqa: B010
setattr(UsageExecNode, "__ne__", _ne)  # noqa: B010
setattr(UsageExecNode, "__gt__", _gt)  # noqa: B010
setattr(UsageExecNode, "__ge__", _ge)  # noqa: B010
setattr(UsageExecNode, "__add__", _add)  # noqa: B010
setattr(UsageExecNode, "__sub__", _sub)  # noqa: B010
setattr(UsageExecNode, "__mul__", _mul)  # noqa: B010
setattr(UsageExecNode, "__matmul__", _matmul)  # noqa: B010
setattr(UsageExecNode, "__truediv__", _truediv)  # noqa: B010
setattr(UsageExecNode, "__floordiv__", _floordiv)  # noqa: B010
setattr(UsageExecNode, "__mod__", _mod)  # noqa: B010
setattr(UsageExecNode, "__divmod__", _divmod)  # noqa: B010
setattr(UsageExecNode, "__pow__", _pow)  # noqa: B010
setattr(UsageExecNode, "__lshift__", _lshift)  # noqa: B010
setattr(UsageExecNode, "__rshift__", _rshift)  # noqa: B010
setattr(UsageExecNode, "__and__", _and)  # noqa: B010
setattr(UsageExecNode, "__xor__", _xor)  # noqa: B010
setattr(UsageExecNode, "__or__", _or)  # noqa: B010

# reflected binary operators
setattr(UsageExecNode, "__radd__", reflected(_add))  # noqa:B010
setattr(UsageExecNode, "__rsub__", reflected(_sub))  # noqa:B010
setattr(UsageExecNode, "__rmul__", reflected(_mul))  # noqa:B010
setattr(UsageExecNode, "__rmatmul__", reflected(_matmul))  # noqa:B010
setattr(UsageExecNode, "__rtruediv__", reflected(_truediv))  # noqa:B010
setattr(UsageExecNode, "__rfloordiv__", reflected(_floordiv))  # noqa:B010
setattr(UsageExecNode, "__rmod__", reflected(_mod))  # noqa:B010
setattr(UsageExecNode, "__rdivmod__", reflected(_divmod))  # noqa:B010
setattr(UsageExecNode, "__rpow__", reflected(_pow))  # noqa:B010
setattr(UsageExecNode, "__rlshift__", reflected(_lshift))  # noqa:B010
setattr(UsageExecNode, "__rrshift__", reflected(_rshift))  # noqa:B010
setattr(UsageExecNode, "__rand__", reflected(_and))  # noqa:B010
setattr(UsageExecNode, "__rxor__", reflected(_xor))  # noqa:B010
setattr(UsageExecNode, "__ror__", reflected(_or))  # noqa:B010

# unary operations
setattr(UsageExecNode, "__neg__", _neg)  # noqa: B010
setattr(UsageExecNode, "__pos__", _pos)  # noqa: B010
setattr(UsageExecNode, "__abs__", _abs)  # noqa: B010
setattr(UsageExecNode, "__invert__", _invert)  # noqa: B010
