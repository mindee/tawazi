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

from tawazi._helpers import _filter_noval, _lazy_xn_id, _make_raise_arg_error, ordinal
from tawazi.config import cfg
from tawazi.consts import (
    ARG_NAME_ACTIVATE,
    ARG_NAME_SEP,
    ARG_NAME_TAG,
    ARG_NAME_UNPACK_TO,
    RESERVED_KWARGS,
    RETURN_NAME_SEP,
    RVXN,
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

from .helpers import _validate_tuple

# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: Dict[Identifier, "ExecNode"] = {}
exec_nodes_lock = Lock()

Alias = Union[Tag, Identifier, "ExecNode"]  # multiple ways of identifying an XN


class ExecNode:
    """This class is the base executable node of the Directed Acyclic Execution Graph.

    An ExecNode is an Object that can be executed inside a DAG scheduler.

    It basically consists of a function (exec_function) that takes args and kwargs and returns a value.

    When the ExecNode is executed in the DAG, the resulting value will be stored in the ExecNode.result instance attribute.

    Note: This class is not meant to be instantiated directly.
        Please use `@xn` decorator.
    """

    def __init__(
        self,
        id_: Identifier,
        exec_function: Callable[..., Any] = lambda *args, **kwargs: None,
        args: Optional[List["UsageExecNode"]] = None,
        kwargs: Optional[Dict[str, "UsageExecNode"]] = None,
        priority: int = 0,
        is_sequential: bool = cfg.TAWAZI_IS_SEQUENTIAL,
        debug: bool = False,
        tag: Optional[TagOrTags] = None,
        setup: bool = False,
        unpack_to: Optional[int] = None,
        resource: Resource = cfg.TAWAZI_DEFAULT_RESOURCE,
    ):
        """Constructor of ExecNode.

        Args:
            id_ (Identifier): identifier of ExecNode.
            exec_function (Callable): a callable will be executed in the graph.
                This is useful to make Joining ExecNodes (Nodes that enforce dependencies on the graph)
            args (Optional[List[ExecNode]], optional): *args to pass to exec_function.
            kwargs (Optional[Dict[str, ExecNode]], optional): **kwargs to pass to exec_function.
            priority (int): priority compared to other ExecNodes; the higher the number the higher the priority.
            is_sequential (bool): whether to execute this ExecNode in sequential order with respect to others.
                When this ExecNode must be executed, all other nodes are waited to finish before starting execution.
                Defaults to False.
            debug (bool): Make this ExecNode a debug Node. Defaults to False.
            tag (TagOrTags): Attach a Tag or Tags to this ExecNode. Defaults to None.
            setup (bool): Make this ExecNode a setup Node. Defaults to False.
            unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding to the given value
            resource (str): the resource to use to execute this ExecNode. Defaults to "thread".

        Raises:
            ValueError: if setup and debug are both True.
        """
        # 1. assign attributes
        self._id = id_
        self.exec_function = exec_function
        self.priority = priority
        self.is_sequential = is_sequential
        self.debug = debug
        self.tag = tag
        self.setup = setup
        self.unpack_to = unpack_to
        self.active = True
        self.resource = resource

        self.args: List[UsageExecNode] = args or []
        self.kwargs: Dict[Identifier, UsageExecNode] = kwargs or {}

        # 2. compound_priority equals priority at the start but will be modified during the build process
        self.compound_priority = priority

        # 3. Assign the name
        # This can be used in the future but is not particularly useful at the moment
        self.__name__ = self.exec_function.__name__ if not isinstance(id_, str) else id_

        # 4. Assign a default NoVal to the result of the execution of this ExecNode,
        #  when this ExecNode will be executed, self.result will be overridden
        # It would be amazing if we can remove self.result and make ExecNode immutable
        self.result: Union[NoValType, Any] = NoVal
        """Internal attribute to store the result of the execution of this ExecNode (Might change!)."""
        # even though setting result to NoVal is not necessary... it clarifies debugging

        self.profile = Profile(cfg.TAWAZI_PROFILE_ALL_NODES)

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

    @property
    def id(self) -> Identifier:
        """The identifier of this ExecNode."""
        return self._id

    @property
    def tag(self) -> Optional[TagOrTags]:
        """The Tag or Tags of this ExecNode."""
        return self._tag

    @tag.setter
    def tag(self, value: Optional[TagOrTags]) -> None:
        is_none = value is None
        is_tag = isinstance(value, Tag)
        is_tuple_tag = isinstance(value, tuple) and all(isinstance(v, Tag) for v in value)
        if not (is_none or is_tag or is_tuple_tag):
            raise TypeError(
                f"tag should be of type {TagOrTags} but {value} of type {type(value)} is provided"
            )
        self._tag = value

    @property
    def priority(self) -> int:
        """The priority of this ExecNode."""
        return self._priority

    @priority.setter
    def priority(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError(f"priority must be an int, provided {type(value)}")
        self._priority = value

    @property
    def resource(self) -> Resource:
        """The resource used to run this ExecNode."""
        return self._resource

    @resource.setter
    def resource(self, value: Resource) -> None:
        if not isinstance(value, Resource):
            raise ValueError(f"resource must be of type {Resource}, provided {type(value)}")

        self._resource = value

    @property
    def is_sequential(self) -> bool:
        """Whether `ExecNode` runs in sequential order with respect to other `ExecNode`s."""
        return self._is_sequential

    @is_sequential.setter
    def is_sequential(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError(f"is_sequential should be of type bool, but {value} provided")
        self._is_sequential = value

    @property
    def debug(self) -> bool:
        """Whether this ExecNode is a debug Node. ExecNode can't be setup and debug simultaneously."""
        return self._debug

    @debug.setter
    def debug(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError(f"debug must be of type bool, but {value} provided")
        self._debug = value
        self._validate()

    @property
    def setup(self) -> bool:
        """Whether this ExecNode is a setup Node. ExecNode can't be setup and debug simultaneously."""
        return self._setup

    @setup.setter
    def setup(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError(f"setup must be of type bool, but {value} provided")
        self._setup = value
        self._validate()

    @property
    def active(self) -> Union["UsageExecNode", bool]:
        """Whether this ExecNode is active or not."""
        # the value is set during the DAG description
        if isinstance(self._active, UsageExecNode):
            return self._active
        # the valule set is a constant value
        return bool(self._active)

    @active.setter
    def active(self, value: Any) -> None:
        self._active = value

    def _execute(self, node_dict: Dict[Identifier, "ExecNode"]) -> Optional[Any]:
        """Execute the ExecNode inside of a DAG.

        Args:
            node_dict (Dict[Identifier, ExecNode]): A shared dictionary containing the other ExecNodes in the DAG;
                the key is the id of the ExecNode. This node_dict refers to the current execution

        Returns:
            the result of the execution of the current ExecNode
        """
        logger.debug(f"Start executing {self.id} with task {self.exec_function}")
        self.profile = Profile(cfg.TAWAZI_PROFILE_ALL_NODES)

        if self.executed:
            logger.debug(f"Skipping execution of a pre-computed node {self.id}")
            return self.result

        # 1. prepare args and kwargs for usage:
        args = [xnw.result(node_dict) for xnw in self.args]
        kwargs = {
            key: xnw.result(node_dict)
            for key, xnw in self.kwargs.items()
            if key not in RESERVED_KWARGS
        }
        # args = [arg.result for arg in self.args]
        # kwargs = {key: arg.result for key, arg in self.kwargs.items()}

        # 1. pre-
        # 1.1 prepare the profiling
        with self.profile:
            # 2 post-
            # 2.1 write the result
            self.result = self.exec_function(*args, **kwargs)

        # 3. useless return value
        logger.debug(f"Finished executing {self.id} with task {self.exec_function}")
        return self.result

    def _validate(self) -> None:
        if getattr(self, "debug", None) and getattr(self, "setup", None):
            raise ValueError(
                f"The node {self.id} can't be a setup and a debug node at the same time."
            )

    @property
    def unpack_to(self) -> Optional[int]:
        """The number of elements in the unpacked results of this ExecNode.

        Returns:
            Optional[int]: the number of elements in the unpacked results of this ExecNode.
        """
        return self._unpack_to

    @unpack_to.setter
    def unpack_to(self, value: Optional[int]) -> None:
        if value is not None:
            if not isinstance(value, int):
                raise ValueError(
                    f"unpack_to must be a positive int or None, provided {type(value)}"
                )
            # yes... empty tuples exist in Python
            if value < 0:
                raise ValueError(f"unpack_to must be a positive int or None, provided {value}")

            _validate_tuple(self.exec_function, value)

        self._unpack_to = value


class ReturnExecNode(ExecNode):
    """ExecNode corresponding to a constant Return value of a DAG."""

    def __init__(self, func: Callable[..., Any], name_or_order: Union[str, int], value: Any):
        """Constructor of ArgExecNode.

        Args:
            func (Callable[..., Any]): The function (DAG describer) that this return is reattached to
            name_or_order (Union[str, int]): key of the dict or order in the return.
                For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                    1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                    2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6] will be of type ArgExecNode with a name="iterable"
            value (Any): The preassigned value to the corresponding Return value.

        Raises:
            TypeError: if type parameter is passed (Internal)
        """
        if callable(func):
            base_id = func
        else:
            raise TypeError("ReturnExecNode can only be attached to a Callable")

        if isinstance(name_or_order, str):
            suffix = name_or_order
        elif isinstance(name_or_order, int):
            suffix = f"{ordinal(name_or_order)} argument"
        else:
            raise TypeError(
                f"ReturnExecNode needs the key (str) or order (int) of the returned value, "
                f"but {name_or_order} of type {type(name_or_order)} is provided"
            )

        id_ = f"{base_id}{RETURN_NAME_SEP}{suffix}"

        super().__init__(id_=id_, is_sequential=False)

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
            xn_or_func_or_id (Union[ExecNode, Callable[..., Any], Identifier]): The ExecNode or function that this Argument is rattached to
            name_or_order (Union[str, int]): Argument name or order in the calling function.
                For example Python's builtin sorted function takes 3 arguments (iterable, key, reverse).
                    1. If called like this: sorted([1,2,3]) then [1,2,3] will be of type ArgExecNode with an order=0
                    2. If called like this: sorted(iterable=[4,5,6]) then [4,5,6] will be of type ArgExecNode with a name="iterable"
            value (Any): The preassigned value to the corresponding Argument.

        Raises:
            TypeError: if type parameter is passed (Internal)
        """
        # raises TawaziArgumentException: if this argument is not provided during the Attached ExecNode usage

        # TODO: use pydantic!
        if isinstance(xn_or_func_or_id, ExecNode):
            base_id = xn_or_func_or_id.id
        elif callable(xn_or_func_or_id):
            base_id = xn_or_func_or_id.__qualname__
        elif isinstance(xn_or_func_or_id, Identifier):
            base_id = xn_or_func_or_id
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

        raise_err = _make_raise_arg_error(base_id, suffix)

        super().__init__(id_=id_, exec_function=raise_err, is_sequential=False)

        if value is not NoVal:
            self.result = value


# NOTE: how can we make a LazyExecNode more configurable ?
#  This might not be as important as it seems actually because
#  one can simply create Partial Functions and wrap them in an ExecNode
# TODO: create a twz_deps reserved variable to support Nothing dependency
class LazyExecNode(ExecNode, Generic[P, RVXN]):
    """A lazy function simulator.

    The __call__ behavior of the original function is overridden to record the dependencies to build the DAG.
    The original function is kept to be called during the scheduling phase when calling the DAG.
    """

    def __init__(
        self,
        func: Callable[P, RVXN],
        priority: int,
        is_sequential: bool,
        debug: bool,
        tag: Optional[TagOrTags],
        setup: bool,
        unpack_to: Optional[int],
        resource: Resource,
    ):
        """Constructor of LazyExecNode.

        Args:
            func (Callable[..., Any]): Look at ExecNode's Documentation
            priority (int): Look at ExecNode's Documentation
            is_sequential (bool): Look at ExecNode's Documentation
            debug (bool): Look at ExecNode's Documentation
            tag (Any): Look at ExecNode's Documentation
            setup (bool): Look at ExecNode's Documentation
            unpack_to (Optional[int]): Look at ExecNode's Documentation
            resource (Resource): Look at ExecNode's Documentation
        """
        if isinstance(func, partial):
            func = functools.update_wrapper(func, func.func)

        super().__init__(
            id_=func.__qualname__,
            exec_function=func,
            priority=priority,
            is_sequential=is_sequential,
            debug=debug,
            tag=tag,
            setup=setup,
            unpack_to=unpack_to,
            resource=resource,
        )

    # in reality it returns "XNWrapper":
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVXN:
        """Record the dependencies in a global variable to be called later in DAG.

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
        # 0.1 LazyExecNodes calls outside outside DAG dependency calculation is not recommended
        if not exec_nodes_lock.locked():
            msg = f"Invoking {self} outside of a `DAG`. Executing wrapped function instead of describing dependency."
            if cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.error:
                raise TawaziUsageError(msg)
            if cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.warning:
                warnings.warn(RuntimeWarning(msg), stacklevel=2)

            # else cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.ignore:
            return self.exec_function(*args, **kwargs)  # type: ignore[no-any-return]

        # # 0.2 if self is a debug ExecNode and Tawazi is configured to skip running debug Nodes
        # #   then skip registering this node in the list of ExecNodes to be executed

        # 1.1 Make a deep copy of self because every Call to an ExecNode corresponds to a new instance
        self_copy = copy(self)
        # 1.2 Assign the id
        count_usages = sum(xn_id.split(USE_SEP_START)[0] == self.id for xn_id in exec_nodes)
        # if ExecNode is used multiple times, <<usage_count>> is appended to its ID
        self_copy._id = _lazy_xn_id(self.id, count_usages)

        # 2. Make the corresponding ExecNodes that corresponds to the Arguments
        # Make new objects because these should be different between different XN_calls
        self_copy.args = []
        self_copy.kwargs = {}

        # 2.1 *args can contain either:
        #  1. ExecNodes corresponding to the dependencies that come from predecessors
        #  2. or non ExecNode values which are constants passed directly to the LazyExecNode.__call__ (eg. strings, int, etc.)
        for i, arg in enumerate(args):
            if not isinstance(arg, UsageExecNode):
                # arg here is definitely not a return value of a LazyExecNode!
                # it must be a default value
                xn = ArgExecNode(self_copy, i, arg)
                exec_nodes[xn.id] = xn
                arg = UsageExecNode(xn.id)

            self_copy.args.append(arg)

        # 2.2 support **kwargs
        for kwarg_name, kwarg in kwargs.items():
            # support reserved kwargs for tawazi
            # These are necessary in order to pass information about the call of an ExecNode (the deep copy)
            #  independently of the original LazyExecNode
            if kwarg_name == ARG_NAME_TAG:
                self_copy.tag = kwarg  # type: ignore[assignment]
                continue
            if kwarg_name == ARG_NAME_UNPACK_TO:
                self_copy.unpack_to = kwarg  # type: ignore[assignment]
                continue

            if not isinstance(kwarg, UsageExecNode):
                # passed in constants
                xn = ArgExecNode(self_copy, kwarg_name, kwarg)
                exec_nodes[xn.id] = xn
                kwarg = UsageExecNode(xn.id)

            if kwarg_name == ARG_NAME_ACTIVATE:
                self_copy.active = kwarg

            self_copy.kwargs[kwarg_name] = kwarg

        for dep in self_copy.dependencies:
            # if ExecNode is not a debug node, all its dependencies must not be debug node
            if not self_copy.debug and exec_nodes[dep.id].debug:
                raise TawaziBaseException(f"Non debug node {self_copy} depends on debug node {dep}")

            # if ExecNode is a setup node, all its dependencies should be either:
            # 1. setup nodes
            # 2. Constants (ArgExecNode)
            # 3. Arguments passed directly to the PipeLine (ArgExecNode)
            accepted_case = exec_nodes[dep.id].setup or isinstance(exec_nodes[dep.id], ArgExecNode)
            if self_copy.setup and not accepted_case:
                raise TawaziBaseException(f"setup node {self_copy} depends on non setup node {dep}")

        # exec_nodes contain a single copy of self!
        # but multiple XNWrapper instances hang arround in the dag.
        # However, they might relate to the same ExecNode
        exec_nodes[self_copy.id] = self_copy

        if self_copy.unpack_to is not None:
            uxn_tuple = tuple(
                UsageExecNode(self_copy.id, key=[i]) for i in range(self_copy.unpack_to)
            )
            return uxn_tuple  # type: ignore[return-value]
        return UsageExecNode(self_copy.id)  # type: ignore[return-value]

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


# NOTE: None is hashable! In theory it can be used as a key in a dict!
KeyType = Union[str, int, Tuple[Any, ...], None, NoValType]


# TODO: transform this logic into the ExecNode itself ?
@dataclass
class UsageExecNode:
    """The usage of the ExecNode / LazyExecNode inside the function describing the DAG.

    If ExecNode is not indexed with a key or an int, NoVal is used as the key.
    """

    id: Identifier
    key: List[KeyType] = field(default_factory=list)

    # TODO: make type of key immutable or something hashable
    # used in the dag dependency description
    def __getitem__(self, key: KeyType) -> "UsageExecNode":
        """Record the used key in a new UsageExecNode.

        Args:
            key (Union[str, int, Tuple[Any]]): the used key for indexing (whether int like Lists or strings like dicts)

        Returns:
            XNWrapper: the new UsageExecNode where the key is recorded
        """
        # deepcopy self because UsageExecNode can be reused with different indexing
        new_uxn = deepcopy(self)
        new_uxn.key.append(key)
        return new_uxn

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
        # ignore typing error because it is the responsibility of the user to insure the result contained in the XN is indexable!
        # Will raise the appropriate exception automatically
        #  The user might have specified a subgraph to run => xn contain NoVal
        #  or the user tried to access a non-indexable object
        # NOTE: maybe handle the 3 types of exceptions that might occur properly to help the user through debugging
        # if isinstance(xn.result, NoValType):
        #     raise TawaziTypeError(f"{xn} didn't run, hence its resulting value is not indexable. Check your DAG's configuration")

        return _filter_noval(reduce(lambda obj, key: obj.__getitem__(key), self.key, xn.result))

    def __lt__(self, other: Any) -> bool:
        """__lt__ operator."""
        return _uxn_lt(self, other)

    def __le__(self, other: Any) -> bool:
        """__le__ operator."""
        return _uxn_le(self, other)

    def __eq__(self, other: Any) -> bool:
        """__eq__ operator."""
        return _uxn_eq(self, other)

    def __ne__(self, other: Any) -> bool:
        """__ne__ operator."""
        return _uxn_ne(self, other)

    def __gt__(self, other: Any) -> bool:
        """__gt__ operator."""
        return _uxn_gt(self, other)

    def __ge__(self, other: Any) -> bool:
        """__ge__ operator."""
        return _uxn_ge(self, other)

    def __bool__(self) -> NoReturn:
        """__bool__ operator."""
        # __bool__ can not be faked because it must return True or False
        raise NotImplementedError

    def __contains__(self, other: Any) -> NoReturn:
        """___contains__ operator."""
        # __contains__ can not be faked because the return value gets automatically casted to bool
        raise NotImplementedError

    def __add__(self, other: Any) -> Any:
        """__add__ operator."""
        return _uxn_add(self, other)

    def __radd__(self, other: Any) -> Any:
        """__radd__ operator."""
        return _uxn_add(other, self)

    def __sub__(self, other: Any) -> Any:
        """__sub__ operator."""
        return _uxn_sub(self, other)

    def __rsub__(self, other: Any) -> Any:
        """__rsub__ operator."""
        return _uxn_sub(other, self)

    def __mul__(self, other: Any) -> Any:
        """__mul__ operator."""
        return _uxn_mul(self, other)

    def __rmul__(self, other: Any) -> Any:
        """__rmul__ operator."""
        return _uxn_mul(other, self)

    def __matmul__(self, other: Any) -> Any:
        """__matmul__ operator."""
        return _uxn_matmul(self, other)

    def __rmatmul__(self, other: Any) -> Any:
        """__rmatmul__ operator."""
        return _uxn_matmul(other, self)

    def __truediv__(self, other: Any) -> Any:
        """__truediv__ operator."""
        return _uxn_truediv(self, other)

    def __rtruediv__(self, other: Any) -> Any:
        """__rtruediv__ operator."""
        return _uxn_truediv(other, self)

    def __floordiv__(self, other: Any) -> Any:
        """__floordiv__ operator."""
        return _uxn_floordiv(self, other)

    def __rfloordiv__(self, other: Any) -> Any:
        """__rfloordiv__ operator."""
        return _uxn_floordiv(other, self)

    def __mod__(self, other: Any) -> Any:
        """__mod__ operator."""
        return _uxn_mod(self, other)

    def __rmod__(self, other: Any) -> Any:
        """__rmod__ operator."""
        return _uxn_mod(other, self)

    def __divmod__(self, other: Any) -> Any:
        """__divmod__ operator."""
        return _uxn_divmod(self, other)

    def __rdivmod__(self, other: Any) -> Any:
        """__rdivmod__ operator."""
        return _uxn_divmod(other, self)

    def __pow__(self, other: Any) -> Any:
        """__pow__ operator."""
        return _uxn_pow(self, other)

    def __rpow__(self, other: Any) -> Any:
        """__rpow__ operator."""
        return _uxn_pow(other, self)

    def __lshift__(self, other: Any) -> Any:
        """__lshift__ operator."""
        return _uxn_lshift(self, other)

    def __rlshift__(self, other: Any) -> Any:
        """__rlshift__ operator."""
        return _uxn_lshift(other, self)

    def __rshift__(self, other: Any) -> Any:
        """__rshift__ operator."""
        return _uxn_rshift(self, other)

    def __rrshift__(self, other: Any) -> Any:
        """__rrshift__ operator."""
        return _uxn_rshift(other, self)

    def __and__(self, other: Any) -> Any:
        """__and__ operator."""
        return _uxn_and(self, other)

    def __rand__(self, other: Any) -> Any:
        """__rand__ operator."""
        return _uxn_and(other, self)

    def __xor__(self, other: Any) -> Any:
        """__xor__ operator."""
        return _uxn_xor(self, other)

    def __rxor__(self, other: Any) -> Any:
        """__rxor__ operator."""
        return _uxn_xor(other, self)

    def __or__(self, other: Any) -> Any:
        """__or__ operator."""
        return _uxn_or(self, other)

    def __ror__(self, other: Any) -> Any:
        """__ror__ operator."""
        return _uxn_or(other, self)

    def __neg__(self) -> Any:
        """__neg__ operator."""
        return _uxn_neg(self)

    def __pos__(self) -> Any:
        """__pos__ operator."""
        return _uxn_pos(self)

    def __abs__(self) -> Any:
        """__abs__ operator."""
        return _uxn_abs(self)

    def __invert__(self) -> Any:
        """__invert__ operator."""
        return _uxn_invert(self)


def _xn(func: Callable[P, RVXN]) -> LazyExecNode[P, RVXN]:
    return LazyExecNode(
        func=func,
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
def _uxn_lt(a: Any, b: Any) -> bool:
    return a < b  # type: ignore[no-any-return]


@_xn
def _uxn_le(a: Any, b: Any) -> bool:
    return a <= b  # type: ignore[no-any-return]


@_xn
def _uxn_eq(a: Any, b: Any) -> bool:
    return a == b  # type: ignore[no-any-return]


@_xn
def _uxn_ne(a: Any, b: Any) -> bool:
    return a != b  # type: ignore[no-any-return]


@_xn
def _uxn_gt(a: Any, b: Any) -> bool:
    return a > b  # type: ignore[no-any-return]


@_xn
def _uxn_ge(a: Any, b: Any) -> bool:
    return a >= b  # type: ignore[no-any-return]


# "numeric emulation" methods definitions
# https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types

# binary operations
# automatically implement:
# 1. right hand operations (allowing float + UsageExecNode[float])
# 2. augmented arithmetic (+=, -=, etc.)@_xn


@_xn
def _uxn_add(a: Any, b: Any) -> Any:
    return a + b


@_xn
def _uxn_sub(a: Any, b: Any) -> Any:
    return a - b


@_xn
def _uxn_mul(a: Any, b: Any) -> Any:
    return a * b


@_xn
def _uxn_matmul(a: Any, b: Any) -> Any:
    return a @ b


@_xn
def _uxn_truediv(a: Any, b: Any) -> Any:
    return a / b


@_xn
def _uxn_floordiv(a: Any, b: Any) -> Any:
    return a // b


@_xn
def _uxn_mod(a: Any, b: Any) -> Any:
    return a % b


@_xn
def _uxn_divmod(a: Any, b: Any) -> Any:
    return divmod(a, b)


@_xn
def _uxn_pow(a: Any, b: Any) -> Any:
    return pow(a, b)


@_xn
def _uxn_lshift(a: Any, b: Any) -> Any:
    return a << b


@_xn
def _uxn_rshift(a: Any, b: Any) -> Any:
    return a >> b


@_xn
def _uxn_and(a: Any, b: Any) -> Any:
    return a & b


@_xn
def _uxn_xor(a: Any, b: Any) -> Any:
    return a ^ b


@_xn
def _uxn_or(a: Any, b: Any) -> Any:
    return a | b


# unary operations
@_xn
def _uxn_neg(a: Any) -> Any:
    return -a


@_xn
def _uxn_pos(a: Any) -> Any:
    return +a


@_xn
def _uxn_abs(a: Any) -> Any:
    return abs(a)


@_xn
def _uxn_invert(a: Any) -> Any:
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
