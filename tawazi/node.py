"""Module describing ExecNode Class and subclasses (The basic building Block of a DAG."""
from copy import copy
from dataclasses import dataclass
from threading import Lock
from types import MethodType
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, Union

from loguru import logger

from tawazi.consts import (
    ARG_NAME_SEP,
    ARG_NAME_TAG,
    RESERVED_KWARGS,
    RVXN,
    USE_SEP_START,
    Identifier,
    NoVal,
    NoValType,
    P,
    ReturnTypeErrString,
    Tag,
)
from tawazi.profile import Profile

from .config import Cfg
from .errors import InvalidExecNodeCall, TawaziBaseException, TawaziTypeError
from .helpers import _filter_noval, _lazy_xn_id, _make_raise_arg_error, ordinal

# a temporary variable used to pass in exec_nodes to the DAG during building
exec_nodes: Dict[Identifier, "ExecNode"] = {}
exec_nodes_lock = Lock()

Alias = Union[Tag, Identifier, "ExecNode"]  # multiple ways of identifying an XN


class ExecNode:
    """This class is the base executable node of the Directed Acyclic Execution Graph.

    An ExecNode is an Object that can be executed inside a DAG scheduler.
    It basically consists of a function (exec_function) that takes *args and **kwargs and return a Value.
    When the ExecNode is executed in the DAG, the resulting value will be stored in the ExecNode.result instance attribute.
    """

    def __init__(
        self,
        id_: Identifier,
        exec_function: Callable[..., Any] = lambda *args, **kwargs: None,
        args: Optional[List["UsageExecNode"]] = None,
        kwargs: Optional[Dict[str, "UsageExecNode"]] = None,
        priority: int = 0,
        is_sequential: bool = Cfg.TAWAZI_IS_SEQUENTIAL,
        debug: bool = False,
        tag: Tag = None,
        setup: bool = False,
        unpack_to: Optional[int] = None,
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
            tag (Tag): Attach a Tag of this ExecNode. Defaults to None.
            setup (bool): Make this ExecNode a setup Node. Defaults to False.
            unpack_to (Optional[int]): if not None, this ExecNode's execution must return unpacked results corresponding to the given value

        Raises:
            ValueError: if setup and debug are both True.
        """
        # NOTE: validate attributes using pydantic perhaps
        # 1. assign attributes
        self.id = id_
        self.exec_function = exec_function
        self.priority = priority
        self.is_sequential = is_sequential
        self.debug = debug  # TODO: do the fix to run debug nodes if their inputs exist
        self.tag = tag
        self.setup = setup
        self.unpack_to = unpack_to

        if debug and setup:
            raise ValueError(
                f"The node {self.id} can't be a setup and a debug node at the same time."
            )

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
        # even though setting result to NoVal is not necessary... it clarifies debugging

        self.profile = Profile(Cfg.TAWAZI_PROFILE_ALL_NODES)

    @property
    def executed(self) -> bool:
        """Whether this ExecNode has been executed.

        Returns:
            bool: whether this ExecNode has been executed.
        """
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
            List[ExecNode]: the List of ExecNode dependencies of This ExecNode.
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
        logger.debug(f"Start executing {self.id} with task {self.exec_function}")
        self.profile = Profile(Cfg.TAWAZI_PROFILE_ALL_NODES)

        if self.executed:
            logger.debug(f"Skipping execution of a pre-computed node {self.id}")
            return self.result

        # 1. prepare args and kwargs for usage:
        args = [xnw.result(node_dict) for xnw in self.args]
        kwargs = {key: xnw.result(node_dict) for key, xnw in self.kwargs.items()}
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

    def _assign_reserved_args(self, arg_name: str, value: Any) -> bool:
        # TODO: change value type to Union[Tag, Setup etc...] when other special attributes are introduced
        if arg_name == ARG_NAME_TAG:
            self.tag = value
            return True

        return False

    @property
    def tag(self) -> Tag:
        """The Tag of this ExecNode.

        Returns:
            Tag: the Tag of this ExecNode.
        """
        return self._tag

    @tag.setter
    def tag(self, value: Tag) -> None:
        if not isinstance(value, (str, tuple)) and value is not None:
            raise TypeError(f"tag should be of type {Tag} but {value} provided")
        self._tag = value

    @property
    def priority(self) -> int:
        """The priority of this ExecNode.

        Returns:
            int: the priority of this ExecNode.
        """
        return self._priority

    @priority.setter
    def priority(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError(f"priority must be an int, provided {type(value)}")
        self._priority = value

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

        # TODO: raise a warning if the typing of the ExecNode doesn't correspond with the number of elements in the unpacked results!
        # NOTE: the typing supports an arbitrary number of elements in the unpacked results! support this as well!
        self._unpack_to = value


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


# TODO: make the LazyExecNode call outside the dag a normal function call!
# NOTE: how can we make a LazyExecNode more configurable ?
#  This might not be as important as it seems actually because
#  one can simply create Partial Functions and wrap them in an ExecNode
# TODO: give ExecNode the possibility to expand its result
#  this means that it will return its values as Tuple[LazyExecNode] or Dict[LazyExecNode]
#  Hence ExecNode can return multiple values!
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
        tag: Any,
        setup: bool,
        unpack_to: Optional[int],
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
        """
        super().__init__(
            id_=func.__qualname__,
            exec_function=func,
            priority=priority,
            is_sequential=is_sequential,
            debug=debug,
            tag=tag,
            setup=setup,
            unpack_to=unpack_to,
        )

    def __call__(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> RVXN:  # in reality it returns "XNWrapper":
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
        count_usages = sum(xn_id.split(USE_SEP_START)[0] == self.id for xn_id in exec_nodes)
        # if ExecNode is used multiple times, <<usage_count>> is appended to its ID
        self_copy.id = _lazy_xn_id(self.id, count_usages)

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
            if kwarg_name in RESERVED_KWARGS:
                self_copy._assign_reserved_args(kwarg_name, kwarg)
                continue
            if not isinstance(kwarg, UsageExecNode):
                # passed in constants
                xn = ArgExecNode(self_copy, kwarg_name, kwarg)
                exec_nodes[xn.id] = xn
                kwarg = UsageExecNode(xn.id)

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

        if self.unpack_to is not None:
            uxn_tuple = tuple(UsageExecNode(self_copy.id, key=i) for i in range(self.unpack_to))
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
    key: KeyType = NoVal

    # TODO: make type of key immutable or something hashable
    # used in the dag dependency description
    def __getitem__(self, key: KeyType) -> "UsageExecNode":
        """Record the used key in a new UsageExecNode.

        Args:
            key (Union[str, int, Tuple[Any]]): the used key for indexing (whether int like Lists or strings like dicts)

        Returns:
            XNWrapper: the new UsageExecNode where the key is recorded
        """
        return UsageExecNode(self.id, key)

    @property
    def is_indexable(self) -> bool:
        """Whether UsageExecNode is used with an index.

        Returns:
            bool: whether the ExecNode is indexable
        """
        return self.key is not NoVal

    def result(self, xn_dict: Dict[Identifier, ExecNode]) -> Any:
        """Extract the result of the ExecNode corresponding to used key(s).

        Returns:
            Any: value inside the container
        """
        xn = xn_dict[self.id]
        # TODO: support infinitely many indices by
        #  * either making the attribute xn a Union[ExecNode, ExecNodeUsage] and then keep fetching the value inside until reaching an ExecNode
        #  * or make the key an infinitely recusive key ['key1', 'key2',..., 'keyN']
        if isinstance(self.key, NoValType):
            return _filter_noval(xn.result)
        # ignore typing error because it is the responsibility of the user to insure the result contained in the XN is indexable!
        # Will raise the appropriate exception automatically
        #  The user might have specified a subgraph to run => xn contain NoVal
        #  or the user tried to access a non-indexable object
        # NOTE: maybe handle the 3 types of exceptions that might occur properly to help the user through debugging
        if isinstance(xn.result, NoValType):
            raise TawaziTypeError(f"{xn} didn't run. Check DAG configuration")

        return _filter_noval(xn.result[self.key])


# TODO: make this a subpackage
# helpers for ExecNode

ReturnUXNsType = Union[
    None, UsageExecNode, Tuple[UsageExecNode], List[UsageExecNode], Dict[str, UsageExecNode]
]


def _is_list_or_tuple(returned_uxns: ReturnUXNsType) -> bool:
    if isinstance(returned_uxns, (tuple, list)):
        # 3.1 Collect all the return ids
        for ren in returned_uxns:
            if not isinstance(ren, UsageExecNode):
                # NOTE: this error shouldn't ever raise during usage.
                # Please report in https://github.com/mindee/tawazi/issues
                raise TawaziTypeError(f"{ReturnTypeErrString}. provided {ren}")
        return True
    return False


def _is_dict(returned_uxns: ReturnUXNsType) -> bool:
    if isinstance(returned_uxns, dict):
        for ren in returned_uxns.values():
            # 4.1 key should be str and value should be an ExecNode generated by running an xnode...
            if not isinstance(ren, UsageExecNode):
                raise TawaziTypeError(
                    f"return dict should only contain ExecNodes, but {ren} is of type {type(ren)}"
                )
        return True
    return False


def validate_returned_usage_exec_nodes(returned_uxns: ReturnUXNsType) -> None:
    """Get the IDs of the returned UsageExecNodes.

    Args:
        returned_uxns (ReturnXNsType): Aliases of the returned UsageExecNodes

    Raises:
        TawaziTypeError: _description_
        TawaziTypeError: _description_

    Returns:
        ReturnIDsType: Corresponding IDs of the returned UsageExecNodes
    """
    # TODO: support iterators etc.

    # 1 No value returned by the execution
    no_val = returned_uxns is None
    # 2 a single value is returned
    single_val = isinstance(returned_uxns, UsageExecNode)
    # 3 multiple values returned
    tuple_or_list_val = _is_list_or_tuple(returned_uxns)
    # 4 support dict
    dict_val = _is_dict(returned_uxns)
    if no_val or single_val or tuple_or_list_val or dict_val:
        return
    raise TawaziTypeError(
        f"{ReturnTypeErrString}. Type of the provided return: {type(returned_uxns)}"
    )
