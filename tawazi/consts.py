"""Module containing constants used by Tawazi."""
from enum import Enum, unique
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union

from typing_extensions import ParamSpec

ARG_NAME_TAG = "twz_tag"
ARG_NAME_ACTIVATE = "twz_active"
ARG_NAME_UNPACK_TO = "twz_unpack_to"
RESERVED_KWARGS = ARG_NAME_TAG, ARG_NAME_ACTIVATE, ARG_NAME_UNPACK_TO

# TODO: check for possible collisions
ARG_NAME_SEP = ">>>"
RETURN_NAME_SEP = "<<<"
USE_SEP_START = "<<"
USE_SEP_END = ">>"

ReturnTypeErrString = (
    "Return type of the pipeline must be either a Single Xnode,"
    " Tuple of Xnodes, List of Xnodes, dict of Xnodes or None"
)


class NoValType:
    """Tawazi's special None.

    This class is a singleton similar to None to determine that no value is assigned
    >>> NoVal1 = NoValType()
    >>> NoVal2 = NoValType()
    >>> assert NoVal1 is NoVal2
    >>> from copy import deepcopy, copy
    >>> assert NoVal1 is deepcopy(NoVal1)
    >>> assert NoVal1 is copy(NoVal1)
    >>> assert NoVal1 != NoVal1
    """

    _instance = None

    def __new__(cls: Type["NoValType"]) -> "NoValType":
        """Constructor for NoValType.

        Returns:
            NoValType: new instance of NoValType.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __bool__(self) -> bool:
        """Whether NoVal is Truthy or Falsy.

        Returns:
            bool: always False
        """
        return False

    def __repr__(self) -> str:
        """Representation of NoValType.

        Returns:
            str: "NoVal"
        """
        return "NoVal"

    def __eq__(self, __o: object) -> bool:
        """Check for equality.

        Args:
            __o (object): the other object

        Returns:
            bool: always returns False
        """
        return False

    def __copy__(self) -> "NoValType":
        """Copy of NoVal.

        Returns:
            NoValType: Returns the original because NoVal is a singleton.
        """
        return self

    def __deepcopy__(self, _prev: Dict[Any, Any]) -> "NoValType":
        """Deep copy NoVal.

        Args:
            _prev (Dict[Any, Any]): the previous state of the object

        Returns:
            NoValType: the original NoVal because NoVal is a singleton.
        """
        return self


NoVal = NoValType()

# Constant types

Identifier = str
Tag = str  # anything immutable but not a sequence
TagOrTags = Union[Tag, Tuple[Tag]]  # a sequence of tags

RVTypes = Union[Any, Tuple[Any, ...], List[Any], Dict[str, Any]]
P = ParamSpec("P")
RVDAG = TypeVar("RVDAG", bound=RVTypes, covariant=True)

RVXN = TypeVar("RVXN", covariant=True)


@unique
class Resource(str, Enum):
    """The Resource to use launching ExecNodes inside the DAG scheduler a DAG.

    Resource can be either:
    1. "thread": Launch the ExecNode in a thread (Default)
    2. "main-thread": Launch the ExecNode inside the main thread, directly inside the main scheduler.

    Notice that when "main-thread" is used, some of the scheduler functionalities stop working as previously expected:
    1. No new ExecNode will be launched during the execution of the corresponding ExecNode
    2. If timeout is set on the corresponding ExecNode, it is not guaranteed to work properly.
    """

    # supported behavior following a raised error
    thread: str = "thread"
    main_thread: str = "main-thread"
    # process: str = "process"  # Reserved for the future
    # sub_interpreter: str = "sub-interpreter"  # Reserved for the future


# ImmutableType = Union[str, int, float, bool, Tuple[ImmutableType]]  # doesn't work because of cyclic typing


@unique
class XNOutsideDAGCall(str, Enum):
    """The strategy to use when an ExecNode is called outside a DAG."""

    warning: str = "warning"  # raise a warning a single time
    error: str = "error"  # raise an error and stop DAG description
    ignore: str = "ignore"  # do nothing
