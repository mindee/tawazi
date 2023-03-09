"""Module containing constants used by Tawazi."""
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union

from typing_extensions import ParamSpec

ARG_NAME_TAG = "twz_tag"

RESERVED_KWARGS = [ARG_NAME_TAG]
ARG_NAME_SEP = ">>>"
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
Tag = Union[None, str, tuple]  # anything immutable

RVTypes = Union[Any, Tuple[Any], List[Any], Dict[str, Any]]
P = ParamSpec("P")
RVDAG = TypeVar("RVDAG", bound=RVTypes, covariant=True)

RVXN = TypeVar("RVXN", covariant=True)

# ImmutableType = Union[str, int, float, bool, Tuple[ImmutableType]]  # doesn't work because of cyclic typing
