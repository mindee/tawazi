from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

from typing_extensions import ParamSpec

ARG_NAME_TAG = "twz_tag"

RESERVED_KWARGS = [ARG_NAME_TAG]
ARG_NAME_SEP = ">>>"
USE_SEP_START = "<<"
USE_SEP_END = ">>"


class NoValType:
    """
    Tawazi's special None.
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
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "NoVal"

    def __eq__(self, __o: object) -> bool:
        return False

    def __copy__(self) -> "NoValType":
        return self

    def __deepcopy__(self, _prev: Dict[Any, Any]) -> "NoValType":
        return self


NoVal = NoValType()

# Constant types

Identifier = str
Tag = Union[None, str, tuple]  # anything immutable

# NOTE: maybe support other key types? for example int... or even tuple...
ReturnIDsType = Optional[
    Union[Dict[str, Identifier], List[Identifier], Tuple[Identifier], Identifier]
]
RVTypes = Union[Any, Tuple[Any], List[Any], Dict[str, Any]]
P = ParamSpec("P")
RVDAG = TypeVar("RVDAG", bound=RVTypes, covariant=True)

RVXN = TypeVar("RVXN", covariant=True)

# ImmutableType = Union[str, int, float, bool, Tuple[ImmutableType]]  # doesn't work because of cyclic typing
