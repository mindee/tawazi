from typing import Any, Dict, List, Optional, Tuple, Type, Union

IdentityHash = str
Tag = Union[None, str, tuple]  # anything immutable

ARG_NAME_TAG = "twz_tag"

RESERVED_KWARGS = [ARG_NAME_TAG]
ARG_NAME_SEP = ">>>"
USE_SEP_START = "<<"
USE_SEP_END = ">>"
ReturnIDsType = Optional[
    Union[Dict[Any, IdentityHash], List[IdentityHash], Tuple[IdentityHash, ...], IdentityHash]
]


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
