"""UsageExecNode module."""

from copy import deepcopy
from dataclasses import dataclass, field
from functools import reduce
from typing import Any, NoReturn, Union

from typing_extensions import Self

from tawazi.consts import Identifier, NoValType

# NOTE: None is hashable! In theory it can be used as a key in a dict!
KeyType = Union[str, int, tuple[Any, ...], None, NoValType]


# TODO: transform this logic into the ExecNode itself ?
@dataclass(frozen=True)
class UsageExecNode:
    """The usage of the ExecNode / LazyExecNode inside the function describing the DAG.

    If ExecNode is not indexed with a key or an int, NoVal is used as the key.
    """

    id: Identifier
    key: list[KeyType] = field(default_factory=list)

    # TODO: make type of key immutable or something hashable, that way
    #  we can directly build a nx DAG with nodes instead of node ids
    #  removing almost all of the duplicate graph building logic
    # used in the dag dependency description
    def __getitem__(self, key: KeyType) -> Self:
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

    def result(self, results: dict[Identifier, Any]) -> Any:
        """Extract the result of the ExecNode corresponding to used key(s).

        If no Value exists returns None.

        Returns:
            Any: value inside the container
        """
        # ignore typing error because it is the responsibility of the user to ensure the result of the XN is indexable!
        # Will raise the appropriate exception automatically
        #  The user might have specified a subgraph to run => xn contain NoVal
        #  or the user tried to access a non-indexable object
        # NOTE: maybe handle the 3 types of exceptions that might occur properly to help the user through debugging
        # if isinstance(xn.result, NoValType):
        #     raise TawaziTypeError(f"{xn} didn't run, hence its result is not indexable. Check your DAG's config")

        if self.id in results:
            return reduce(lambda obj, key: obj.__getitem__(key), self.key, results[self.id])
        return None

    def __bool__(self) -> NoReturn:
        """__bool__ operator."""
        # __bool__ can not be faked because it must return True or False
        raise NotImplementedError

    def __contains__(self, other: Any) -> NoReturn:
        """___contains__ operator."""
        # __contains__ can not be faked because the return value gets automatically casted to bool
        raise NotImplementedError
