"""Module for helper functions."""

from typing import Any, Callable, NoReturn, TypeVar

import yaml

from tawazi.errors import TawaziArgumentError


def ordinal(numb: int) -> str:
    """Construct the string corresponding to the ordinal of a number.

    Args:
        numb (int): order

    Returns:
        str: "0th", "1st", "2nd", etc...

    >>> ordinal(0)
    '0th'
    >>> ordinal(1)
    '1st'
    >>> ordinal(2)
    '2nd'
    >>> ordinal(3)
    '3rd'
    >>> ordinal(4)
    '4th'
    >>> ordinal(10)
    '10th'
    >>> ordinal(11)
    '11th'
    >>> ordinal(21)
    '21st'
    >>> ordinal(22)
    '22nd'
    >>> ordinal(23)
    '23rd'
    >>> ordinal(24)
    '24th'
    >>> ordinal(113)
    '113th'
    """
    if numb < 20:  # determining suffix for < 20
        if numb == 1:
            suffix = "st"
        elif numb == 2:
            suffix = "nd"
        elif numb == 3:
            suffix = "rd"
        else:
            suffix = "th"
    else:  # determining suffix for > 20
        tens = str(numb)
        tens = tens[-2]
        unit = str(numb)
        unit = unit[-1]
        if tens == "1":
            suffix = "th"
        else:
            if unit == "1":
                suffix = "st"
            elif unit == "2":
                suffix = "nd"
            elif unit == "3":
                suffix = "rd"
            else:
                suffix = "th"
    return str(numb) + suffix


def make_raise_arg_error(func_name: str, arg_name: str) -> Callable[[], NoReturn]:
    # declare a local function that will raise an error in the scheduler if
    # the user doesn't pass in This ArgExecNode as argument to the Attached LazyExecNode
    def local_func() -> NoReturn:
        raise TawaziArgumentError(func_name, arg_name)

    return local_func


# courtesy of https://gist.github.com/pypt/94d747fe5180851196eb?permalink_comment_id=3401011#gistcomment-3401011
class UniqueKeyLoader(yaml.SafeLoader):
    """Unique key safe loader for yaml."""

    def construct_mapping(self, node: Any, deep: bool = False) -> Any:
        """Construct mapping of the corresponding YAML."""
        mapping = []
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise KeyError(f"key {key} already in yaml file")
            mapping.append(key)
        return super().construct_mapping(node, deep)


T = TypeVar("T")
V = TypeVar("V")


class StrictDict(dict[T, V]):
    """A Dict that raises an error if key already used.

    >>> d = StrictDict({1: 2, 2: 3})
    >>> d[3] = 4
    >>> d[3] = 5
    Traceback (most recent call last):
    ...
    KeyError: 'key: 3, is already occupied by 4'
    """

    def __setitem__(self, key: T, value: V) -> None:
        if key in self:
            raise KeyError(f"key: {key}, is already occupied by {self[key]}")
        super().__setitem__(key, value)

    def force_set(self, key: T, value: V) -> None:
        """Force Set a key to a value."""
        super().__setitem__(key, value)
