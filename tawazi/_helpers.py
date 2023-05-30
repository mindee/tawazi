"""Module for helper functions."""
import inspect
from typing import Any, Callable, Dict, List, Tuple, Union

import yaml

from tawazi.consts import USE_SEP_END, USE_SEP_START, Identifier, NoVal, NoValType
from tawazi.errors import _raise_arg_exc


def ordinal(numb: int) -> str:
    """Construct the string corresponding to the ordinal of a number.

    Args:
        numb (int): order

    Returns:
        str: "0th", "1st", "2nd", etc...
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


def get_args_and_default_args(func: Callable[..., Any]) -> Tuple[List[str], Dict[str, Any]]:
    """Retrieves the arguments names and the default arguments of a function.

    Args:
        func: the target function

    Returns:
        A Tuple containing a List of argument names of non default arguments,
         and the mapping between the arguments and their default value for default arguments

    >>> def f(a1, a2, *args, d1=123, d2=None): pass
    >>> get_args_and_default_args(f)
    (['a1', 'a2', 'args'], {'d1': 123, 'd2': None})
    """
    signature = inspect.signature(func)
    args = []
    default_args = {}
    for k, v in signature.parameters.items():
        if v.default is not inspect.Parameter.empty:
            default_args[k] = v.default
        else:
            args.append(k)

    return args, default_args


def _make_raise_arg_error(func_name: str, arg_name: str) -> Callable[[], None]:
    # declare a local function that will raise an error in the scheduler if
    # the user doesn't pass in This ArgExecNode as argument to the Attached LazyExecNode
    return lambda: _raise_arg_exc(func_name, arg_name)


def _lazy_xn_id(base_id: Identifier, count_usages: int) -> Identifier:
    if count_usages > 0:
        return f"{base_id}{USE_SEP_START}{count_usages}{USE_SEP_END}"

    return base_id


def _filter_noval(v: Union[NoValType, Any]) -> Any:
    if v is NoVal:
        return None
    return v


# courtesy of https://gist.github.com/pypt/94d747fe5180851196eb?permalink_comment_id=3401011#gistcomment-3401011
class _UniqueKeyLoader(yaml.SafeLoader):
    def construct_mapping(self, node: Any, deep: bool = False) -> Any:
        mapping = []
        for key_node, _value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise KeyError(f"key {key} already in yaml file")
            mapping.append(key)
        return super().construct_mapping(node, deep)
