import inspect
from typing import Any, Callable, Dict, List, Tuple


def ordinal(numb: int) -> str:
    """Construct the string corresponding to the ordinal of a number

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
    """
    Retrieves the arguments names and the default arguments of a function.
    Args:
        func: the target function

    Returns:
        A Tuple containing a List of argument names of non default arguments,
         and the mapping between the arguments and their default value for default arguments
    >>> def f(a1, a2, *args, d1=123, d2=None): pass
    >>> get_args_and_default_args(f)
    >>> (['a1', 'a2', 'args'], {'d1': 123, 'd2': None})
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
