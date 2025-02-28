"""Helpers for node subpackage that use both ExecNode and UsageExecNode."""

from collections.abc import Iterable, Iterator
from typing import Any, Callable, Optional, Union

from . import node
from .node import ReturnExecNode
from .uxn import UsageExecNode

ReturnUXNsType = Union[
    None, UsageExecNode, tuple[UsageExecNode, ...], list[UsageExecNode], dict[str, UsageExecNode]
]


def _wrap_in_iterator_helper(
    func: Callable[..., Any], r_val: Iterable[Any]
) -> Iterator[UsageExecNode]:
    l_uxn: list[UsageExecNode] = []
    for i, v in enumerate(r_val):
        if not isinstance(v, UsageExecNode):
            xn = ReturnExecNode.from_function(func, i)
            uxn = UsageExecNode(xn.id)
            l_uxn.append(uxn)

            node.exec_nodes[xn.id] = xn
            node.results[xn.id] = v
        else:
            l_uxn.append(v)
    return iter(l_uxn)


def _wrap_in_list(func: Callable[..., Any], r_val: Any) -> Optional[list[UsageExecNode]]:
    if not isinstance(r_val, list):
        return None
    return list(_wrap_in_iterator_helper(func, r_val))


def _wrap_in_tuple(func: Callable[..., Any], r_val: Any) -> Optional[tuple[UsageExecNode, ...]]:
    if not isinstance(r_val, tuple):
        return None
    return tuple(_wrap_in_iterator_helper(func, r_val))


def _wrap_in_dict(func: Callable[..., Any], r_val: Any) -> Optional[dict[str, UsageExecNode]]:
    if not isinstance(r_val, dict):
        return None
    d_uxn: dict[str, UsageExecNode] = {}
    for k, v in r_val.items():
        if not isinstance(v, UsageExecNode):
            xn = ReturnExecNode.from_function(func, k)
            uxn = UsageExecNode(xn.id)
            d_uxn[k] = uxn

            node.exec_nodes[xn.id] = xn
            node.results[xn.id] = v
        else:
            d_uxn[k] = v
    return d_uxn


def _wrap_in_uxn(func: Callable[..., Any], r_val: Any) -> UsageExecNode:
    if not isinstance(r_val, UsageExecNode):
        xn = ReturnExecNode.from_function(func, 0)
        node.exec_nodes[xn.id] = xn
        node.results[xn.id] = r_val
        return UsageExecNode(xn.id)
    return r_val


def wrap_in_uxns(func: Callable[..., Any], r_val: Any) -> ReturnUXNsType:
    """Get the IDs of the returned UsageExecNodes.

    Args:
        func (ReturnXNsType): function of the DAG description
        r_val (Any): Returned value from DAG describing function
        results (Dict[Identifier, Any): The results of the DAG's description containg the results (containing the constants in this context)

    Raises:
        TawaziTypeError: _description_
        TawaziTypeError: _description_

    Returns:
        ReturnIDsType: Corresponding IDs of the returned UsageExecNodes
    """
    # TODO: support iterators etc.

    # 1 No value returned by the execution
    no_val = r_val is None
    if no_val:
        return None
    # 3 multiple values returned
    list_ = _wrap_in_list(func, r_val)
    if list_ is not None:
        return list_
    tuple_ = _wrap_in_tuple(func, r_val)
    if tuple_ is not None:
        return tuple_
    dict_ = _wrap_in_dict(func, r_val)
    if dict_ is not None:
        return dict_
    # 2 a single value is returned
    return _wrap_in_uxn(func, r_val)
