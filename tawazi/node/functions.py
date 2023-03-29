"""Helpers for node subpackage."""

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from . import node
from .node import ReturnExecNode, UsageExecNode

ReturnUXNsType = Union[
    None, UsageExecNode, Tuple[UsageExecNode, ...], List[UsageExecNode], Dict[str, UsageExecNode]
]


def _wrap_in_iterator_helper(
    func: Callable[..., Any], r_val: Iterable[Any]
) -> Iterator[UsageExecNode]:
    l_uxn: List[UsageExecNode] = []
    for i, v in enumerate(r_val):
        if not isinstance(v, UsageExecNode):
            xn = ReturnExecNode(func, i, v)
            node.exec_nodes[xn.id] = xn
            uxn = UsageExecNode(xn.id)
            l_uxn.append(uxn)
        else:
            l_uxn.append(v)
    return iter(l_uxn)


def _wrap_in_list(func: Callable[..., Any], r_val: Any) -> Optional[List[UsageExecNode]]:
    if not isinstance(r_val, list):
        return None
    return list(_wrap_in_iterator_helper(func, r_val))


def _wrap_in_tuple(func: Callable[..., Any], r_val: Any) -> Optional[Tuple[UsageExecNode, ...]]:
    if not isinstance(r_val, tuple):
        return None
    return tuple(_wrap_in_iterator_helper(func, r_val))


def _wrap_in_dict(func: Callable[..., Any], r_val: Any) -> Optional[Dict[str, UsageExecNode]]:
    if not isinstance(r_val, dict):
        return None
    d_uxn: Dict[str, UsageExecNode] = {}
    for k, v in r_val.items():
        if not isinstance(v, UsageExecNode):
            xn = ReturnExecNode(func, k, v)
            node.exec_nodes[xn.id] = xn
            uxn = UsageExecNode(xn.id)
            d_uxn[k] = uxn
        else:
            d_uxn[k] = v
    return d_uxn


def _wrap_in_uxn(func: Callable[..., Any], r_val: Any) -> UsageExecNode:
    if not isinstance(r_val, UsageExecNode):
        xn = ReturnExecNode(func, 0, r_val)
        node.exec_nodes[xn.id] = xn
        return UsageExecNode(xn.id)
    return r_val


def wrap_in_uxns(func: Callable[..., Any], r_val: Any) -> ReturnUXNsType:
    """Get the IDs of the returned UsageExecNodes.

    Args:
        func (ReturnXNsType): function of the DAG description
        r_val (Any): Returned value from DAG describing function

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
