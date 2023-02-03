from copy import deepcopy
from typing import List, Optional, Set, Union

import networkx as nx
from loguru import logger

from tawazi.node import ExecNode

from .consts import IdentityHash


# TODO: find a .pre-commit hook that aligns properly the fstrings
#   if it doesn't exist... make one! it should take max columns as argument
# todo remove dependency on DiGraph!
class DiGraphEx(nx.DiGraph):
    """
    Extends the DiGraph with some methods
    """

    def root_nodes(self) -> List[IdentityHash]:
        """
        Safely gets the root nodes
        Returns:
            the root nodes
        """
        return [node for node, degree in self.in_degree if degree == 0]

    @property
    def leaf_nodes(self) -> List[IdentityHash]:
        """
        Safely gets the leaf nodes
        Returns:
            the leaf nodes
        """
        return [node for node, degree in self.out_degree if degree == 0]

    def remove_recursively(self, root_node: IdentityHash) -> None:
        """
        Recursively removes all the nodes that depend on the provided one
        Args:
            root_node: the root node
        """
        nodes_to_remove: Set[IdentityHash] = set()

        def dfs(n: IdentityHash, graph: DiGraphEx, visited: Set[IdentityHash]) -> None:
            if n in visited:
                return
            else:
                visited.add(n)
                for child in graph[n].keys():
                    dfs(child, graph, visited)

        dfs(root_node, self, nodes_to_remove)
        for node in nodes_to_remove:
            self.remove_node(node)

    def subgraph_leaves(self, nodes: List[IdentityHash]) -> Set[IdentityHash]:
        """
        modifies the graph to become a subgraph
        that contains the provided nodes as leaf nodes.
        For example:
        TODO: use the future print to test this function!
        graph =
        "
        A
        | \
        B  C
        |  |\
        D  E F
        "
        subgraph_leaves(D, C, E) ->
        "
        A
        | \
        B  C
        |  |
        D  E
        "
        C is not a node that can be made into leaf nodes
        Args:
            nodes: the list of nodes to be executed
        Returns: the nodes that are provided but can never become leaf nodes:
            Impossible cases are handled using a best effort approach;
                For example, if a node and its children are provided,
                all those nodes will be left in the subgraph. However,
                a warning will be issued
        """

        # works by pruning the graph until all leaf nodes
        # are contained inside the provided "nodes"
        # in the arguments of this method

        if any([node not in self.nodes for node in nodes]):
            raise ValueError(
                f"The provided nodes are not in the graph. "
                f"The provided nodes are: {nodes}."
                f"The graph only contains: {self.nodes}."
            )

        nodes_to_remove = set(self.leaf_nodes).difference(set(nodes))

        while nodes_to_remove:
            node_to_remove = nodes_to_remove.pop()
            self.remove_node(node_to_remove)

            nodes_to_remove = set(self.leaf_nodes).difference(set(nodes))

        unremovable_nodes = set(nodes).difference(set(self.leaf_nodes))

        if len(unremovable_nodes) > 0:
            logger.debug(
                f"The provided nodes contain more nodes than necessary, "
                f"please remove {unremovable_nodes} nodes"
            )

        return unremovable_nodes

    @property
    def topologically_sorted(self) -> List[IdentityHash]:
        """
        Makes the simple topological sort of the graph nodes
        """
        return list(nx.topological_sort(self))


def subgraph(
    graph: DiGraphEx, leaves_ids: Optional[List[Union[IdentityHash, ExecNode]]]
) -> DiGraphEx:
    """
    returns a deep copy of the same graph if leaves_ids is None,
    otherwise returns a new graph by applying `graph.subgraph_leaves`

    Args:
        graph (DiGraphEx): graph describing the DAG
        leaves_ids (List[Union[IdentityHash, ExecNode]]): The leaves that must be executed

    Returns:
        DiGraphEx: The subgraph of the provided graph
    """

    # TODO: avoid mutable state, hence avoid doing deep copies ?
    # 0. deep copy the graph ids because it will be pruned during calculation
    graph = deepcopy(graph)

    # TODO: make the creation of subgraph possible directly from initialization
    # 1. create the subgraph
    if leaves_ids is not None:
        # Extract the ids from the provided leaves/leaves_ids
        leaves_str_ids = [
            node_id.id if isinstance(node_id, ExecNode) else node_id for node_id in leaves_ids
        ]

        graph.subgraph_leaves(leaves_str_ids)

    return graph
