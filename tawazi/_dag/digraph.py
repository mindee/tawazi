"""Module containing the definition of a Directed Graph Extension of networkx.DiGraph."""
from itertools import chain
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import networkx as nx
from loguru import logger
from networkx import NetworkXNoCycle, NetworkXUnfeasible, find_cycle

from tawazi.consts import Identifier, Tag


class DiGraphEx(nx.DiGraph):
    """Extends the DiGraph with some methods."""

    @classmethod
    def from_exec_nodes_mapping(
        cls, exec_nodes_mapping: Dict[Identifier, List[Tuple[Identifier, Identifier]]]
    ) -> "DiGraphEx":
        """Build a DigraphEx from a mapping between nodes and edges.

        Args:
            exec_nodes_mapping: the mapping between nodes and their edges

        Returns:
            the DigraphEx object
        """
        graph = DiGraphEx()

        for node_id, dependencies in exec_nodes_mapping.items():
            # add node and edges
            graph.add_node(node_id)
            graph.add_edges_from(dependencies)

        # check for circular dependencies
        cycle = graph.find_cycle()
        if cycle:
            raise NetworkXUnfeasible(f"the DAG contains at least a circular dependency: {cycle}")
        return graph

    @property
    def root_nodes(self) -> List[Identifier]:
        """Safely gets the root nodes.

        Returns:
            List of root nodes
        """
        return [node for node, degree in self.in_degree if degree == 0]

    @property
    def leaf_nodes(self) -> List[Identifier]:
        """Safely get the leaf nodes.

        Returns:
            List of leaf nodes
        """
        return [node for node, degree in self.out_degree if degree == 0]

    @property
    def tags(self) -> Set[str]:
        """Get all the tags available for the graph.

        Returns:
            A set of tags
        """
        return set(chain([tags for _, tags in self.nodes(data="tag")]))

    def get_tagged_nodes(self, tag: Tag) -> List[str]:
        """Get nodes with a certain tag.

        Args:
            tag: the tag identifier

        Returns:
            a list of nodes
        """
        return [xn for xn, tags in self.nodes(data="tag") if tags is not None and tag in tags]

    def single_node_successors(self, node_id: Identifier) -> List[Identifier]:
        """Get all the successors of a node with a depth first search.

        Args:
            node_id: the node acting as the root of the search

        Returns:
            list of the node's successors
        """
        return list(nx.dfs_tree(self, node_id).nodes())

    def multiple_nodes_successors(self, nodes_ids: Sequence[Identifier]) -> Set[Identifier]:
        """Get the successors of all nodes in the iterable.

        Args:
            nodes_ids: nodes of which we want successors

        Returns:
            a set of all the sucessors
        """
        return set(list(chain(*[self.single_node_successors(node) for node in nodes_ids])))

    def remove_recursively(self, root_node: Identifier, remove_root_node: bool = True) -> None:
        """Recursively removes all the nodes that depend on the provided.

        Args:
            root_node (Identifier): the root node
            remove_root_node (bool, optional): whether to remove the root node or not. Defaults to True.
        """
        nodes_to_remove = self.single_node_successors(root_node)

        # skip removing the root node if requested
        if not remove_root_node:
            nodes_to_remove.remove(root_node)

        for node in nodes_to_remove:
            self.remove_node(node)

    def subgraph_leaves(self, nodes: List[Identifier]) -> Set[Identifier]:
        """Modifies the graph to become a subgraph.

        The generated subgraph contains the provided nodes as leaf nodes.
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

        Returns:
            the nodes that are provided but can never become leaf nodes:
                NOTE Impossible cases are handled using a best effort approach;
                For example, if a node and its children are provided,
                all those nodes will be left in the subgraph. However,
                a warning will be issued

        Raises:
            ValueError: if the provided nodes are not in the graph
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
    def topologically_sorted(self) -> List[Identifier]:
        """Makes the simple topological sort of the graph nodes.

        Returns:
            List of nodes of the graph listed in topological order
        """
        return list(nx.topological_sort(self))

    def find_cycle(self) -> Optional[List[Tuple[Identifier, Identifier]]]:
        """Finds the cycles in the DAG. A DAG shouldn't have any dependency cycle.

        Returns:
            A list of the edges responsible for the cycles in case there are some (in forward and backward),
                otherwise nothing. (e.g. [('taxes', 'amount_reconciliation'),('amount_reconciliation', 'taxes')])
        """
        try:
            cycle: List[Tuple[Identifier, Identifier]] = find_cycle(self)
            return cycle
        except NetworkXNoCycle:
            return None

    def ancestors_of_iter(self, nodes: Iterable[Identifier]) -> Set[Identifier]:
        """Returns the ancestors of the provided nodes.

        Args:
            nodes (Set[Identifier]): The nodes to find the ancestors of

        Returns:
            Set[Identifier]: The ancestors of the provided nodes
        """
        ancestors = set()
        for node in nodes:
            ancestors.update(nx.ancestors(self, node))
        return ancestors
