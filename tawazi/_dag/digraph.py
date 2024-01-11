"""Module containing the definition of a Directed Graph Extension of networkx.DiGraph."""
import time
from copy import deepcopy
from itertools import chain
from typing import Dict, Iterable, List, Optional, Sequence, Set, Union

import networkx as nx
from networkx import NetworkXNoCycle, NetworkXUnfeasible, find_cycle
from typing_extensions import Self

from tawazi.config import Config
from tawazi.consts import Identifier, Tag
from tawazi.errors import TawaziUsageError
from tawazi.node import ExecNode, UsageExecNode


class DiGraphEx(nx.DiGraph):
    """Extends the DiGraph with some methods."""

    @classmethod
    def from_exec_nodes(
        cls, input_nodes: List[UsageExecNode], exec_nodes: Dict[Identifier, ExecNode]
    ) -> Self:
        """Build a DigraphEx from exec nodes.

        Args:
            input_nodes: nodes that are the inputs of the graph
            exec_nodes: the graph nodes

        Returns:
            the DigraphEx object
        """
        graph = DiGraphEx()

        input_ids = [uxn.id for uxn in input_nodes]
        for node in exec_nodes.values():
            # add node and edges
            graph.add_node(node.id)
            graph.add_edges_from([(dep.id, node.id) for dep in node.dependencies])

            # add tag, setup and debug
            if node.tag:
                if isinstance(node.tag, Tag):
                    graph.nodes[node.id]["tag"] = [node.tag]
                else:
                    graph.nodes[node.id]["tag"] = [t for t in node.tag]

            graph.nodes[node.id]["debug"] = node.debug
            graph.nodes[node.id]["setup"] = node.setup
            graph.nodes[node.id]["compound_priority"] = node.priority

            # validate setup ExecNodes
            if node.setup and any(dep.id in input_ids for dep in node.dependencies):
                raise TawaziUsageError(
                    f"The ExecNode {node} takes as parameters one of the DAG's input parameter"
                )

        # check for circular dependencies
        try:
            cycle = find_cycle(graph)
            raise NetworkXUnfeasible(f"the DAG contains at least a circular dependency: {cycle}")
        except NetworkXNoCycle:
            pass

        # compute the sum of priorities of all recursive children
        graph.assign_compound_priority()

        return graph

    def make_subgraph(
        self,
        target_nodes: Optional[List[str]] = None,
        exclude_nodes: Optional[List[str]] = None,
        root_nodes: Optional[List[str]] = None,
    ) -> Self:
        """Builds the DigraphEx, with potential graph pruning.

        Args:
            target_nodes: nodes that we want to run and their dependencies
            exclude_nodes: nodes that should be excluded from the graph
            root_nodes: base ancestor nodes from which to start graph resolution

        Returns:
            Base Graph that will be used for the computations
        """
        graph = deepcopy(self)

        # first try to heavily prune removing roots
        if root_nodes is not None:
            if not set(root_nodes).issubset(set(graph.root_nodes)):
                raise ValueError(
                    f"nodes {set(graph.root_nodes).difference(set(root_nodes))} aren't root nodes."
                )

            # extract subgraph with only provided roots
            # NOTE: copy is because edges/nodes are shared with original graph
            graph = graph.subgraph(graph.multiple_nodes_successors(root_nodes)).copy()

        # then exclude nodes
        if exclude_nodes is not None:
            graph.remove_nodes_from(graph.multiple_nodes_successors(exclude_nodes))

        # lastly select additional nodes
        if target_nodes is not None:
            graph = graph.minimal_induced_subgraph(target_nodes).copy()

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
    def debug_nodes(self) -> List[Identifier]:
        """Get the debug nodes.

        Returns:
            the debug nodes
        """
        return [node for node, debug in self.nodes(data="debug") if debug]

    @property
    def setup_nodes(self) -> List[Identifier]:
        """Get the setup nodes.

        Returns:
            the setup nodes
        """
        return [node for node, setup in self.nodes(data="setup") if setup]

    @property
    def tags(self) -> Set[str]:
        """Get all the tags available for the graph.

        Returns:
            A set of tags
        """
        return set(chain(*(tags for _, tags in self.nodes(data="tag"))))

    @property
    def topologically_sorted(self) -> List[Identifier]:
        """Makes the simple topological sort of the graph nodes.

        Returns:
            List of nodes of the graph listed in topological order
        """
        return list(nx.topological_sort(self))

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
        return set(list(chain(*[self.single_node_successors(node_id) for node_id in nodes_ids])))

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

    def include_debug_nodes(self, leaves_ids: List[Identifier]) -> List[Identifier]:
        """Get debug nodes that are runnable with provided nodes as direct roots.

        For example:
        A
        |
        B
        | \
        D E

        if D is not a debug ExecNode and E is a debug ExecNode.
        If the subgraph whose leaf ExecNode D is executed,
        E should also be included in the execution because it can be executed (debug node whose inputs are provided)
        Hence we should extend the subgraph containing only D to also contain E

        Args:
            leaves_ids: the leaves ids of the subgraph

        Returns:
            the leaves ids of the new extended subgraph that contains more debug ExecNodes
        """
        new_debug_xn_discovered = True
        while new_debug_xn_discovered:
            new_debug_xn_discovered = False
            for id_ in leaves_ids:
                for successor_id in self.successors(id_):
                    if successor_id not in leaves_ids and successor_id in self.debug_nodes:
                        # a new debug XN has been discovered!
                        if set(self.predecessors(successor_id)).issubset(set(leaves_ids)):
                            new_debug_xn_discovered = True
                            # this new XN can run by only running the current leaves_ids
                            leaves_ids.append(successor_id)
        return leaves_ids

    def extend_graph_with_debug_nodes(self, original_graph: Self, cfg: Config) -> Self:
        """Add or remove debug nodes depending on the configuration.

        Args:
            original_graph: the graph containing a broader set of nodes
            cfg: the tawazi configuration

        Returns:
            the correct subgraph
        """
        if cfg.RUN_DEBUG_NODES:
            nodes_to_include = original_graph.include_debug_nodes(self.leaf_nodes) + list(
                self.nodes
            )
        else:
            nodes_to_include = list(set(self.nodes) - set(self.debug_nodes))

        # networkx typing problem
        return original_graph.subgraph(nodes_to_include).copy()  # type: ignore[no-any-return]

    def minimal_induced_subgraph(self, nodes: List[Identifier]) -> Self:
        """Get the minimal induced subgraph containing the provided nodes.

        The generated subgraph contains the provided nodes as leaf nodes.
        For example:
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
            the induced subgraph over the original graph with the provided nodes:

        Raises:
            ValueError: if the provided nodes are not in the graph
        """
        if any([node not in self.nodes for node in nodes]):
            raise ValueError(
                f"The provided nodes are not in the graph. "
                f"The provided nodes are: {nodes}."
                f"The graph only contains: {self.nodes}."
            )

        # compute all the ancestor nodes that will be included in the graph
        all_ancestors = self.ancestors_of_iter(nodes)
        induced_subgraph: DiGraphEx = nx.induced_subgraph(self, all_ancestors | set(nodes))
        return induced_subgraph

    def ancestors_of_iter(self, nodes: Iterable[Identifier]) -> Set[Identifier]:
        """Returns the ancestors of the provided nodes.

        Args:
            nodes (Set[Identifier]): The nodes to find the ancestors of

        Returns:
            Set[Identifier]: The ancestors of the provided nodes
        """
        return set().union(*chain(nx.ancestors(G=self, source=node) for node in nodes))

    def assign_compound_priority(self) -> None:
        """Assigns a compound priority to all nodes in the graph.

        The compound priority is the sum of the priorities of all children recursively.
        """
        # 1. start from bottom up
        leaf_ids = set(self.leaf_nodes)

        # 2. assign the compound priority for all the remaining nodes in the graph:
        # Priority assignment happens by epochs:
        # 2.1. during every epoch, we assign the compound priority for the parents of the current leaf nodes

        while leaf_ids:
            next_leaf_ids = set()
            for leaf_id in leaf_ids:
                compound_priority = self.nodes(data="compound_priority")[leaf_id]

                # for parent nodes, this loop won't execute
                for parent_id in self.predecessors(leaf_id):
                    # increment the compound_priority of the parent node by the leaf priority
                    self.nodes[parent_id]["compound_priority"] += compound_priority

                    next_leaf_ids.add(parent_id)
            leaf_ids = next_leaf_ids

    def draw(self, k: float = 0.8, t: Union[float, int] = 3) -> None:
        """Draws the Networkx directed graph.

        Args:
            k (float): parameter for the layout of the graph, the higher, the further the nodes apart. Defaults to 0.8.
            t (int): time to display in seconds. Defaults to 3.
        """
        import matplotlib.pyplot as plt

        # TODO: use graphviz instead! it is much more elegant

        pos = nx.spring_layout(self, seed=42069, k=k, iterations=20)
        nx.draw(self, pos, with_labels=True)
        plt.ion()
        plt.show()
        time.sleep(t)
        plt.close()
