import logging
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import deepcopy
from logging import Logger
from typing import Any, Dict, Hashable, List, Optional, Set, Tuple

import networkx as nx
from networkx import find_cycle
from networkx.exception import NetworkXNoCycle, NetworkXUnfeasible

from .errors import ErrorStrategy
from .node import ExecNode

logger_ = logging.getLogger(__name__)


# todo remove dependency on DiGraph!
class DiGraphEx(nx.DiGraph):
    """
    Extends the DiGraph with some methods
    """

    def root_nodes(self) -> List[Hashable]:
        """
        Safely gets the root nodes
        Returns:
            the root nodes
        """
        return [node for node, degree in self.in_degree if degree == 0]

    def leaf_nodes(self) -> List[Hashable]:
        """
        Safely gets the leaf nodes
        Returns:
            the leaf nodes
        """
        return [node for node, degree in self.out_degree if degree == 0]


    def remove_recursively(self, root_node: "ExecNode") -> None:
        """
        Recursively removes all the nodes that depend on the provided one
        Args:
            root_node: the root node
        """
        nodes_to_remove: Set[ExecNode] = set()

        def dfs(n: ExecNode, graph: DiGraphEx, visited: Set[ExecNode]) -> None:
            if n in visited:
                return
            else:
                visited.add(n)
                for child in graph[n].keys():
                    dfs(child, graph, visited)

        dfs(root_node, self, nodes_to_remove)
        for node in nodes_to_remove:
            self.remove_node(node)


class DAG:
    """
    Data Structure containing ExecNodes with interdependencies.
    The ExecNodes can be executed in parallel with the following restrictions:
        * Limited number of threads.
        * Parallelization constraint of each ExecNode (is_sequential attribute)
    """

    def __init__(
        self,
        exec_nodes: List[ExecNode],
        max_concurrency: int = 1,
        behaviour: ErrorStrategy = ErrorStrategy.strict,
        logger: Logger = logger_,
    ):
        """
        Args:
            exec_nodes: all the ExecNodes
            max_concurrency: the maximal number of threads running in parallel
            logger: the inferfwk logger name
            behaviour: specify the behavior if an ExecNode raises an Error. Three option are currently supported:
                1. DAG.STRICT: stop the execution of all the DAG
                2. DAG.ALL_CHILDREN: do not execute all children ExecNodes, and continue execution of the DAG
                2. DAG.PERMISSIVE: continue execution of the DAG and ignore the error
        """
        self.graph = DiGraphEx()

        # since ExecNodes are modified they must be copied
        self.exec_nodes = exec_nodes

        self.max_concurrency = int(max_concurrency)
        assert max_concurrency >= 1, "Invalid maximum number of threads! Must be a positive integer"

        # variables necessary for DAG construction
        self.upwards_hierarchy: Dict[Hashable, List[Hashable]] = {
            exec_node.id: exec_node.depends_on for exec_node in self.exec_nodes
        }
        self.node_dict: Dict[Hashable, ExecNode] = {
            exec_node.id: exec_node for exec_node in self.exec_nodes
        }

        self.node_dict_by_name: Dict[str, ExecNode] = {
            exec_node.__name__: exec_node for exec_node in self.exec_nodes
        }

        # a sequence of execution to be applied in a for loop
        self.exec_node_sequence: List[ExecNode] = []

        self.logger = logger
        self.behaviour = behaviour

        self._build()

    def find_cycle(self) -> Optional[List[Tuple[str, str]]]:
        """
        A DAG doesn't have any dependency cycle.
        This method returns the cycles if found.
        return: A list of the edges responsible for the cycles in case there are some (in forward and backward),
        otherwise nothing.
        return example: [('taxes', 'amount_reconciliation'),('amount_reconciliation', 'taxes')]
        """
        try:
            cycle: List[Tuple[str, str]] = find_cycle(self.graph)
            return cycle
        except NetworkXNoCycle:
            return None

    def _build(self) -> None:
        """
        Builds the graph and the sequence order for the computation.
        """
        # add nodes
        for node_id in self.upwards_hierarchy.keys():
            self.graph.add_node(node_id)

        # add edges
        for node_id, dependencies in self.upwards_hierarchy.items():
            if dependencies is not None:
                edges = [(dep, node_id) for dep in dependencies]
                self.graph.add_edges_from(edges)

        # set sequence order and check for circular dependencies
        try:
            topological_order = self.topological_sort()
        except NetworkXUnfeasible:

            self.logger.warning(
                f"The graph can't be built because "
                f"the product contains at least a circular dependency: {self.find_cycle()} "
            )
            raise NetworkXUnfeasible

        # calculate the sum of priorities of all recursive children
        self.assign_recursive_children_compound_priority()


        self.exec_node_sequence = [self.node_dict[node_name] for node_name in topological_order]

    def assign_recursive_children_compound_priority(self) -> None:
        """
        Assigns a compound priority to all nodees in the graph.
        The compound priority is the sum of the priorities of all children recursively.
        """
        # if there was a forward dependency recorded, this would have been much easier
        graph = deepcopy(self.graph)
        
        leaf_ids = graph.leaf_nodes()

        for leaf_id in leaf_ids:
            node = self.node_dict[leaf_id]
            node.compound_priority = node.priority
        
        while len(graph) > 0:
            for leaf_id in leaf_ids:
                for parent in self.upwards_hierarchy[leaf_id]:
                    parent_node = self.node_dict[parent]
                    leaf_node = self.node_dict[leaf_id]
                    if parent_node.compound_priority is None:
                        parent_node.compound_priority = parent_node.priority 
                    
                    parent_node.compound_priority += leaf_node.compound_priority

                graph.remove_node(leaf_id)    
            leaf_ids = graph.leaf_nodes()

    def draw(self, k: float = 0.8) -> None:
        """
        Draws the Networkx directed graph.
        Args:
            k: parameter for the layout of the graph, the higher, the further the nodes apart
        """
        import matplotlib.pyplot as plt

        # todo use graphviz instead! it is much more elegant

        pos = nx.spring_layout(self.graph, seed=42069, k=k, iterations=20)
        nx.draw(self.graph, pos, with_labels=True)
        plt.show()

    def topological_sort(self) -> List[Hashable]:
        """
        Makes the simple topological sort of the graph nodes
        """
        return list(nx.topological_sort(self.graph))

    def execute(self) -> Dict[Hashable, Any]:
        """
        Thread safe execution of the DAG.
        Returns:
            node_dict: dictionary with keys the name of the function and value the result after the execution
        """
        # TODO: avoid mutable state, hence avoid doing deep copies ?
        graph = deepcopy(self.graph)
        node_dict = deepcopy(self.node_dict)

        # variables related to futures
        futures: Dict[Hashable, "Future[Any]"] = {}
        done: Set["Future[Any]"] = set()
        running: Set["Future[Any]"] = set()

        def get_num_running_threads(_futures: Dict[Hashable, "Future[Any]"]) -> int:
            # use not future.done() because there is no guarantee that Thread pool will directly execute
            # the submitted thread
            return sum([not future.done() for future in _futures.values()])

        # will be empty if all root nodes are running
        runnable_nodes = graph.root_nodes()

        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            while len(graph):
                # attempt to run **A SINGLE** root node #

                # 6. block scheduler execution if no root node can be executed.
                #    this can occur in two cases:
                #       1. if maximum concurrency is reached
                #       2. if no runnable node exists (i.e. all root nodes are being executed)
                #    in both cases: block until a node finishes
                #       => a new root node will be available
                num_running_threads = get_num_running_threads(futures)
                num_runnable_nodes = len(runnable_nodes)
                if num_running_threads == self.max_concurrency or num_runnable_nodes == 0:
                    # must wait and not submit any workers before a worker ends
                    # (that might create a new more prioritized node) to be executed
                    self.logger.debug(
                        f"Waiting for ExecNodes {running} to finish. Finished running {done}"
                    )
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    done = done.union(done_)

                # 1. among the finished futures:
                #       1. checks for exceptions
                #       2. and remove them from the graph
                for id_, fut in futures.items():
                    if fut.done() and id_ in graph:
                        self.logger.debug(f"Remove ExecNode {id_} from the graph")
                        self.handle_exception(graph, fut, id_)
                        graph.remove_node(id_)

                # 2. list the root nodes that aren't being executed
                runnable_nodes = list(set(graph.root_nodes()) - set(futures.keys()))

                # 3. if no runnable node exist, go to step 6 (wait for a node to finish)
                #   (This **might** create a new root node)
                if len(runnable_nodes) == 0:
                    self.logger.debug("No runnable Nodes available")
                    continue

                # 4. choose a node to run
                # 4.1 get the most prioritized runnable node
                node_id = sorted(runnable_nodes, key=lambda n: node_dict[n].priority)[-1]

                exec_node = node_dict[node_id]
                self.logger.info(f"{node_id} will run!")

                # 4.2 if the current node must be run sequentially, wait for a running node to finish.
                # in that case we must prune the graph to re-check whether a new root node
                # (maybe with a higher priority) has been created => continue the loop
                # Note: This step might run a number of times in the while loop
                #       before the exec_node gets submitted
                num_running_threads = get_num_running_threads(futures)
                if exec_node.is_sequential and num_running_threads != 0:
                    self.logger.debug(
                        f"{node_id} must run without parallelisme. "
                        f"Wait for the end of a node in {running}"
                    )
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    continue  # go to step 6

                # 5.1 submit the exec node to the executor
                exec_future = executor.submit(exec_node.execute, node_dict=node_dict)
                running.add(exec_future)
                futures[exec_node.id] = exec_future

                # 5.2 wait for the sequential node to finish
                # TODO: not sure this code ever runs
                if exec_node.is_sequential:
                    wait(futures.values(), return_when=ALL_COMPLETED)
        return node_dict

    def safe_execute(self) -> None:
        """
        Execute the ExecNodes in topological order without priority in for loop manner for debugging purposes
        """
        node_dict = deepcopy(self.node_dict)
        for node_id in self.topological_sort():
            node_dict[node_id].execute(node_dict)

    def handle_exception(self, graph: DiGraphEx, fut: "Future[Any]", id_: Hashable) -> None:
        """
        checks if futures have produced exceptions, and handles them
        according to the specified behaviour
        Args:
            graph: the graph
            fut: the future
            id_: the identification of the ExecNode

        Returns:

        """

        if self.behaviour == ErrorStrategy.strict:
            # will raise the first encountered exception if there's one
            # no simpler way to check for exception, and not supported by flake8
            _res = fut.result()  # noqa: F841

        else:
            try:
                _res = fut.result()  # noqa: F841

            except Exception:
                self.logger.exception(f"The feature {id_} encountered the following error:")

                if self.behaviour == ErrorStrategy.permissive:
                    self.logger.warning("Ignoring exception as the behaviour is set to permissive")

                elif self.behaviour == ErrorStrategy.all_children:
                    # remove all its children. Current node will be removed directly afterwards
                    successors = list(graph.successors(id_))
                    for children_ids in successors:
                        graph.remove_recursively(children_ids)

                else:
                    raise NotImplementedError(f"Unknown behaviour name: {self.behaviour}")
