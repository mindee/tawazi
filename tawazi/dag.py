import concurrent
import copy
import logging
from concurrent.futures import (ALL_COMPLETED, FIRST_COMPLETED,
                                ThreadPoolExecutor, wait, Future)
from copy import deepcopy
from logging import Logger
from types import FunctionType
from typing import Any, Callable, Dict, Hashable, List, Tuple, Union, Set, Optional

import networkx as nx
from networkx import find_cycle
from networkx.exception import NetworkXNoCycle, NetworkXUnfeasible

from .errors import ErrorStrategy
import tawazi


# todo remove reliance on DiGraph!
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
        return [n for n, d in self.in_degree if d == 0]

    def remove_recursively(self, root_node: Hashable):
        """
        Recursively removes all the nodes that depend on the provided one
        Args:
            root_node: the root node
        """
        nodes_to_remove: Set[ExecNode] = set()

        def dfs(n, graph, visited):
            if n in visited:
                return
            else:
                visited.add(n)
                for child in graph[n].keys():
                    dfs(child, graph, visited)

        dfs(root_node, self, nodes_to_remove)
        for node in nodes_to_remove:
            self.remove_node(node)


# todo look into the typing of the id_
class ExecNode:
    """
    This class is the base executable node of the Directed Acyclic Execution Graph
    """

    def __init__(
        self,
        id_: Hashable,
        exec_function: Callable = lambda **kwargs: None,
        depends_on: List[Hashable] = None,
        argument_name: Optional[str] = None,
        priority: int = 0,
        is_sequential: bool = True,
        logger: Logger = tawazi.logger,  # type: ignore
    ):
        """
        Args:
            id_ (Hashable): identifier of ExecNode.
            exec_function (Callable, optional): a callable will be executed in the graph.
            This is useful to make Joining ExecNodes (Nodes that enforce dependencies on the graph)
            depends_on (list): a list of ExecNodes' ids that must be executed beforehand.
            argument_name (str): The name of the argument used by ExecNodes that depend on this ExecNode.
            priority (int, optional): priority compared to other ExecNodes;
                the higher the number the higher the priority.
            is_sequential (bool, optional): whether to execute this ExecNode in sequential order with respect to others.
             When this ExecNode must be executed, all other nodes are waited to finish before starting execution.
             Defaults to False.
        """

        self.id = id_
        self.exec_function = exec_function
        self.depends_on = depends_on if depends_on else []
        self.priority = priority
        self.is_sequential = is_sequential
        self.logger = logger

        if isinstance(argument_name, str) and argument_name != "":
            self.argument_name = argument_name
        else:
            self.argument_name = self.id.__name__ if isinstance(self.id, FunctionType) else str(self.id)

        # todo remove and make ExecNode immutable
        self.result = None

    @property
    def calculated_dependencies(self):
        return isinstance(self.depends_on, list)

    # this is breaking change however
    def execute(self, node_dict: Dict[Hashable, "ExecNode"]) -> Dict[str, Any]:
        """
        Execute the ExecNode directly or according to an execution graph.
        Args:
            node_dict (Dict[Hashable, ExecNode]): A shared dictionary containing the other ExecNodes in the DAG;
                                                the key is the id of the ExecNode.

        Returns: the result of the execution of the current ExecNode
        """
        # 1. fabricate the arguments for this ExecNode
        self.logger.debug(f"Start executing {self.id} with task {self.exec_function}")

        kwargs = {node_dict[dep_hash].argument_name: node_dict[dep_hash].result for dep_hash in self.depends_on}
        result = self.exec_function(**kwargs)

        # 2. write the result
        self.result = result

        # 3. useless return value
        self.logger.debug(f"Finished executing {self.id} with task {self.exec_function}")
        return result

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
        logger: Logger = tawazi.logger,  # type: ignore
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
        self.hierarchy: Dict[Hashable, List[Hashable]] = \
            {exec_node.id: exec_node.depends_on for exec_node in self.exec_nodes}
        self.node_dict: Dict[Hashable, ExecNode] = {
            exec_node.id: exec_node for exec_node in self.exec_nodes
        }

        # a sequence of execution to be applied in a for loop
        self.exec_node_sequence: List[ExecNode] = []

        self.logger = logger
        self.behaviour = behaviour

        self._build()

    def find_cycle(self) -> Union[List[Tuple[str, str]], None]:
        """
        A DAG doesn't have any dependency cycle.
        This method returns the cycles if found.
        return: A list of the edges responsible for the cycles in case there are some (in forward and backward),
        otherwise nothing.
        return example: [('taxes', 'amount_reconciliation'),('amount_reconciliation', 'taxes')]
        """
        try:
            return find_cycle(self.graph)
        except NetworkXNoCycle:
            return None

    def _build(self) -> None:
        """
        Builds the graph and the sequence order for the computation.
        """
        # add nodes
        for node_id in self.hierarchy.keys():
            self.graph.add_node(node_id)

        # add edges
        for node_id, dependencies in self.hierarchy.items():
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

        self.exec_node_sequence = [self.node_dict[node_name] for node_name in topological_order]

    def draw(self, k=0.8) -> None:
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
        # todo is it possible to avoid mutable state, hence avoid doing deep copies ?
        graph = deepcopy(self.graph)
        node_dict = deepcopy(self.node_dict)

        # variables related to futures
        futures: Dict[Hashable, concurrent.futures.Future] = {}
        done: Set[Future] = set()
        running: Set[Future] = set()

        def get_num_running_threads(_futures: Dict[Hashable, concurrent.futures.Future]):
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
                    self.logger.debug(f"Waiting for ExecNodes {running} to finish. Finished running {done}")
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
                    self.logger.debug(f"{node_id} must run without parallelisme. "
                                      f"Wait for the end of a node in {running}")
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    continue  # go to step 6

                # 5.1 submit the exec node to the executor
                exec_future = executor.submit(exec_node.execute, node_dict=node_dict)
                running.add(exec_future)
                futures[exec_node.id] = exec_future

                # 5.2 wait for the sequential node to finish
                # todo I don't think this code ever runs maybe must delete it
                if exec_node.is_sequential:
                    wait(futures.values(), return_when=ALL_COMPLETED)
        return node_dict

    def safe_execute(self):
        """
        Execute the ExecNodes in topological order without priority in for loop manner for debugging purposes
        """
        node_dict = deepcopy(self.node_dict)
        for node_id in self.topological_sort():
            node_dict[node_id].execute(node_dict)

    def handle_exception(self, graph: DiGraphEx, fut: concurrent.futures.Future, id_: Hashable):
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
            _res = fut.result()

        else:
            try:
                _res = fut.result()

            except Exception as _exc:
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
