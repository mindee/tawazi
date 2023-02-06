import time
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import networkx as nx
from loguru import logger
from networkx import find_cycle
from networkx.exception import NetworkXNoCycle, NetworkXUnfeasible

from tawazi.config import Cfg
from tawazi.consts import ReturnIDsType
from tawazi.helpers import filter_NoVal

from .consts import IdentityHash, Tag
from .digraph import DiGraphEx, subgraph
from .errors import ErrorStrategy, TawaziTypeError, TawaziUsageError
from .node import ArgExecNode, ExecNode


# TODO: make ExecNodes configurable using a yaml configuration file
class DAG:
    """
    Data Structure containing ExecNodes with interdependencies.
    The ExecNodes can be executed in parallel with the following restrictions:
        * Limited number of threads.
        * Parallelization constraint of each ExecNode (is_sequential attribute)
    """

    # TODO: transform into basemodel to do validation
    def __init__(
        self,
        exec_nodes: List[ExecNode],
        max_concurrency: int = 1,
        behavior: ErrorStrategy = ErrorStrategy.strict,
    ):
        """
        Args:
            exec_nodes: all the ExecNodes
            max_concurrency: the maximal number of threads running in parallel
            logger: the inferfwk logger name
            behavior: specify the behavior if an ExecNode raises an Error. Three option are currently supported:
                1. DAG.STRICT: stop the execution of all the DAG
                2. DAG.ALL_CHILDREN: do not execute all children ExecNodes, and continue execution of the DAG
                2. DAG.PERMISSIVE: continue execution of the DAG and ignore the error
        """
        self.graph_ids = DiGraphEx()

        # ExecNodes can be shared between Graphs, their call signatures might also be different
        # NOTE: maybe this should be transformed into a property because there is a deepcopy for node_dict...
        #  this means that there are different ExecNodes that are hanging arround in the same instance of the DAG
        self.exec_nodes = exec_nodes

        self.max_concurrency = max_concurrency

        self.node_dict: Dict[IdentityHash, ExecNode] = {
            exec_node.id: exec_node for exec_node in self.exec_nodes
        }
        # Calculate all the tags in the DAG to reduce overhead during computation
        self.tag_node_dict = {xn.tag: xn for xn in self.exec_nodes if xn.tag}

        # Might be useful in the future
        self.node_dict_by_name: Dict[str, ExecNode] = {
            exec_node.__name__: exec_node for exec_node in self.exec_nodes
        }
        self.return_ids: ReturnIDsType = None
        self.input_ids: List[IdentityHash] = []

        # a sequence of execution to be applied in a for loop
        self.exec_node_sequence: List[ExecNode] = []

        self.behavior = behavior

        self._build()

        self.bckrd_deps = {
            xn.id: list(self.graph_ids.predecessors(xn.id)) for xn in self.exec_nodes
        }
        self.frwrd_deps = {xn.id: list(self.graph_ids.successors(xn.id)) for xn in self.exec_nodes}

        # calculate the sum of priorities of all recursive children
        self._assign_compound_priority()

        # make a valid execution sequence to run sequentially if needed
        topological_order = self.graph_ids.topologically_sorted
        self.exec_node_sequence = [self.node_dict[xn_id] for xn_id in topological_order]

    @property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @max_concurrency.setter
    def max_concurrency(self, value: int) -> None:
        if value < 1:
            raise ValueError("Invalid maximum number of threads! Must be a positive integer")
        self._max_concurrency = value

    # getters
    def get_nodes_by_tag(self, tag: Any) -> List[ExecNode]:
        nodes = [ex_n for ex_n in self.exec_nodes if ex_n.tag == tag]
        return nodes

    def get_node_by_id(self, id_: IdentityHash) -> ExecNode:
        # TODO: ? catch the keyError and
        #   help the user know the id of the ExecNode by pointing to documentation!?
        return self.node_dict[id_]

    # TODO: get node by usage (the order of call of an ExecNode)

    # TODO: validate using Pydantic
    def _find_cycle(self) -> Optional[List[Tuple[str, str]]]:
        """
        A DAG doesn't have any dependency cycle.
        This method returns the cycles if found.
        return: A list of the edges responsible for the cycles in case there are some (in forward and backward),
        otherwise nothing.
        return example: [('taxes', 'amount_reconciliation'),('amount_reconciliation', 'taxes')]
        """
        try:
            cycle: List[Tuple[str, str]] = find_cycle(self.graph_ids)
            return cycle
        except NetworkXNoCycle:
            return None

    def _build(self) -> None:
        """
        Builds the graph and the sequence order for the computation.
        """
        # 1. Make the graph
        # 1.1 add nodes
        for xn in self.exec_nodes:
            self.graph_ids.add_node(xn.id)

        # 1.2 add edges
        for xn in self.exec_nodes:
            edges = [(dep.id, xn.id) for dep in xn.dependencies]
            self.graph_ids.add_edges_from(edges)

        # 2. Validate the DAG: check for circular dependencies
        cycle = self._find_cycle()
        if cycle:
            raise NetworkXUnfeasible(
                f"the product contains at least a circular dependency: {cycle}"
            )

    def _validate(self) -> None:
        # validate setup ExecNodes
        for xn in self.exec_nodes:
            if xn.setup and any(dep.id in self.input_ids for dep in xn.dependencies):
                raise TawaziUsageError(
                    f"The ExecNode {xn} takes as parameters one of the DAG's input parameter"
                )
        # future validations...

    def _assign_compound_priority(self) -> None:
        """
        Assigns a compound priority to all nodes in the graph.
        The compound priority is the sum of the priorities of all children recursively.
        """
        # 1. deepcopy graph_ids because it will be modified (pruned)
        graph_ids = deepcopy(self.graph_ids)
        leaf_ids = graph_ids.leaf_nodes

        # 2. assign the compound priority for all the remaining nodes in the graph:
        # Priority assignment happens by epochs:
        # 2.1. during every epoch, we assign the compound priority for the parents of the current leaf nodes
        # 2.2. at the end of every epoch, we trim the graph from its leaf nodes;
        #       hence the previous parents become the new leaf nodes
        while len(graph_ids) > 0:

            # Epoch level
            for leaf_id in leaf_ids:
                leaf_node = self.node_dict[leaf_id]

                for parent_id in self.bckrd_deps[leaf_id]:
                    # increment the compound_priority of the parent node by the leaf priority
                    parent_node = self.node_dict[parent_id]
                    parent_node.compound_priority += leaf_node.compound_priority

                # trim the graph from its leaf nodes
                graph_ids.remove_node(leaf_id)

            # assign the new leaf nodes
            leaf_ids = graph_ids.leaf_nodes

    def draw(self, k: float = 0.8, display: bool = True, t: int = 3) -> None:
        """
        Draws the Networkx directed graph.
        Args:
            k: parameter for the layout of the graph, the higher, the further the nodes apart
            display: display the layout created
            t: time to display in seconds
        """
        import matplotlib.pyplot as plt

        # TODO: use graphviz instead! it is much more elegant

        pos = nx.spring_layout(self.graph_ids, seed=42069, k=k, iterations=20)
        nx.draw(self.graph_ids, pos, with_labels=True)
        if display:
            plt.ion()
            plt.show()
            time.sleep(t)
            plt.close()

    @classmethod
    def _deepcopy_non_setup_x_nodes(cls, x_nodes: Dict[str, ExecNode]) -> Dict[str, ExecNode]:
        """
        Deep copy all ExecNodes except setup ExecNodes because they are shared throughout the DAG instance
        """
        x_nodes_copy = {}
        for id_, x_nd in x_nodes.items():
            # if execnode is a setup node, it shouldn't be copied
            if x_nd.setup:
                x_nodes_copy[id_] = x_nd
            else:
                x_nodes_copy[id_] = deepcopy(x_nd)
        return x_nodes_copy

    def _execute(
        self,
        leaves_ids: Optional[List[Union[IdentityHash, ExecNode]]] = None,
        modified_node_dict: Optional[Dict[str, ExecNode]] = None,
    ) -> Dict[IdentityHash, Any]:
        """
        Thread safe execution of the DAG...
         (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).
        Args:
            leaves_ids: The nodes (or the ids of the nodes) to be executed
        Returns:
            node_dict: dictionary with keys the name of the function and value the result after the execution
        """
        # 0.1 create a subgraph of the graph if necessary
        graph = subgraph(self.graph_ids, leaves_ids)

        # 0.2 deepcopy the node_dict in order to modify the results inside every node and make the dag reusable
        #     modified_node_dict are used to modify the values inside the ExecNode corresponding
        #     to the input arguments provided to the whole DAG (ArgExecNode)
        # if modified_node_dict:
        #     for ex_n in modified_node_dict.values():
        #         if ex_n.setup:
        #             raise TawaziBaseException(f"Setup nodes can't be provided as input to the DAG!,")

        # TODO: remove this deep copy because it already happens inside DAG.__call__
        node_dict = modified_node_dict or DAG._deepcopy_non_setup_x_nodes(self.node_dict)

        # 0.3 create variables related to futures
        futures: Dict[IdentityHash, "Future[Any]"] = {}
        done: Set["Future[Any]"] = set()
        running: Set["Future[Any]"] = set()

        # TODO: optimize the execution by directly running the pre-computed ExecNodes! (setup, ArgExecNodes)

        # TODO: support non "threadable" ExecNodes.
        #  These are ExecNodes that can't run inside a thread because their arguments aren't pickelable!

        # 0.4 create helpers functions encapsulated from the outside
        def get_num_running_threads(_futures: Dict[IdentityHash, "Future[Any]"]) -> int:
            # use not future.done() because there is no guarantee that Thread pool will directly execute
            # the submitted thread
            return sum([not future.done() for future in _futures.values()])

        def get_highest_priority_nodes(nodes: List[ExecNode]) -> List[ExecNode]:
            highest_priority = max(node.priority for node in nodes)
            return [node for node in nodes if node.priority == highest_priority]

        # 0.5 get the candidates root nodes that can be executed
        # runnable_nodes_ids will be empty if all root nodes are running
        runnable_nodes_ids = graph.root_nodes()

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
                num_runnable_nodes_ids = len(runnable_nodes_ids)
                if num_running_threads == self.max_concurrency or num_runnable_nodes_ids == 0:
                    # must wait and not submit any workers before a worker ends
                    # (that might create a new more prioritized node) to be executed
                    logger.debug(
                        f"Waiting for ExecNodes {running} to finish. Finished running {done}"
                    )
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    done = done.union(done_)

                # 1. among the finished futures:
                #       1. checks for exceptions
                #       2. and remove them from the graph
                for id_, fut in futures.items():
                    if fut.done() and id_ in graph:
                        logger.debug(f"Remove ExecNode {id_} from the graph")
                        self._handle_exception(graph, fut, id_)
                        graph.remove_node(id_)

                # 2. list the root nodes that aren't being executed
                runnable_nodes_ids = list(set(graph.root_nodes()) - set(futures.keys()))

                # 3. if no runnable node exist, go to step 6 (wait for a node to finish)
                #   (This **might** create a new root node)
                if len(runnable_nodes_ids) == 0:
                    logger.debug("No runnable Nodes available")
                    continue

                # 4. choose a node to run
                # 4.1 get the most prioritized node to run
                # 4.1.1 get all the nodes that have the highest priority
                runnable_nodes = [node_dict[node_id] for node_id in runnable_nodes_ids]
                highest_priority_nodes = get_highest_priority_nodes(runnable_nodes)

                # 4.1.2 get the node with the highest compound priority
                # (randomly selected if multiple are suggested)
                exec_node = sorted(highest_priority_nodes, key=lambda node: node.compound_priority)[
                    -1
                ]

                logger.info(f"{exec_node.id} will run!")

                # 4.2 if the current node must be run sequentially, wait for a running node to finish.
                # in that case we must prune the graph to re-check whether a new root node
                # (maybe with a higher priority) has been created => continue the loop
                # Note: This step might run a number of times in the while loop
                #       before the exec_node gets submitted
                num_running_threads = get_num_running_threads(futures)
                if exec_node.is_sequential and num_running_threads != 0:
                    logger.debug(
                        f"{exec_node.id} must not run in parallel."
                        f"Wait for the end of a node in {running}"
                    )
                    done_, running = wait(running, return_when=FIRST_COMPLETED)
                    # go to step 6
                    continue

                # 5.1 submit the exec node to the executor
                # TODO: make a special case if self.max_concurrency == 1
                #   then invoke the function directly instead of launching a thread
                exec_future = executor.submit(exec_node.execute, node_dict=node_dict)
                running.add(exec_future)
                futures[exec_node.id] = exec_future

                # 5.2 wait for the sequential node to finish
                # TODO: not sure this code ever runs
                if exec_node.is_sequential:
                    wait(futures.values(), return_when=ALL_COMPLETED)
        return node_dict

    def _get_leaves_ids(
        self, twz_nodes: Optional[List[Union[Tag, IdentityHash, ExecNode]]] = None
    ) -> List[IdentityHash]:
        """
        get the ids of ExecNodes corresponding to twz_nodes.
        The identification can be carried out using the tag, the Id, or the ExecNode itself.
        Keep in Mind that depending on the way ExecNode is provided inside twz_nodes,
         the returned id
        if nothing is provided it will return all leaves_ids
        Handles the debug nodes

        Args:
            twz_nodes (List[Union[Tag, IdentityHash, ExecNode]]): list of a mix of identifier that the user might provide to run a subgraph

        Raises:
            ValueError: if a requested ExecNode is not found in the DAG
            TawaziTypeError: if the Type of the identifier is not Tag, IdentityHash or ExecNode
            TawaziBaseException: if the returned List[IdentityHash] has the wrong length, this indicates a bug in the code

        Returns:
            List[IdentityHash]: ExecNodes' Identities
        """
        if twz_nodes is None:
            # TODO: make cached!
            leaves_ids = [xn.id for xn in self.exec_nodes]
        # 2. create leaves_ids
        else:
            leaves_ids = []
            for tag_or_id_or_node in twz_nodes:
                if isinstance(tag_or_id_or_node, ExecNode):
                    leaves_ids.append(tag_or_id_or_node.id)
                # todo: do further validation for the case of the tag!!
                elif isinstance(tag_or_id_or_node, (IdentityHash, tuple)):
                    tag_or_id = tag_or_id_or_node

                    # if leaves_identification is not ExecNode, it can be either
                    #  1. a Tag (Highest priority in case an id with the same value exists)
                    if node := self.tag_node_dict.get(tag_or_id):
                        leaves_ids.append(node.id)
                    #  2. or a node id!
                    elif isinstance(tag_or_id, IdentityHash):
                        node = self.get_node_by_id(tag_or_id)
                        leaves_ids.append(node.id)
                    else:
                        raise ValueError(f"{tag_or_id_or_node} is not found in the DAG")
                else:

                    raise TawaziTypeError(
                        "twz_nodes must be of type ExecNode, "
                        f"str or tuple identifying the node but provided {tag_or_id_or_node}"
                    )

            # after extending leaves_ids, we should do a recheck because this might recreate another debug-able XN...
            if Cfg.RUN_DEBUG_NODES:
                leaves_ids = self._extend_leaves_ids_debug_xns(leaves_ids)

        # 3. clean all leaves_ids from debug XNs if necessary
        if not Cfg.RUN_DEBUG_NODES:
            leaves_ids = [id_ for id_ in leaves_ids if not self.node_dict[id_].debug]

        # TODO: review the way the interface with DAG.execute works! it is not the best interface!
        return leaves_ids

    def _extend_leaves_ids_debug_xns(self, leaves_ids: List[IdentityHash]) -> List[IdentityHash]:
        new_debug_xn_discovered = True
        while new_debug_xn_discovered:
            new_debug_xn_discovered = False
            for id_ in leaves_ids:
                for successor_id in self.frwrd_deps[id_]:
                    is_successor_debug = self.node_dict[successor_id].debug
                    if successor_id not in leaves_ids and is_successor_debug:
                        # a new debug XN has been discovered!
                        new_debug_xn_discovered = True
                        preds_of_succs_ids = [xn_id for xn_id in self.bckrd_deps[successor_id]]

                        if set(preds_of_succs_ids).issubset(set(leaves_ids)):
                            # this new XN can run by only running the current leaves_ids
                            leaves_ids.append(successor_id)
        return leaves_ids

    # TODO: setup nodes should not have dependencies that pass in through the pipeline parameters!
    #  raise an error in this case!!
    def setup(self, twz_nodes: Optional[List[Union[Tag, IdentityHash, ExecNode]]] = None) -> None:
        """
        Run the setup ExecNodes for the DAG.
        If twz_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: currently if setup ExecNodes receive arguments from the Pipeline this method won't work because it doesn't support *args.
         This might be supported in the future though

        Args:
            twz_nodes (Optional[List[Union[Tag, IdentityHash, ExecNode]]], optional): The ExecNodes that the user aims to use in the DAG.
              This might inlcude setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes. Defaults to None.
        """

        # 1. select all setup ExecNodes
        #  do not copy the setup nodes because we want them to be modified per DAG instance!
        all_setup_nodes = {
            nd.id: nd
            for nd in self.exec_nodes
            if nd.setup or (isinstance(nd, ArgExecNode) and nd.executed)
        }

        # 2. if twz_nodes is not provided run all setup ExecNodes
        if twz_nodes is None:
            setup_leaves_ids = list(all_setup_nodes.keys())
        else:
            # 2.1 the leaves_ids that the user wants to execute
            #  however they might contain non setup nodes... so we should extract all the nodes ids
            #  that must be run in order to run the twz_nodes ExecNodes
            #  afterwards we can remove the non setup nodes
            leaves_ids = self._get_leaves_ids(twz_nodes)
            graph = subgraph(self.graph_ids, leaves_ids)  # type: ignore

            # 2.2 filter non setup ExecNodes
            setup_leaves_ids = [id_ for id_ in graph.nodes if id_ in all_setup_nodes]

        self._execute(setup_leaves_ids, all_setup_nodes)  # type: ignore

    def __call__(
        self, *args: Any, twz_nodes: Optional[List[Union[Tag, IdentityHash, ExecNode]]] = None
    ) -> Any:
        """
        Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Args:
            twz_nodes (Optional[List[Union[Tag, IdentityHash, ExecNode]]], optional): ExecNodes to execute as a subgraph;
             executes the whole DAG if None. Defaults to None.

        Raises:
            TypeError: If called with an invalid number of arguments
            TawaziTypeError: if twz_nodes contains a wrong typed identifier or if the return value contain a non LazyExecNode

        Returns:
            Any: _description_
        """
        # 1. get the leaves ids to execute in case of a subgraph
        leaves_ids = self._get_leaves_ids(twz_nodes)
        #

        # 2. copy the ExecNodes
        call_xn_dict = self._make_call_xn_dict(*args)

        # 3. Execute the scheduler
        all_nodes_dict = self._execute(leaves_ids, call_xn_dict)  # type: ignore

        # 4. extract the returned value/values
        returned_values = self._get_return_values(all_nodes_dict)

        return returned_values

    # def make_executor(self):

    def _make_call_xn_dict(self, *args: Any) -> Dict[IdentityHash, ExecNode]:
        """
        Generate the calling ExecNode dict.
        This is a dict containing ExecNodes that will be executed (hence modified) by the DAG scheduler.
        This takes into consideration:
         1. deep copying the ExecNodes
         2. filling the arguments of the call
         3. skipping the copy for setup ExecNodes
        """
        # 1. deepcopy the node_dict because it will be modified by the DAG's execution
        call_xn_dict = DAG._deepcopy_non_setup_x_nodes(self.node_dict)

        # 2. parse the input arguments of the pipeline
        # 2.1 default valued arguments can be skipped and not provided!
        # note: if not enough arguments are provided then the code will fail inside the DAG's execution through the raise_err lambda
        if args:
            # 2.2 can't provide more than enough arguments
            if len(args) > len(self.input_ids):
                raise TypeError(
                    f"The DAG takes a maximum of {len(self.input_ids)} arguments. {len(args)} arguments provided"
                )

            # 2.3 modify ExecNodes corresponding to input ArgExecNodes
            for ind_arg, arg in enumerate(args):
                node_id = self.input_ids[ind_arg]

                call_xn_dict[node_id].result = arg

        return call_xn_dict

    def _get_return_values(
        self, xn_dict: Dict[IdentityHash, ExecNode]
    ) -> Union[Any, Tuple[Any], List[Any]]:
        """
        Extract the return value/values from the output of the DAG's scheduler!

        Args:
            xn_dict (Dict[IdentityHash, ExecNode]): _description_

        Raises:
            TawaziTypeError: _description_

        Returns:
            Union[Any, Tuple[Any], List[Any]]: _description_
        """
        if self.return_ids is None:
            return None
        if isinstance(self.return_ids, IdentityHash):
            return filter_NoVal(xn_dict[self.return_ids].result)
        if isinstance(self.return_ids, (tuple, list)):
            gen = (filter_NoVal(xn_dict[ren_id].result) for ren_id in self.return_ids)
            if isinstance(self.return_ids, tuple):
                return tuple(gen)
            if isinstance(self.return_ids, list):
                return list(gen)
        if isinstance(self.return_ids, dict):
            return {
                key: filter_NoVal(xn_dict[ren_id].result) for key, ren_id in self.return_ids.items()
            }

        raise TawaziTypeError("Return type for the DAG can only be a single value, Tuple or List")

    # NOTE: this function should be used in case there was a bizarre behavior noticed during the
    #   the execution of the DAG via DAG.execute(...)
    def _safe_execute(
        self, *args: Any, twz_nodes: Optional[List[Union[Tag, IdentityHash, ExecNode]]] = None
    ) -> Any:
        """
        Execute the ExecNodes in topological order without priority in for loop manner for debugging purposes
        """
        # 1. make the graph_ids to be executed!
        leaves_ids = self._get_leaves_ids(twz_nodes)
        graph_ids = subgraph(self.graph_ids, leaves_ids)  # type: ignore

        # 2. make call_xn_dict that will be modified
        call_xn_dict = self._make_call_xn_dict(*args)

        # 3. deep copy the node_dict to store the results in each node
        for xn_id in graph_ids.topologically_sorted:
            # only execute ExecNodes that are part of the subgraph
            call_xn_dict[xn_id].execute(call_xn_dict)

        # 4. make returned values
        return_values = self._get_return_values(call_xn_dict)

        return return_values

    def _handle_exception(self, graph: DiGraphEx, fut: "Future[Any]", id_: IdentityHash) -> None:
        """
        checks if futures have produced exceptions, and handles them
        according to the specified behavior
        Args:
            graph: the graph
            fut: the future
            id_: the identification of the ExecNode

        Returns:

        """

        if self.behavior == ErrorStrategy.strict:
            # will raise the first encountered exception if there's one
            # no simpler way to check for exception, and not supported by flake8
            _res = fut.result()  # noqa: F841

        else:
            try:
                _res = fut.result()  # noqa: F841

            except Exception:
                logger.exception(f"The feature {id_} encountered the following error:")

                if self.behavior == ErrorStrategy.permissive:
                    logger.warning("Ignoring exception as the behavior is set to permissive")

                elif self.behavior == ErrorStrategy.all_children:
                    # remove all its children. Current node will be removed directly afterwards
                    successors = list(graph.successors(id_))
                    for children_ids in successors:
                        graph.remove_recursively(children_ids)

                else:
                    raise NotImplementedError(f"Unknown behavior name: {self.behavior}")
