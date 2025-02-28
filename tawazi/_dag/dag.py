"""module containing DAG and DAGExecution which are the containers that run ExecNodes in Tawazi."""

import json
import logging
import pickle
import warnings
from collections import Counter
from collections.abc import Iterable, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from itertools import chain
from pathlib import Path
from typing import Any, Callable, Generic, NoReturn, Optional, Union

import networkx as nx
import yaml

from tawazi import consts
from tawazi._helpers import StrictDict, UniqueKeyLoader
from tawazi.config import cfg
from tawazi.consts import ARG_NAME_ACTIVATE, RVDAG, Identifier, P, Tag
from tawazi.errors import TawaziTypeError, TawaziUsageError
from tawazi.node import Alias, ArgExecNode, ExecNode, ReturnUXNsType, UsageExecNode, node
from tawazi.node.node import LazyExecNode, make_active, make_axn_id
from tawazi.profile import Profile

from .digraph import DiGraphEx
from .helpers import async_execute, extend_results_with_args, get_return_values, sync_execute

logger = logging.getLogger(__name__)


def construct_subdag_arg_uxns(
    *args: Iterable[Union[UsageExecNode, Any]], to_subdag_id: Callable[[str], str], qualname: str
) -> list[UsageExecNode]:
    """Construct UsageExecNodes from the arguments passed to a subdag."""
    # Construct default arguments
    uxns: list[UsageExecNode] = []
    for i, arg in enumerate(args):
        # arg is a default value
        if not isinstance(arg, UsageExecNode):
            axn = ArgExecNode(make_axn_id(to_subdag_id(qualname), i))
            node.exec_nodes[axn.id] = axn
            node.results[axn.id] = arg
            uxn = UsageExecNode(axn.id)
        else:
            uxn = arg
        uxns.append(uxn)
    return uxns


def detect_duplicates(expanded_config: list[tuple[Identifier, Any]]) -> None:
    duplicates = [
        id for id, count in Counter([id for id, _ in expanded_config]).items() if count > 1
    ]
    if duplicates:
        raise ValueError(f"trying to set two configs for nodes {duplicates}.")


@dataclass
class BaseDAG(Generic[P, RVDAG]):
    """Data Structure containing ExecNodes with interdependencies.

    Please do not instantiate this class directly. Use the decorator `@dag` instead.
    The ExecNodes can be executed in parallel with the following restrictions:
        * Limited number of threads.
        * Parallelization constraint of each ExecNode (is_sequential attribute)
        * Priority of each ExecNode (priority attribute)
        * Specific Resource per ExecNode (resource attribute)
    This Class has two flavors:
        * DAG: for synchronous execution
        * AsyncDAG: for asynchronous execution

    Args:
        exec_nodes: all the ExecNodes
        input_uxns: all the input UsageExecNodes
        return_uxns: the return UsageExecNodes of various types: None, a single value, tuple, list, dict.
        max_concurrency: the maximal number of threads running in parallel
    """

    qualname: str
    results: StrictDict[Identifier, Any]
    exec_nodes: StrictDict[Identifier, ExecNode]
    input_uxns: list[UsageExecNode]
    return_uxns: ReturnUXNsType
    max_concurrency: int = 1
    graph_ids: DiGraphEx = field(init=False)

    def __post_init__(self) -> None:
        self.graph_ids = DiGraphEx.from_exec_nodes(
            input_nodes=self.input_uxns, exec_nodes=self.exec_nodes
        )

        # verification
        if not isinstance(self.max_concurrency, int):
            raise ValueError("max_concurrency must be an int")
        if self.max_concurrency < 1:
            raise ValueError("Invalid maximum number of threads! Must be a positive integer")
        self._max_concurrency = self.max_concurrency

        if not isinstance(self.results, StrictDict):
            raise ValueError("results must be a StrictDict")
        if not isinstance(self.exec_nodes, StrictDict):
            raise ValueError("exec_nodes must be a StrictDict")

    def draw(
        self, *, include_args: bool = False, filename: Optional[str] = None, view: bool = True
    ) -> None:
        """Draws the Networkx directed graph.

        Args:
            include_args: whether to include the arguments or not
            filename: the name of the file to save the graph to
            view: whether to view the graph or not
        """
        from graphviz import Digraph

        dot = Digraph()

        for node_id in self.graph_ids.nodes():
            node = self.get_node_by_id(node_id)
            is_arg = isinstance(node, ArgExecNode)
            if is_arg and not include_args:
                continue

            dot.node(node_id)
            if is_arg:
                dot.node(node_id, label=f"{node_id}")
            else:
                path_to_file, line_number = node.call_location.split(":")

                dot.node(node_id, label=f"{node_id} #L{line_number}", URL="file://" + path_to_file)

        for edge in self.graph_ids.edges():
            if isinstance(self.get_node_by_id(edge[0]), ArgExecNode) and not include_args:
                continue
            dot.edge(edge[0], edge[1])

        dot.render(filename, view=view)

    # getters
    def get_nodes_by_tag(self, tag: Tag) -> list[ExecNode]:
        """Get the ExecNodes with the given tag.

        Note: the returned ExecNode is not modified by any execution!
            This means that you can not get the result of its execution via `DAG.get_nodes_by_tag(<tag>).result`.
            In order to do that, you need to make a DAGExecution and then call
             `DAGExecution.get_nodes_by_tag(<tag>).result`, which will contain the results.

        Args:
            tag (Any): tag of the ExecNodes

        Returns:
            List[ExecNode]: corresponding ExecNodes
        """
        return [self.exec_nodes[xn_id] for xn_id in self.graph_ids.get_tagged_nodes(tag)]

    def get_node_by_id(self, node_id: Identifier) -> ExecNode:
        """Get the ExecNode with the given id.

        Note: the returned ExecNode is not modified by any execution!
            This means that you can not get the result of its execution via `DAG.get_node_by_id(<id>).result`.
            In order to do that, you need to make a DAGExecution and then call
             `DAGExecution.get_node_by_id(<id>).result`, which will contain the results.

        Args:
            node_id (Identifier): id of the ExecNode

        Returns:
            ExecNode: corresponding ExecNode
        """
        try:
            return self.exec_nodes[node_id]
        except KeyError as e:
            raise ValueError(f"node {node_id} doesn't exist in the DAG.") from e

    def _get_single_xn_by_alias(self, alias: Alias) -> ExecNode:
        """Get the ExecNode corresponding to the given Alias.

        Args:
            alias (Alias): the Alias to be resolved

        Raises:
            ValueError: if the Alias is not unique

        Returns:
            ExecNode: the ExecNode corresponding to the given Alias
        """
        xns = self.alias_to_ids(alias)
        if len(xns) > 1:
            raise ValueError(
                f"Alias {alias} is not unique. It points to {len(xns)} ExecNodes: {xns}"
            )
        return self.exec_nodes[xns[0]]

    # TODO: get node by usage (the order of call of an ExecNode)

    # TODO: implement ellipsis for composing with outputs
    # TODO: should we support kwargs when DAG.__call__ support kwargs?

    def compose(
        self,
        qualname: str,
        inputs: Union[Alias, Sequence[Alias]],
        outputs: Union[Alias, Sequence[Alias]],
        is_async: Optional[bool] = None,
        **kwargs: dict[str, Any],
    ) -> "Union[AsyncDAG[P, RVDAG], DAG[P, RVDAG]]":
        """Compose a new DAG using inputs and outputs ExecNodes (Experimental).

        All provided `Alias`es must point to unique `ExecNode`s. Otherwise ValueError is raised
        The user is responsible to correctly specify inputs and outputs signature of the `DAG`.
        * The inputs can be specified as a single `Alias` or a `Sequence` of `Alias`es.
        * The outputs can be specified as a single `Alias` (a single value is returned)
        or a `Sequence` of `Alias`es in which case a Tuple of the values are returned.
        If outputs are specified as [], () is returned.
        The syntax is the following:

        ```python
        >>> from tawazi import dag, xn, DAG
        >>> from typing import Tuple, Any
        >>> @xn
        ... def unwanted_xn() -> int: return 42
        >>> @xn
        ... def x(v: Any) -> int: return int(v)
        >>> @xn
        ... def y(v: Any) -> str: return str(v)
        >>> @xn
        ... def z(x: int, y: str) -> float: return float(x) + float(y)
        >>> @dag
        ... def pipe() -> Tuple[int, float, int]:
        ...     a = unwanted_xn()
        ...     res = z(x(1), y(1))
        ...     b = unwanted_xn()
        ...     return a, res, b
        >>> composed_dag = pipe.compose("twinkle", [x, y], z)
        >>> assert composed_dag(1, 1) == 2.0
        >>> # composed_dag: DAG[[int, str], float] = pipe.compose([x, y], [z])  # optional typing of the returned DAG!
        >>> # assert composed_dag(1, 1) == 2.0  # type checked!
        ```

        Args:
            qualname (str): the name of the composed DAG
            inputs (ellipsis, Alias | List[Alias]): the Inputs nodes whose results are provided. Provide ... to specify that you will provide every argument of the original DAG.
            outputs (Alias | List[Alias]): the Output nodes that must execute last, The ones that will generate results
            is_async (bool | None): if True, the composed DAG will be an AsyncDAG, if False, it will be a DAG. Defaults to whatever the original DAG is.
            **kwargs (Dict[str, Any]): additional arguments to be passed to the DAG's constructor
        """
        # what happens for edge cases ??
        # 1. if inputs are less than sufficient to produce outputs (-> error)
        # 2. if inputs are more than sufficient to produce outputs (-> warning)
        # 3. if inputs are successors of outputs (-> error)
        # 5. if inputs are successors of inputs but predecessors of outputs
        # 1. inputs and outputs are overlapping (-> error ambiguous ? maybe not)
        # 1. if inputs & outputs are [] (-> ())
        # 4. cst cubgraph inputs is [], outputs is not [] but contains constants (-> works as expected)
        # 5. inputs is not [], outputs is [] same as 2.
        # 6. a subcase of the above (some inputs suffice to produce some of the outputs, but some inputs don't)
        # 7. advanced usage: if inputs contain ... (i.e. Ellipsis) in this case we must expand it to reach all the
        # remaining XN in a smart manner
        #  we should keep the order of the inputs and outputs (future)
        # 8. what if some arguments have default values? should they be provided by the user?
        # 9. how to specify that arguments of the original DAG should be provided by the user? the user should provide
        # the input's ID which is not a stable Alias yet

        def _alias_or_aliases_to_ids(
            alias_or_aliases: Union[Alias, Sequence[Alias]],
        ) -> list[Identifier]:
            if isinstance(alias_or_aliases, str) or isinstance(alias_or_aliases, ExecNode):
                return [self._get_single_xn_by_alias(alias_or_aliases).id]
            return [self._get_single_xn_by_alias(a_id).id for a_id in alias_or_aliases]

        def _raise_input_successor_of_input(pred: Identifier, succ: set[Identifier]) -> NoReturn:
            raise ValueError(
                f"Input ExecNodes {succ} depend on Input ExecNode {pred}."
                f"this is ambiguous. Remove either one of them."
            )

        def _raise_missing_input(input_: Identifier) -> NoReturn:
            raise ValueError(
                f"ExecNode {input_} are not declared as inputs. "
                f"Either declare them as inputs or modify the requests outputs."
            )

        def _alias_or_aliases_to_uxns(
            alias_or_aliases: Union[Alias, Sequence[Alias]],
        ) -> ReturnUXNsType:
            if isinstance(alias_or_aliases, str) or isinstance(alias_or_aliases, ExecNode):
                return UsageExecNode(self._get_single_xn_by_alias(alias_or_aliases).id)
            return tuple(
                UsageExecNode(self._get_single_xn_by_alias(a_id).id) for a_id in alias_or_aliases
            )

        # 1. get input ids and output ids.
        #  Alias should correspond to a single ExecNode,
        #  otherwise an ambiguous situation exists, raise error
        in_ids = (
            [uxn.id for uxn in self.input_uxns]
            if inputs is ...  # type: ignore[comparison-overlap]
            else _alias_or_aliases_to_ids(inputs)
        )
        out_ids = _alias_or_aliases_to_ids(outputs)

        # 2.1 contains all the ids of the nodes that will be in the new DAG
        set_xn_ids = set(in_ids + out_ids)

        # 2.2 all ancestors of the inputs
        in_ids_ancestors: set[Identifier] = self.graph_ids.ancestors_of_iter(in_ids)

        # 3. check edge cases
        # inputs should not be successors of inputs, otherwise (error)
        # and inputs should produce at least one of the outputs, otherwise (warning)
        for in_id in in_ids:
            # if pred is ancestor of an input, raise error
            if in_id in in_ids_ancestors:
                _raise_input_successor_of_input(in_id, set(in_ids))

            # if in_id doesn't produce any of the wanted outputs, raise a warning!
            descendants: set[Identifier] = nx.descendants(self.graph_ids, in_id)
            if descendants.isdisjoint(out_ids):
                warnings.warn(
                    f"Input ExecNode {in_id} is not used to produce any of the requested outputs."
                    f"Consider removing it from the inputs.",
                    stacklevel=2,
                )

        # 4. collect necessary ExecNodes' IDS

        # 4.1 original DAG's inputs that don't contain default values.
        # used to detect missing inputs
        dag_inputs_ids = [uxn.id for uxn in self.input_uxns if uxn.id not in self.results]

        # 4.2 define helper function
        def _add_missing_deps(candidate_id: Identifier, xn_ids: set[Identifier]) -> None:
            """Adds missing dependency to the set of ExecNodes that will be in the new DAG.

            Note: uses nonlocal variable dag_inputs_ids

            Args:
                candidate_id (Identifier): candidate id of an `ExecNode` that will be in the new DAG
                xn_ids (Set[Identifier]): Set of `ExecNode`s that will be in the new DAG
            """
            preds = self.graph_ids.predecessors(candidate_id)
            for pred in preds:
                if pred not in xn_ids:
                    # this candidate is necessary to produce the output,
                    # it is an input to the original DAG
                    # it is not provided as an input to the composed DAG
                    # hence the user forgot to supply it! (raise error)
                    if pred in dag_inputs_ids:
                        _raise_missing_input(pred)

                    # necessary intermediate dependency.
                    # collect it in the set
                    xn_ids.add(pred)
                    _add_missing_deps(pred, xn_ids)

        # 4.3 add all required dependencies for each output
        for o_id in out_ids:
            _add_missing_deps(o_id, set_xn_ids)

        # 5.1 copy the ExecNodes that will be in the composed DAG because
        #  maybe the composed DAG will modify them (e.g. change their tags)
        #  and we don't want to modify the original DAG
        # we avoid adding the inputs because they will be modified just afterwards
        xn_dict = StrictDict(
            (in_id, deepcopy(self.exec_nodes[in_id])) for in_id in set_xn_ids if in_id not in in_ids
        )

        # change input ExecNodes to ArgExecNodes
        new_in_ids = [make_axn_id(qualname, old_id) for old_id in in_ids]
        for old_id, new_id in zip(in_ids, new_in_ids):
            logger.debug("changing Composed-DAG's input {} into ArgExecNode", new_id)
            xn_dict[new_id] = ArgExecNode(new_id)

            # because old_id changed, we must change it to new_id in its corresponding dependencies everywhere
            for xn in xn_dict.values():
                for i, xn_dep in enumerate(xn.args):
                    # if the dependency of xn is an input_id of the newly composed DAG.
                    if xn_dep.id == old_id:
                        xn.args[i] = UsageExecNode(new_id, xn_dep.key)
                for xn_dep_name, xn_dep in xn.kwargs.items():
                    # if dependency of xn is an input_id of the newly composed DAG.
                    if xn_dep.id == old_id:
                        xn.kwargs[xn_dep_name] = UsageExecNode(new_id, xn_dep.key)

        # 5.3 make the inputs and outputs UXNs for the composed DAG
        in_uxns = [UsageExecNode(xn_id) for xn_id in new_in_ids]
        # if a single value is returned make the output a single value
        out_uxns = _alias_or_aliases_to_uxns(outputs)

        # 6. extract the results of only the remaining ExecNodes
        results = StrictDict(
            (node_id, result) for node_id, result in self.results.items() if node_id in xn_dict
        )

        # 7. return the composed DAG/AsyncDAG
        if is_async is False or (is_async is None and isinstance(self, DAG)):
            return DAG(
                qualname=qualname,
                results=results,
                exec_nodes=xn_dict,
                input_uxns=in_uxns,
                return_uxns=out_uxns,
                **kwargs,  # type: ignore[arg-type]
            )
        return AsyncDAG(
            qualname=qualname,
            results=results,
            exec_nodes=xn_dict,
            input_uxns=in_uxns,
            return_uxns=out_uxns,
            **kwargs,  # type: ignore[arg-type]
        )

    def alias_to_ids(self, alias: Alias) -> list[Identifier]:
        """Extract an ExecNode ID from an Alias (Tag, ExecNode ID or ExecNode).

        Args:
            alias (Alias): an Alias (Tag, ExecNode ID or ExecNode)

        Returns:
            The corresponding ExecNode IDs

        Raises:
            ValueError: if a requested ExecNode is not found in the DAG
            TawaziTypeError: if the Type of the identifier is not Tag, Identifier or ExecNode
        """
        if isinstance(alias, ExecNode):
            if alias.id not in self.exec_nodes:
                raise ValueError(f"ExecNode {alias} not found in DAG")
            return [alias.id]
        # todo: do further validation for the case of the tag!!
        if isinstance(alias, (Identifier, tuple)):
            # if leaves_identification is not ExecNode, it can be either
            #  1. a Tag (Highest priority in case an id with the same value exists)
            nodes = [self.exec_nodes[xn_id] for xn_id in self.graph_ids.get_tagged_nodes(alias)]
            if nodes:
                return [node.id for node in nodes]
            #  2. or a node id!
            if isinstance(alias, Identifier) and alias in self.exec_nodes:
                node = self.get_node_by_id(alias)
                return [node.id]
            raise ValueError(
                f"node or tag {alias} not found in DAG.\n"
                f" Available nodes are {self.exec_nodes}.\n"
                f" Available tags are {list(self.graph_ids.tags)}"
            )
        raise TawaziTypeError(
            "target_nodes must be of type ExecNode, "
            f"str or tuple identifying the node but provided {alias}"
        )

    def get_multiple_nodes_aliases(self, nodes: Sequence[Alias]) -> list[Identifier]:
        """Ensure correct Identifiers from aliases.

        Args:
            nodes: iterable of node aliases

        Returns:
            list of correct Identifiers
        """
        return list(chain(*(self.alias_to_ids(alias) for alias in nodes)))

    def _pre_setup(
        self,
        target_nodes: Optional[Sequence[Alias]],
        exclude_nodes: Optional[Sequence[Alias]],
        root_nodes: Optional[Sequence[Alias]],
    ) -> DiGraphEx:
        # 1. if target_nodes is not provided run all setup ExecNodes
        if target_nodes is not None:
            target_nodes = self.get_multiple_nodes_aliases(target_nodes)
        else:
            target_nodes = self.graph_ids.setup_nodes

        # 2. the leaves_ids that the user wants to execute
        if exclude_nodes is not None:
            exclude_nodes = self.get_multiple_nodes_aliases(exclude_nodes)

        if root_nodes is not None:
            root_nodes = self.get_multiple_nodes_aliases(root_nodes)

        graph = self.graph_ids.make_subgraph(
            target_nodes=target_nodes, exclude_nodes=exclude_nodes, root_nodes=root_nodes
        )

        # 3. remove non setup nodes
        graph.remove_nodes_from(
            [node_id for node_id in graph if node_id not in self.graph_ids.setup_nodes]
        )
        return graph

    def _expand_config(
        self, config_nodes: dict[Union[Tag, Identifier], Any]
    ) -> list[tuple[Identifier, Any]]:
        expanded_config = []
        for alias, conf_node in config_nodes.items():
            ids = self.alias_to_ids(alias)
            expanded_config.extend([(id, conf) for id, conf in zip(ids, len(ids) * [conf_node])])
        return expanded_config

    def config_from_dict(self, config: dict[str, Any]) -> None:
        """Allows reconfiguring the parameters of the nodes from a dictionary.

        Args:
            config (Dict[str, Any]): the dictionary containing the config
                example: {"nodes": {"a": {"priority": 3, "is_sequential": True}}, "max_concurrency": 3}

        Raises:
            ValueError: if two nodes are configured by the provided config (which is ambiguous)
        """
        if "nodes" in config:
            expanded_config = self._expand_config(config["nodes"])
            detect_duplicates(expanded_config)
            for node_id, conf_node in expanded_config:
                node = self.get_node_by_id(node_id)
                values = node._conf_to_values(conf_node)
                self.exec_nodes.force_set(node_id, type(node)(**values))

        if "max_concurrency" in config:
            self.max_concurrency = config["max_concurrency"]

        # we might have changed the priority of some nodes we need to recompute the DiGraph
        self.graph_ids = DiGraphEx.from_exec_nodes(
            input_nodes=self.input_uxns, exec_nodes=self.exec_nodes
        )

    def config_from_yaml(self, config_path: str) -> None:
        """Allows reconfiguring the parameters of the nodes from a YAML file.

        Args:
            config_path: the path to the YAML file
        """
        with open(config_path) as f:
            yaml_config = yaml.load(f, Loader=UniqueKeyLoader)  # noqa: S506

        self.config_from_dict(yaml_config)

    def config_from_json(self, config_path: str) -> None:
        """Allows reconfiguring the parameters of the nodes from a JSON file.

        Args:
            config_path: the path to the JSON file
        """
        with open(config_path) as f:
            json_config = json.load(f)

        self.config_from_dict(json_config)


@dataclass
class DAG(BaseDAG[P, RVDAG]):
    """SyncDAG implementation of the BaseDAG."""

    def executor(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
        cache_deps_of: Optional[Sequence[Alias]] = None,
        cache_in: str = "",
        from_cache: str = "",
    ) -> "DAGExecution[P, RVDAG]":
        """Generates a DAGExecution for the DAG.

        Args:
            target_nodes: the nodes to execute, excluding all nodes that can be excluded
            exclude_nodes: the nodes to exclude from the execution
            root_nodes: these nodes and their children will be included in the execution
            cache_deps_of: which nodes to cache the dependencies of
            cache_in: the path to the file where to cache
            from_cache: the cache

        Returns:
            the DAGExecution object associated with the dag
        """
        return DAGExecution(
            dag=self,
            target_nodes=target_nodes,
            exclude_nodes=exclude_nodes,
            root_nodes=root_nodes,
            cache_deps_of=cache_deps_of,
            cache_in=cache_in,
            from_cache=from_cache,
        )

    def setup(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
    ) -> None:
        """Run the setup ExecNodes for the DAG.

        If target_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: `DAG` arguments should not be passed to setup ExecNodes.
            Only pass in constants or setup `ExecNode`s results.

        Args:
            target_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to use in the DAG.
                This might include setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes.
                Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes
                and exclude_nodes is logical.
            root_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to select as ancestor nodes.
                The user is responsible for ensuring that the overlapping between the target_nodes, the exclude_nodes
                and the root nodes is logical.
        """
        graph = self._pre_setup(target_nodes, exclude_nodes, root_nodes)

        # 4. execute the graph and set the results to setup_results
        _, self.results, _ = sync_execute(
            exec_nodes=self.exec_nodes,
            results=self.results,
            max_concurrency=self.max_concurrency,
            graph=graph,
        )

    # TODO: discuss whether we want to expose it or not
    def run_subgraph(  # type: ignore[valid-type]
        self, subgraph: DiGraphEx, results: Optional[StrictDict[Identifier, Any]], *args: P.args
    ) -> tuple[
        StrictDict[Identifier, ExecNode],
        StrictDict[Identifier, Any],
        StrictDict[Identifier, Profile],
    ]:
        """Run a subgraph of the original graph (might be the same graph).

        Args:
            subgraph: the subgraph to run
            results: the results provided from the dag (containing setup) or coming from a modified DAG (from DAGExecution)
            *args: the args to pass to the graph

        Returns:
            a mapping between the execnodes and there identifiers
        """
        if results is None:
            results = extend_results_with_args(self.results, self.input_uxns, *args)
        else:
            results = extend_results_with_args(results, self.input_uxns, *args)

        exec_nodes, results, profiles = sync_execute(
            exec_nodes=self.exec_nodes,
            results=results,
            max_concurrency=self.max_concurrency,
            graph=subgraph,
        )

        # set DAG.results to the obtained value from setup ExecNodes
        for node_id, result in results.items():
            xn = self.exec_nodes[node_id]
            if xn.setup and not xn.executed(self.results):
                logger.debug("Setting result of setup ExecNode {} to {}", node_id, result)
                logger.debug("Future executions will use this result.")
                self.results[node_id] = result

        return exec_nodes, results, profiles

    def _describe_subdag(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Describe current DAG as part of a DAG."""
        logger.debug("Describing SubDAG {} in DAG", self)

        # NOTE: can't call the base describing function because composed DAGs can't be supported in that case
        #  so must modify ExecNodes of SubDAG
        node.DAG_PREFIX.append(self.qualname)

        def to_subdag_id(id_: str) -> str:
            return ".".join(node.DAG_PREFIX + [id_])

        # only the ExecNodes of the SubDAG must be affected by the is_active
        is_active = False if ARG_NAME_ACTIVATE not in kwargs else kwargs[ARG_NAME_ACTIVATE]

        input_uxns = [UsageExecNode(to_subdag_id(uxn.id), uxn.key) for uxn in self.input_uxns]

        # provided args to the subdag
        arg_uxns = construct_subdag_arg_uxns(
            *args, to_subdag_id=to_subdag_id, qualname=self.qualname
        )

        registered_input_ids: list[str] = []
        # provided *args to the call is <= than input_uxns! because of defaults args
        for axn, uxn in zip(arg_uxns, input_uxns):  # strict=False
            # a stub that fills the value of an input ExecNode with an arg of the subdag
            stub: LazyExecNode[[UsageExecNode], UsageExecNode] = LazyExecNode(
                id_=uxn.id,
                exec_function=lambda x: x,
                resource=consts.Resource.main_thread,
                args=[axn],
            )
            # register this LazyExecNode in the dict
            # pass kwargs to pass in the twz_active!
            _val: UsageExecNode = stub(axn, **kwargs)
            registered_input_ids.append(uxn.id)

        # updating ids of results already registered in the DAG due to pipeline.setup and default args
        node.results.update(
            StrictDict((to_subdag_id(id_), res) for id_, res in self.results.items())
        )

        # updating values of the ExecNodes with the new Ids only for the inputs that were changed!
        graph = DiGraphEx()
        for xn in self.exec_nodes.values():
            graph.add_exec_node(xn)
        # must go by order because Dict doesn't respect the order of insertion
        while len(graph):
            id_ = graph.remove_any_root_node()
            exec_node = self.exec_nodes[id_]
            new_id = to_subdag_id(id_)
            # input ExecNode was already registered during step for zip
            if new_id in registered_input_ids:
                logger.debug("Skipping ExecNode {} because the input is already registered", new_id)
                continue

            values = asdict(exec_node)
            values["id_"] = new_id

            values["args"] = [
                UsageExecNode(to_subdag_id(uxn.id), uxn.key) for uxn in exec_node.args
            ]
            values["kwargs"] = {
                to_subdag_id(id_): UsageExecNode(to_subdag_id(uxn.id), uxn.key)
                for id_, uxn in exec_node.kwargs.items()
            }
            if not exec_node.setup:
                if exec_node.active is not None:
                    values["active"] = UsageExecNode(
                        to_subdag_id(exec_node.active.id), exec_node.active.key
                    )

                if is_active is not False:
                    if exec_node.active is not None:
                        raise RuntimeError(
                            f"Trying to set active status for ExecNode {id_} in SubDAG {self.qualname} "
                            f"ExecNode {id_} already has an activation (twz_active) associated with it."
                            "This feature will be supported in the future."
                        )
                    values["active"] = make_active(new_id, **kwargs)

            node.exec_nodes[new_id] = type(exec_node)(**values)

        try:
            if isinstance(self.return_uxns, UsageExecNode):
                return UsageExecNode(to_subdag_id(self.return_uxns.id), self.return_uxns.key)  # type: ignore[return-value]

            if isinstance(self.return_uxns, tuple):
                return tuple(
                    UsageExecNode(to_subdag_id(uxn.id), uxn.key) for uxn in self.return_uxns  # type: ignore[return-value]
                )
            if isinstance(self.return_uxns, list):
                return [UsageExecNode(to_subdag_id(uxn.id), uxn.key) for uxn in self.return_uxns]  # type: ignore[return-value]

            if isinstance(self.return_uxns, dict):
                return {  # type: ignore[return-value]
                    k: UsageExecNode(to_subdag_id(uxn.id), uxn.key)
                    for k, uxn in self.return_uxns.items()
                }
            # TODO: allow return of None from subdag
            raise RuntimeError(
                "SubDAG must have return values as a single value, tuple, list or dict."
            )
        finally:
            node.DAG_PREFIX.pop()

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Note: Currently kwargs are not supported.

        Args:
            *args (P.args): arguments to be passed to the call of the DAG
            **kwargs (P.kwargs): keyword arguments to be passed to the call of the DAG

        Returns:
            RVDAG: return value of the DAG's execution

        Raises:
            TawaziUsageError: kwargs are passed
        """
        description_context = node.exec_nodes_lock.locked()
        # is_active is only allowed when describing a SubDAG
        if kwargs and (not description_context or set(kwargs.keys()) != {ARG_NAME_ACTIVATE}):
            raise TawaziUsageError(f"currently DAG does not support keyword arguments: {kwargs}")

        if description_context:
            return self._describe_subdag(*args, **kwargs)

        graph = self.graph_ids.extend_graph_with_debug_nodes(self.graph_ids, cfg)
        _, results, _ = self.run_subgraph(graph, None, *args)
        return get_return_values(self.return_uxns, results)  # type: ignore[return-value]


@dataclass
class AsyncDAG(BaseDAG[P, RVDAG]):
    def executor(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
        cache_deps_of: Optional[Sequence[Alias]] = None,
        cache_in: str = "",
        from_cache: str = "",
    ) -> "AsyncDAGExecution[P, RVDAG]":
        """Generates a AsyncDAGExecution for the current AsyncDAG.

        Args:
            target_nodes: the nodes to execute, excluding all nodes that can be excluded
            exclude_nodes: the nodes to exclude from the execution
            root_nodes: these nodes and their children will be included in the execution
            cache_deps_of: which nodes to cache the dependencies of
            cache_in: the path to the file where to cache
            from_cache: the cache

        Returns:
            the DAGExecution object associated with the dag
        """
        return AsyncDAGExecution(
            dag=self,
            target_nodes=target_nodes,
            exclude_nodes=exclude_nodes,
            root_nodes=root_nodes,
            cache_deps_of=cache_deps_of,
            cache_in=cache_in,
            from_cache=from_cache,
        )

    async def setup(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
    ) -> None:
        """Run the setup ExecNodes for the DAG.

        If target_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: `DAG` arguments should not be passed to setup ExecNodes.
            Only pass in constants or setup `ExecNode`s results.

        Args:
            target_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to use in the DAG.
                This might include setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes.
                Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes
                and exclude_nodes is logical.
            root_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to select as ancestor nodes.
                The user is responsible for ensuring that the overlapping between the target_nodes, the exclude_nodes
                and the root nodes is logical.
        """
        graph = self._pre_setup(target_nodes, exclude_nodes, root_nodes)

        # 4. execute the graph and set the results to setup_results
        _, self.results, _ = await async_execute(
            exec_nodes=self.exec_nodes,
            results=self.results,
            max_concurrency=self.max_concurrency,
            graph=graph,
        )
        return

    # TODO: refactor this with previous method
    async def run_subgraph(  # type: ignore[valid-type]
        self, subgraph: DiGraphEx, results: Optional[StrictDict[Identifier, Any]], *args: P.args
    ) -> tuple[
        StrictDict[Identifier, ExecNode],
        StrictDict[Identifier, Any],
        StrictDict[Identifier, Profile],
    ]:
        """Run a subgraph of the original graph (might be the same graph).

        Args:
            subgraph: the subgraph to run
            results: the results provided from the dag (containing setup) or coming from a modified DAG (from DAGExecution)
            *args: the args to pass to the graph

        Returns:
            a mapping between the execnodes and there identifiers
        """
        if results is None:
            results = extend_results_with_args(self.results, self.input_uxns, *args)
        else:
            results = extend_results_with_args(results, self.input_uxns, *args)

        exec_nodes, results, profiles = await async_execute(
            exec_nodes=self.exec_nodes,
            results=results,
            max_concurrency=self.max_concurrency,
            graph=subgraph,
        )

        # set DAG.results to the obtained value from setup ExecNodes
        for node_id, result in results.items():
            xn = self.exec_nodes[node_id]
            if xn.setup and not xn.executed(self.results):
                logger.debug(
                    "Setting result of setup ExecNode {} to {}"
                    "Future executions will use this result.",
                    node_id,
                    result,
                )
                self.results[node_id] = result

        return exec_nodes, results, profiles

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Note: Currently kwargs are not supported.

        Args:
            *args (P.args): arguments to be passed to the call of the DAG
            **kwargs (P.kwargs): keyword arguments to be passed to the call of the DAG

        Returns:
            RVDAG: return value of the DAG's execution

        Raises:
            TawaziUsageError: kwargs are passed
        """
        if kwargs:
            raise TawaziUsageError(f"currently DAG does not support keyword arguments: {kwargs}")

        graph = self.graph_ids.extend_graph_with_debug_nodes(self.graph_ids, cfg)
        _, results, _ = await self.run_subgraph(graph, None, *args)
        return get_return_values(self.return_uxns, results)  # type: ignore[return-value]


@dataclass
class BaseDAGExecution(Generic[P, RVDAG]):
    """A disposable callable instance of a DAG.

    It holds information about the last execution and is not threadsafe.

    Args:
        dag (DAG): The attached DAG.
        target_nodes (Optional[List[Alias]]): The leave ExecNodes to execute.
            If None will execute all ExecNodes.
        exclude_nodes (Optional[List[Alias]]): The leave ExecNodes to exclude.
            If None will exclude no ExecNode.
        root_nodes (Optional[List[Alias]]): The base ExecNodes that will server as ancestor for the graph.
            If None will run all ExecNodes.
        cache_deps_of (Optional[List[Alias]]): cache all the dependencies of these nodes.
            This option can not be used together with target_nodes nor exclude_nodes.
        cache_in (str):
            the path to the file where the execution should be cached.
            The path should end in `.pkl`.
            Will skip caching if `cache_in` is Falsy.
        from_cache (str):
            the path to the file where the execution should be loaded from.
            The path should end in `.pkl`.
            Will skip loading from cache if `from_cache` is Falsy.
    """

    dag: BaseDAG[P, RVDAG]
    target_nodes: Optional[Sequence[Alias]] = None
    exclude_nodes: Optional[Sequence[Alias]] = None
    root_nodes: Optional[Sequence[Alias]] = None

    # NOTE: from_cache is orthogonal to cache_in which means that if cache_in is set at the same time as from_cache.
    #  in this case the DAG will be loaded from_cache and the results will be saved again to the cache_in file.
    cache_deps_of: Optional[Sequence[Alias]] = None
    cache_in: str = ""
    from_cache: str = ""

    xn_dict: dict[Identifier, ExecNode] = field(init=False, default_factory=dict)
    executed: bool = False
    cached_nodes: list[ExecNode] = field(init=False, default_factory=list)

    profiles: dict[Identifier, Profile] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        """Dynamic construction of attributes."""
        # build the graph from cache if it exists
        if self.cache_deps_of is not None:
            if (
                self.target_nodes is not None
                or self.exclude_nodes is not None
                or self.root_nodes is not None
            ):
                raise ValueError(
                    "cache_deps_of can't be used together with target_nodes or exclude_nodes"
                )

            self.cache_deps_of = self.dag.get_multiple_nodes_aliases(self.cache_deps_of)
            graph = self.dag.graph_ids.make_subgraph(target_nodes=self.cache_deps_of)
            self.cached_nodes = list(graph.nodes)
        else:
            # clean user input
            if self.target_nodes is not None:
                self.target_nodes = self.dag.get_multiple_nodes_aliases(self.target_nodes)

            if self.exclude_nodes is not None:
                self.exclude_nodes = self.dag.get_multiple_nodes_aliases(self.exclude_nodes)

            if self.root_nodes is not None:
                self.root_nodes = self.dag.get_multiple_nodes_aliases(self.root_nodes)

            graph = self.dag.graph_ids.make_subgraph(
                target_nodes=self.target_nodes,
                exclude_nodes=self.exclude_nodes,
                root_nodes=self.root_nodes,
            )

        # add debug nodes
        self.graph = graph.extend_graph_with_debug_nodes(self.dag.graph_ids, cfg)

    @property
    def results(self) -> StrictDict[Identifier, Any]:
        """Returns the results of the previous DAGExecution.

        Before the DAG is executed, the results are the same as the underlying DAG. This also includes before/after setup.
        After Execution, the results have been enriched with all the ExecNodes' results.
        """
        if self.executed:
            return self._results
        return self.dag.results

    @results.setter
    def results(self, value: StrictDict[Identifier, Any]) -> None:
        """Set results."""
        self._results = value

    def _cache_results(self, results: dict[Identifier, Any]) -> None:
        """Cache execution results.

        We are currently only storing the results of the execution,
        so the configuration of the ExecNodes is lost
        But this it should not change between executions.
        """
        Path(self.cache_in).parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_in, "wb") as f:
            if self.cache_deps_of is not None:
                non_cacheable_ids: set[Identifier] = set()
                for aliases in self.cache_deps_of:
                    ids = self.dag.alias_to_ids(aliases)
                    non_cacheable_ids = non_cacheable_ids.union(ids)

                to_cache_results = {
                    id_: res for id_, res in results.items() if id_ not in non_cacheable_ids
                }
            else:
                to_cache_results = results
            pickle.dump(to_cache_results, f, protocol=pickle.HIGHEST_PROTOCOL, fix_imports=False)

    def _pre_call(self) -> None:
        if self.executed:
            raise TawaziUsageError("DAGExecution object has already been executed.")

        if self.from_cache:
            with open(self.from_cache, "rb") as f:
                cached_results = pickle.load(f)  # noqa: S301
            for node in self.cached_nodes:
                self.results = cached_results[node.id]

    def _post_call(self) -> RVDAG:
        # mark as executed. Important for the next step
        self.executed = True

        # 3. cache in the graph results
        if self.cache_in:
            self._cache_results(self.results)

        # 3. extract the returned value/values
        return get_return_values(self.dag.return_uxns, self.results)  # type: ignore[return-value]


@dataclass
class DAGExecution(BaseDAGExecution[P, RVDAG]):
    """Sync implementation of BaseDAGExecution."""

    dag: DAG[P, RVDAG]

    def setup(self) -> None:
        """Same thing as DAG.setup but `target_nodes` and `exclude_nodes` come from the DAGExecution's init."""
        # TODO: handle the case where cache_deps_of is provided instead of target_nodes and exclude_nodes
        #  in which case the deps_of might have a setup node themselves which should not run.
        #  This is an edge case though that is not important to handle at the current moment.
        self.dag.setup(target_nodes=self.target_nodes, exclude_nodes=self.exclude_nodes)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Call the DAG.

        Args:
            *args: positional arguments to pass in to the DAG
            **kwargs: keyword arguments to pass in to the DAG

        Raises:
            TawaziUsageError: if the DAGExecution has already been executed.

        Returns:
            RVDAG: the return value of the DAG's Execution
        """
        self._pre_call()

        # 2. Execute the scheduler
        self.xn_dict, self.results, self.profiles = self.dag.run_subgraph(
            self.graph, self.results, *args
        )

        return self._post_call()


class AsyncDAGExecution(BaseDAGExecution[P, RVDAG]):
    """Async implementation of BaseDAGExecution."""

    dag: AsyncDAG[P, RVDAG]

    async def setup(self) -> None:
        """Same thing as DAG.setup but `target_nodes` and `exclude_nodes` come from the DAGExecution's init."""
        # TODO: handle the case where cache_deps_of is provided instead of target_nodes and exclude_nodes
        #  in which case the deps_of might have a setup node themselves which should not run.
        #  This is an edge case though that is not important to handle at the current moment.
        await self.dag.setup(target_nodes=self.target_nodes, exclude_nodes=self.exclude_nodes)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Call the DAG.

        Args:
            *args: positional arguments to pass in to the DAG
            **kwargs: keyword arguments to pass in to the DAG

        Raises:
            TawaziUsageError: if the DAGExecution has already been executed.

        Returns:
            RVDAG: the return value of the DAG's Execution
        """
        self._pre_call()

        # 2. Execute the scheduler
        self.xn_dict, self.results, self.profiles = await self.dag.run_subgraph(
            self.graph, self.results, *args
        )

        return self._post_call()
