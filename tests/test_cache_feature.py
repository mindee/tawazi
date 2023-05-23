import os
import pickle
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pytest
from tawazi import DAGExecution, dag, xn
from tawazi.consts import NoVal
from tawazi.node.node import ExecNode


@xn
def generate_large_zeros_array() -> Any:
    return np.zeros(1_000_000).astype(np.uint8)


@xn
def incr_large_array(array: Any) -> Any:
    return array + 1


@xn
def pass_large_array(array: Any) -> Any:
    return array


@xn
def avg_array(array: Any) -> Any:
    return np.mean(array)


@dag
def pipe() -> Tuple[Any, Any, Any, Any]:
    zeros = generate_large_zeros_array()
    ones = incr_large_array(zeros)
    ones_ = pass_large_array(ones)
    avg = avg_array(ones)

    return zeros, ones, ones_, avg


def test_cache_results_basic() -> None:
    cache_path = "tests/cache_results/test_cache_results_basic.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecution(pipe, cache_in=cache_path)

    zeros = np.zeros(10**6)
    ones = np.ones(10**6)
    ones_ = ones
    avg = 1

    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert (r3 == ones_).all()
    assert r4 == avg

    # notice how the size of the cached value is only 2MB because ones and ones_ are the same object
    # hence only zeros and ones take space with avg and ones_ taking negligeable space

    with open(cache_path, "rb") as f:
        cached_results = pickle.load(f)  # noqa: S301
    assert (cached_results["generate_large_zeros_array"] == zeros).all()
    assert (cached_results["incr_large_array"] == ones).all()
    assert (cached_results["pass_large_array"] == ones_).all()
    assert cached_results["avg_array"] == avg


def test_cache_results_subgraph() -> None:
    cache_path = "tests/cache_results/test_cache_results_subgraph.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecution(
        pipe, target_nodes=[generate_large_zeros_array, incr_large_array], cache_in=cache_path
    )
    zeros = np.zeros(10**6)
    ones = np.ones(10**6)
    ones_ = NoVal
    avg = NoVal

    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert r3 is None
    assert r4 is None

    with open(cache_path, "rb") as f:
        cached_results = pickle.load(f)  # noqa: S301
    assert (cached_results["generate_large_zeros_array"] == zeros).all()
    assert (cached_results["incr_large_array"] == ones).all()
    assert cached_results["pass_large_array"] is ones_
    assert cached_results["avg_array"] is avg


def test_running_cached_dag() -> None:
    cache_path = "tests/cache_results/test_running_cached_dag.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecution(
        pipe, target_nodes=[generate_large_zeros_array, incr_large_array], cache_in=cache_path
    )
    zeros = np.zeros(10**6)
    ones = np.ones(10**6)
    ones_ = ones
    avg = 1

    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert r3 is None
    assert r4 is None

    exc_cached = DAGExecution(pipe, from_cache=cache_path)
    r1, r2, r3, r4 = exc_cached()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert (r3 == ones_).all()
    assert r4 == avg


def test_cache_read_write() -> None:
    cache_path = "tests/cache_results/test_cache_read_write.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecution(pipe, target_nodes=[generate_large_zeros_array], cache_in=cache_path)
    zeros = np.zeros(10**6)
    ones = np.ones(10**6)
    ones_ = ones
    avg = 1

    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert r2 is None
    assert r3 is None
    assert r4 is None

    exc = DAGExecution(
        pipe, target_nodes=[generate_large_zeros_array, incr_large_array], cache_in=cache_path
    )
    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert r3 is None
    assert r4 is None

    exc = DAGExecution(
        pipe,
        target_nodes=[generate_large_zeros_array, incr_large_array, pass_large_array],
        cache_in=cache_path,
    )
    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert (r3 == ones_).all()
    assert r4 is None

    exc = DAGExecution(
        pipe,
        target_nodes=[generate_large_zeros_array, incr_large_array, pass_large_array, avg_array],
        cache_in=cache_path,
    )
    r1, r2, r3, r4 = exc()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert (r3 == ones_).all()
    assert r4 == avg


def load_cached_results(cache_path: str) -> Any:
    with open(cache_path, "rb") as f:
        return pickle.load(f)  # noqa: S301


def test_cache_in_deps_cache_deps_of_same_as_exclude_nodes() -> None:
    with pytest.raises(ValueError):
        _ = DAGExecution(
            pipe,
            cache_deps_of=[generate_large_zeros_array],
            exclude_nodes=[generate_large_zeros_array],
        )


def test_target_nodes_non_existing() -> None:
    with pytest.raises(ValueError):
        _ = DAGExecution(
            pipe, cache_deps_of=[generate_large_zeros_array], target_nodes=["twinkle toes"]
        )


def validate(_cache_path: str, _cache_deps_of: List[Any]) -> None:
    cached_results = load_cached_results(_cache_path)
    for xn_ in _cache_deps_of:
        assert cached_results.get(xn_.id) is None


@pytest.fixture
def res() -> Tuple[Any, Any, Any, Any]:
    zeros = np.zeros(10**6)
    ones = np.ones(10**6)
    ones_ = ones
    avg = 1
    return zeros, ones, ones_, avg


@pytest.fixture
def cache_path(request: Any) -> str:
    cache_path = (
        f"tests/cache_results/test_cache_in_dpes-{request.node.get_closest_marker('suffix')}.pkl"
    )
    if Path(cache_path).is_file():
        os.remove(cache_path)
    return cache_path


@pytest.mark.suffix(1)
def test_cache_in_deps1(cache_path: str, res: Tuple[Any, ...]) -> None:
    # case 1 node
    cache_deps_of = [generate_large_zeros_array]
    exc = DAGExecution(pipe, cache_deps_of=cache_deps_of, cache_in=cache_path)
    r1, r2, r3, r4 = exc()

    assert (r1 == res[0]).all()

    validate(cache_path, cache_deps_of)


@pytest.mark.suffix(2)
def test_cache_in_deps2(cache_path: str, res: Tuple[Any, ...]) -> None:
    # case 2 nodes
    cache_deps_of: List[ExecNode] = [generate_large_zeros_array, incr_large_array]
    exc = DAGExecution(pipe, cache_deps_of=cache_deps_of, cache_in=cache_path)
    r1, r2, r3, r4 = exc()

    assert (r1 == res[0]).all()
    assert (r2 == res[1]).all()

    validate(cache_path, cache_deps_of)


@pytest.mark.suffix(3)
def test_cache_in_deps3(cache_path: str, res: Tuple[Any, ...]) -> None:
    # case 3 nodes
    cache_deps_of: List[ExecNode] = [generate_large_zeros_array, incr_large_array, pass_large_array]
    exc = DAGExecution(pipe, cache_deps_of=cache_deps_of, cache_in=cache_path)
    r1, r2, r3, r4 = exc()

    assert (r1 == res[0]).all()
    assert (r2 == res[1]).all()
    assert (r3 == res[2]).all()

    validate(cache_path, cache_deps_of)


@pytest.mark.suffix(4)
def test_cache_in_deps4(cache_path: str, res: Tuple[Any, ...]) -> None:
    # case 4 nodes
    cache_deps_of: List[ExecNode] = [
        generate_large_zeros_array,
        incr_large_array,
        pass_large_array,
        avg_array,
    ]
    exc = DAGExecution(pipe, cache_deps_of=cache_deps_of, cache_in=cache_path)
    r1, r2, r3, r4 = exc()

    assert (r1 == res[0]).all()
    assert (r2 == res[1]).all()
    assert (r3 == res[2]).all()
    assert (r4 == res[3]).all()

    validate(cache_path, cache_deps_of)
