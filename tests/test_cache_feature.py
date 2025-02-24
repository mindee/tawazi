import os
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from tawazi import DAGExecution, dag, xn
from tawazi.node import ExecNode


def conditional_assert(result: Any, expected: Any) -> None:
    if expected is not None:
        assert np.array_equal(result, expected)
    else:
        assert result is None


def validate(_cache_path: str, _cache_deps_of: list[Any]) -> None:
    with open(_cache_path, "rb") as f:
        cached_results = pickle.load(f)  # noqa: S301
    for xn_ in _cache_deps_of:
        assert cached_results.get(xn_.id) is None


@pytest.fixture
def zeros() -> Any:
    return np.zeros(10**6)


@pytest.fixture
def ones() -> Any:
    return np.ones(10**6)


@pytest.fixture
def avg() -> int:
    return 1


@pytest.fixture
def cache_path(request: Any) -> str:
    cache_path = (
        f"tests/cache_results/test_cache_in_deps-{request.node.get_closest_marker('suffix')}.pkl"
    )
    if Path(cache_path).is_file():
        os.remove(cache_path)
    return cache_path


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
def pipe() -> tuple[Any, Any, Any, Any]:
    zeros_res = generate_large_zeros_array()
    ones_res = incr_large_array(zeros_res)
    ones_res2 = pass_large_array(ones_res)
    avg_res = avg_array(ones_res)

    return zeros_res, ones_res, ones_res2, avg_res


def test_cache_results_basic(cache_path: str, zeros: Any, ones: Any, avg: Any) -> None:
    exc = DAGExecution(pipe, cache_in=cache_path)
    _ = exc()

    # notice how the size of the cached value is only 2MB because ones and ones_ are the same object
    # hence only zeros and ones take space with avg and ones_ taking minimal space

    with open(cache_path, "rb") as f:
        cached_results = pickle.load(f)  # noqa: S301
    assert np.array_equal(cached_results["generate_large_zeros_array"], zeros)
    assert np.array_equal(cached_results["incr_large_array"], ones)
    assert np.array_equal(cached_results["pass_large_array"], ones)
    assert cached_results["avg_array"] == avg


def test_cache_results_subgraph(cache_path: str, zeros: Any, ones: Any) -> None:
    exc = DAGExecution(
        pipe, target_nodes=[generate_large_zeros_array, incr_large_array], cache_in=cache_path
    )

    _ = exc()

    with open(cache_path, "rb") as f:
        cached_results = pickle.load(f)  # noqa: S301
    assert np.array_equal(cached_results["generate_large_zeros_array"], zeros)
    assert np.array_equal(cached_results["incr_large_array"], ones)
    assert "pass_large_array" not in cached_results
    assert "avg_array" not in cached_results


def test_running_cached_dag(cache_path: str, zeros: Any, ones: Any, avg: Any) -> None:
    exc = DAGExecution(
        pipe, target_nodes=[generate_large_zeros_array, incr_large_array], cache_in=cache_path
    )
    _ = exc()

    exc_cached = DAGExecution(pipe, from_cache=cache_path)
    r1, r2, r3, r4 = exc_cached()
    assert np.array_equal(r1, zeros)
    assert np.array_equal(r2, ones)
    assert np.array_equal(r3, ones)
    assert r4 == avg


@pytest.mark.parametrize(
    "target_nodes, expected_results",
    [
        ([generate_large_zeros_array], (np.zeros(10**6), None, None, None)),
        (
            [generate_large_zeros_array, incr_large_array],
            (np.zeros(10**6), np.ones(10**6), None, None),
        ),
        (
            [generate_large_zeros_array, incr_large_array, pass_large_array],
            (np.zeros(10**6), np.ones(10**6), np.ones(10**6), None),
        ),
        (
            [generate_large_zeros_array, incr_large_array, pass_large_array, avg_array],
            (np.zeros(10**6), np.ones(10**6), np.ones(10**6), 1),
        ),
    ],
)
def test_cache_read_write(target_nodes: list[ExecNode], expected_results: list[Any]) -> None:
    cache_path = "tests/cache_results/test_cache_read_write.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    # with target nodes
    exc = DAGExecution(pipe, target_nodes=target_nodes, cache_in=cache_path)
    r1, r2, r3, r4 = exc()
    conditional_assert(r1, expected_results[0])
    conditional_assert(r2, expected_results[1])
    conditional_assert(r3, expected_results[2])
    assert r4 == expected_results[3]

    # with cache_deps_of
    exc2 = DAGExecution(pipe, cache_deps_of=target_nodes, cache_in=cache_path)
    r11, r21, r31, r41 = exc2()
    conditional_assert(r11, expected_results[0])
    conditional_assert(r21, expected_results[1])
    conditional_assert(r31, expected_results[2])
    assert r41 == expected_results[3]


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
