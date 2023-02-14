# type: ignore
import os
import pickle
from pathlib import Path

import numpy as np

from tawazi import DAGExecutor, dag, xn
from tawazi.consts import NoVal


@xn
def generate_large_zeros_array():
    return np.zeros(1_000_000).astype(np.uint8)


@xn
def incr_large_array(array):
    return array + 1


@xn
def pass_large_array(array):
    return array


@xn
def avg_array(array):
    return np.mean(array)


@dag
def pipe():
    zeros = generate_large_zeros_array()
    ones = incr_large_array(zeros)
    ones_ = pass_large_array(ones)
    avg = avg_array(ones)

    return zeros, ones, ones_, avg


def test_cache_results_basic():
    cache_path = "tests/cache_results/test_cache_results_basic.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecutor(pipe, cache_in=cache_path)

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
        cached_results = pickle.load(f)
    assert (cached_results["generate_large_zeros_array"] == zeros).all()
    assert (cached_results["incr_large_array"] == ones).all()
    assert (cached_results["pass_large_array"] == ones_).all()
    assert cached_results["avg_array"] == avg


def test_cache_results_subgraph():
    cache_path = "tests/cache_results/test_cache_results_subgraph.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecutor(
        pipe, twz_nodes=["generate_large_zeros_array", "incr_large_array"], cache_in=cache_path
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
        cached_results = pickle.load(f)
    assert (cached_results["generate_large_zeros_array"] == zeros).all()
    assert (cached_results["incr_large_array"] == ones).all()
    assert cached_results["pass_large_array"] is ones_
    assert cached_results["avg_array"] is avg


def test_running_cached_dag():
    cache_path = "tests/cache_results/test_running_cached_dag.pkl"
    if Path(cache_path).is_file():
        os.remove(cache_path)

    exc = DAGExecutor(
        pipe, twz_nodes=["generate_large_zeros_array", "incr_large_array"], cache_in=cache_path
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

    exc_cached = DAGExecutor(pipe, from_cache=cache_path)
    r1, r2, r3, r4 = exc_cached()
    assert (r1 == zeros).all()
    assert (r2 == ones).all()
    assert (r3 == ones_).all()
    assert r4 == avg
