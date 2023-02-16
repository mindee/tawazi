# type: ignore
import math
import os
from copy import deepcopy
from time import sleep, time

import numpy as np
import pytest

import tawazi
from tawazi import dag, xn
from tawazi.errors import TawaziBaseException


# helper function for testing
def closeness(a, b):
    return abs(a - b)


def almost_equal(a, b, level=0.02):
    return closeness(a, b) < level


@xn
def sleeper(t):
    sleep(t)
    return t


@xn
def worker(t):
    t0 = time()
    counter = 0
    while time() - t0 < t:
        counter += 1

    return counter


@xn(setup=True)
def setop():
    return np.random.randint(0, 256, size=2**20)  # 16MB


@dag
def pipe(t):
    return sleeper(t), worker(t), setop()


def test_profiler_basic():
    tawazi.Cfg.TAWAZI_PROFILE_ALL_NODES = True

    pipe_ = deepcopy(pipe)
    exec_pipe = pipe_.executor()
    exec_pipe(0.1)

    profile = exec_pipe.get_node_by_id("sleeper").profile
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)

    profile = exec_pipe.get_node_by_id("worker").profile
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.05
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.05)

    profile = exec_pipe.get_node_by_id("setop").profile
    assert not almost_equal(profile.abs_exec_time, 0, 0.002)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.002
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.002)


def test_profiler_setop():
    tawazi.Cfg.TAWAZI_PROFILE_ALL_NODES = True
    pipe_ = deepcopy(pipe)
    exec_pipe = pipe_.executor()
    exec_pipe.setup()

    exec_pipe(0.1)

    profile = exec_pipe.get_node_by_id("sleeper").profile
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)

    profile = exec_pipe.get_node_by_id("worker").profile
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.05
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.05)

    profile = exec_pipe.get_node_by_id("setop").profile
    assert almost_equal(profile.abs_exec_time, 0)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)


def test_profiler_setop():
    tawazi.Cfg.TAWAZI_PROFILE_ALL_NODES = True
    pipe_ = deepcopy(pipe)
    exec_pipe = pipe_.executor()
    exec_pipe.setup()

    exec_pipe(0.1)

    profile = exec_pipe.get_node_by_id("sleeper").profile
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)

    profile = exec_pipe.get_node_by_id("worker").profile
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.05
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.05)

    profile = exec_pipe.get_node_by_id("setop").profile
    assert almost_equal(profile.abs_exec_time, 0)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)
