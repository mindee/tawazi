from copy import deepcopy
from time import sleep, time
from typing import Any

import numpy as np
import tawazi
from tawazi import dag, xn


def closeness(a: float, b: float) -> float:
    return abs(a - b)


def almost_equal(a: float, b: float, level: float = 0.02) -> bool:
    return closeness(a, b) < level


@xn
def sleeper(t: float) -> float:
    sleep(t)
    return t


@xn
def worker(t: float) -> float:
    t0 = time()
    counter = 0
    while time() - t0 < t:
        counter += 1

    return counter


@xn(setup=True)
def setop() -> Any:
    return np.random.randint(0, 256, size=2**20)  # 16MB


@dag
def pipe(t: float) -> tuple[float, float, Any]:
    return sleeper(t), worker(t), setop()


def test_profiler_basic() -> None:
    tawazi.config.cfg.TAWAZI_PROFILE_ALL_NODES = True

    pipe_ = deepcopy(pipe)
    exec_pipe = pipe_.executor()
    exec_pipe(0.1)

    profile = exec_pipe.profiles["sleeper"]
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)

    profile = exec_pipe.profiles["worker"]
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.05
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.05)

    profile = exec_pipe.profiles["setop"]
    assert not almost_equal(profile.abs_exec_time, 0, 0.002)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.002
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.002)


def test_profiler_setop() -> None:
    tawazi.config.cfg.TAWAZI_PROFILE_ALL_NODES = True
    pipe_ = deepcopy(pipe)
    exec_pipe = pipe_.executor()
    exec_pipe.setup()

    exec_pipe(0.1)

    profile = exec_pipe.profiles["sleeper"]
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert almost_equal(profile.process_exec_time, 0)
    assert almost_equal(profile.thread_exec_time, 0)

    profile = exec_pipe.profiles["worker"]
    assert almost_equal(profile.abs_exec_time, 0.1)
    assert not almost_equal(
        profile.process_exec_time, 0, 0.05
    )  # make sur that it doesn't equal 0 by a large margin
    assert not almost_equal(profile.thread_exec_time, 0, 0.05)

    assert "setop" not in exec_pipe.profiles
