import json

import pytest
import yaml
from tawazi import dag, xn
from tawazi.errors import TawaziUsageError

cfg = {"nodes": {"a": {"priority": 42, "is_sequential": False}}, "max_threads_concurrency": 3}
cfg_warning = {"nodes": {"a": {"priority": 42, "is_sequential": False}}, "max_concurrency": 3}
cfg_error = {
    "nodes": {"a": {"priority": 42, "is_sequential": False}},
    "max_concurrency": 3,
    "max_threads_concurrency": 3,
}


@xn(tag="toto")
def a(cst: int) -> int:
    return cst


@xn
def b(a: int, cst: str) -> str:
    return str(a) + cst


@dag
def my_dag() -> str:
    var_a = a(1234)
    return b(var_a, "poulpe")


def test_config_from_dict() -> None:
    d = my_dag
    d.config_from_dict(cfg)

    assert d.max_threads_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential


def test_config_from_dict_warning() -> None:
    d = my_dag
    with pytest.warns(
        DeprecationWarning,
        match="max_concurrency is deprecated. Will be removed in 0.5. Use max_threads_concurrency instead",
    ):
        d.config_from_dict(cfg_warning)

    assert d.max_threads_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential


def test_config_from_dict_error() -> None:
    d = my_dag
    with pytest.raises(
        TawaziUsageError,
        match="max_concurrency and max_threads_concurrency can not be used together. Please use max_threads_concurrency only",
    ):
        d.config_from_dict(cfg_error)


def test_config_from_yaml(tmp_path: str) -> None:
    p = f"{tmp_path}/my_cfg.yaml"
    yaml_cfg = yaml.dump(cfg)
    with open(p, "w") as f:
        f.write(yaml_cfg)

    d = my_dag
    d.config_from_yaml(p)

    assert d.max_threads_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential


def test_config_from_json(tmp_path: str) -> None:
    p = f"{tmp_path}/my_cfg.yaml"
    with open(p, "w") as f:
        json.dump(cfg, f)

    d = my_dag
    d.config_from_json(p)

    assert d.max_threads_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential


def test_dup_conf_dag() -> None:
    dup_cfg = {
        "nodes": {"a": {"priority": 42, "is_sequential": False}, "toto": {"priority": 256}},
        "max_threads_concurrency": 3,
    }
    d = my_dag
    with pytest.raises(ValueError):
        d.config_from_dict(dup_cfg)


def test_conf_dag_via_tag() -> None:
    tag_cfg = {"nodes": {"toto": {"priority": 256}}, "max_threads_concurrency": 3}
    d = my_dag
    d.config_from_dict(tag_cfg)
    assert d.max_threads_concurrency == 3
    assert d.get_node_by_id("a").priority == 256
