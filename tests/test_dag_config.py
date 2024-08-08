import json

import pytest
import yaml
from tawazi import dag, xn

cfg = {"nodes": {"a": {"priority": 42, "is_sequential": False}}, "max_concurrency": 3}


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

    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential
    assert my_dag() == "1234poulpe"


def test_config_from_yaml(tmp_path: str) -> None:
    p = f"{tmp_path}/my_cfg.yaml"
    yaml_cfg = yaml.dump(cfg)
    with open(p, "w") as f:
        f.write(yaml_cfg)

    d = my_dag
    d.config_from_yaml(p)

    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential
    assert my_dag() == "1234poulpe"


def test_config_from_json(tmp_path: str) -> None:
    p = f"{tmp_path}/my_cfg.yaml"
    with open(p, "w") as f:
        json.dump(cfg, f)

    d = my_dag
    d.config_from_json(p)

    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential
    assert my_dag() == "1234poulpe"


def test_dup_conf_dag() -> None:
    dup_cfg = {
        "nodes": {"a": {"priority": 42, "is_sequential": False}, "toto": {"priority": 256}},
        "max_concurrency": 3,
    }
    d = my_dag
    with pytest.raises(ValueError):
        d.config_from_dict(dup_cfg)


def test_conf_dag_via_tag() -> None:
    tag_cfg = {"nodes": {"toto": {"priority": 256}}, "max_concurrency": 3}
    d = my_dag
    d.config_from_dict(tag_cfg)
    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 256


def test_with_twz_active() -> None:
    @dag
    def my_dag() -> str:
        var_a = a(1234, twz_active=True)  # type: ignore[call-arg]
        return b(var_a, "poulpe")

    my_dag.config_from_dict(
        {"nodes": {"a": {"priority": 42, "is_sequential": False}}, "max_concurrency": 3}
    )
    assert my_dag() == "1234poulpe"
