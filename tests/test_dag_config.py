# type: ignore
import json

import yaml

from tawazi import dag, xn

cfg = {"nodes": {"a": {"priority": 42, "is_sequential": False}}, "max_concurrency": 3}


@xn
def a(cst: int):
    print(cst)
    return cst


@xn
def b(a, cst: str):
    print(a, cst)
    return str(a) + cst


@dag
def my_dag():
    var_a = a(1234)
    var_b = b(var_a, "poulpe")
    return var_b


def test_config_from_dict():
    d = my_dag
    d.config_from_dict(cfg)

    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential


def test_config_from_yaml(tmp_path):
    p = f"{tmp_path}/my_cfg.yaml"
    yaml_cfg = yaml.dump(cfg)
    with open(p, "w") as f:
        f.write(yaml_cfg)

    d = my_dag
    d.config_from_yaml(p)

    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential


def test_config_from_json(tmp_path):
    p = f"{tmp_path}/my_cfg.yaml"
    with open(p, "w") as f:
        json.dump(cfg, f)

    d = my_dag
    d.config_from_json(p)

    assert d.max_concurrency == 3
    assert d.get_node_by_id("a").priority == 42
    assert not d.get_node_by_id("a").is_sequential
