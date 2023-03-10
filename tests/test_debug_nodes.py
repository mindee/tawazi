# type: ignore # noqa: PGH003
from typing import Any, List

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziBaseException


@xn
def stub(img: List[Any]) -> List[Any]:
    return img


@xn(debug=True)
def my_len(img: List[Any]) -> int:
    len_img = len(img)
    pytest.my_len_has_ran = True
    return len_img


@xn(debug=True)
def is_positive_len(len_img: int) -> None:
    # this node depends of the my_len!
    if len_img > 0:
        print("positive")  # noqa: T201
    else:
        print("negative")  # noqa: T201

    pytest.is_positive_len_has_ran = True


def test_pipeline_with_debug_node() -> None:
    pytest.my_len_has_ran = False
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    @dag
    def pipeline(img: List[Any]) -> List[Any]:
        img = stub(img)
        my_len(img)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert pytest.my_len_has_ran is True


def test_pipeline_without_debug_node() -> None:
    pytest.my_len_has_ran = False
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = False

    @dag
    def pipeline(img: List[Any]) -> List[Any]:
        img = stub(img)
        my_len(img)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert pytest.my_len_has_ran is False


def test_interdependant_debug_nodes() -> None:
    pytest.my_len_has_ran = False
    pytest.is_positive_len_has_ran = False
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    @dag
    def pipeline(img):
        img = stub(img)
        len_ = my_len(img)
        is_positive_len(len_)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert pytest.my_len_has_ran is True
    assert pytest.is_positive_len_has_ran is True


def test_wrongly_defined_pipeline() -> None:
    with pytest.raises(TawaziBaseException):

        @dag
        def pipeline(img: List[Any]) -> List[Any]:
            len_ = my_len(img)
            # wrongly defined dependency node!
            # a production node depends on a debug node!
            return stub(len_)


@xn(debug=True)
def print_(in1: Any) -> None:
    pytest.prin_share_var = in1


@xn(debug=True)
def incr(in1: int) -> int:
    res = in1 + 1
    pytest.inc_shared_var = res
    return res


@dag
def triple_incr_debug(in1: int) -> int:
    return incr(incr(incr(in1, twz_tag="1st")))


def test_triple_incr_debug() -> None:
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    assert triple_incr_debug(1) == 4


def test_triple_incr_no_debug() -> None:
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = False

    assert triple_incr_debug(1) is None


def test_triple_incr_debug_subgraph() -> None:
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    # by only running a single dependency all subsequent debug nodes shall run
    exec_ = triple_incr_debug.executor(target_nodes=["1st"])
    assert exec_(1) == 4


def test_reachable_debuggable_node_in_subgraph() -> None:
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    @dag
    def pipe(in1):
        res1 = stub(in1)
        res2 = incr(res1)
        print_(res2)
        return res1

    exec_ = pipe.executor(target_nodes=["stub"])
    assert exec_(2) == 2
    assert pytest.prin_share_var == 3

    exec_ = pipe.executor(target_nodes=["stub", "incr"])
    assert exec_(0) == 0
    assert pytest.prin_share_var == 1


# should this be True ???
# def test_return_debug_node_value_should_raise_error():
# pass
