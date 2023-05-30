from typing import Any, List, TypeVar

import pytest
from tawazi import dag, xn
from tawazi.errors import TawaziBaseException

my_len_has_ran = False
is_positive_len_has_ran = False
print_share_var = None
inc_shared_var = 0

T = TypeVar("T")


@xn
def stub(img: T) -> T:
    return img


@xn(debug=True)
def my_len(img: List[Any]) -> int:
    len_img = len(img)
    global my_len_has_ran
    my_len_has_ran = True
    return len_img  # noqa: RET504


@xn(debug=True)
def is_positive_len(len_img: int) -> None:
    global is_positive_len_has_ran
    # this node depends of the my_len!
    if len_img > 0:
        print("positive")  # noqa: T201
    else:
        print("negative")  # noqa: T201

    is_positive_len_has_ran = True


def test_pipeline_with_debug_node() -> None:
    global my_len_has_ran
    my_len_has_ran = False
    import tawazi

    tawazi.config.cfg.RUN_DEBUG_NODES = True

    @dag
    def pipeline(img: List[Any]) -> List[Any]:
        img = stub(img)
        my_len(img)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert my_len_has_ran is True


def test_pipeline_without_debug_node() -> None:
    global my_len_has_ran
    my_len_has_ran = False
    import tawazi

    tawazi.config.cfg.RUN_DEBUG_NODES = False

    @dag
    def pipeline(img: List[Any]) -> List[Any]:
        img = stub(img)
        my_len(img)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert my_len_has_ran is False


def test_interdependant_debug_nodes() -> None:
    global my_len_has_ran, is_positive_len_has_ran
    my_len_has_ran = False
    is_positive_len_has_ran = False
    import tawazi

    tawazi.config.cfg.RUN_DEBUG_NODES = True

    @dag
    def pipeline(img: List[Any]) -> List[Any]:
        img = stub(img)
        len_ = my_len(img)
        is_positive_len(len_)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert my_len_has_ran is True
    assert is_positive_len_has_ran is True


def test_wrongly_defined_pipeline() -> None:
    with pytest.raises(TawaziBaseException):

        @dag
        def pipeline(img: List[Any]) -> int:
            len_ = my_len(img)
            # wrongly defined dependency node!
            # a production node depends on a debug node!
            return stub(len_)


@xn(debug=True)
def print_(in1: Any) -> None:
    global print_share_var
    print_share_var = in1


@xn(debug=True)
def incr(in1: int) -> int:
    res = in1 + 1
    global inc_shared_var
    inc_shared_var += 1
    return res  # noqa: RET504


@dag
def triple_incr_debug(in1: int) -> int:
    return incr(incr(incr(in1, twz_tag="1st")))  # type: ignore[call-arg]


def test_triple_incr_debug() -> None:
    import tawazi

    global inc_shared_var
    inc_shared_var = 0
    tawazi.config.cfg.RUN_DEBUG_NODES = True

    assert triple_incr_debug(1) == 4
    assert inc_shared_var == 3


def test_triple_incr_no_debug() -> None:
    import tawazi

    tawazi.config.cfg.RUN_DEBUG_NODES = False

    assert triple_incr_debug(1) is None


def test_triple_incr_debug_subgraph() -> None:
    import tawazi

    tawazi.config.cfg.RUN_DEBUG_NODES = True

    # by only running a single dependency all subsequent debug nodes shall run
    exec_ = triple_incr_debug.executor(target_nodes=["1st"])
    assert exec_(1) == 4


def test_reachable_debuggable_node_in_subgraph() -> None:
    import tawazi

    global print_share_var, inc_shared_var
    tawazi.config.cfg.RUN_DEBUG_NODES = True
    inc_shared_var = 0

    @dag
    def pipe(in1: int) -> int:
        res1 = stub(in1)
        res2 = incr(res1)
        print_(res2)
        return res1

    exec_ = pipe.executor(target_nodes=["stub"])
    assert exec_(2) == 2
    assert print_share_var == 3
    assert inc_shared_var == 1

    inc_shared_var = 0

    exec_ = pipe.executor(target_nodes=["stub", "incr"])
    assert exec_(0) == 0
    assert print_share_var == 1
    assert inc_shared_var == 1


# should this be True ???
# def test_return_debug_node_value_should_raise_error():
# pass
