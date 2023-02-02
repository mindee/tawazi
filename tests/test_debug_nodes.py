# type: ignore
import pytest

from tawazi import to_dag, xnode
from tawazi.errors import TawaziBaseException


@xnode
def n1(img):
    print(f"did operation on {img}")
    return img


@xnode(debug=True)
def my_len(img):
    len_img = len(img)
    pytest.my_len_has_ran = True
    return len_img


@xnode(debug=True)
def is_positive_len(len_img):
    # this node depends of the my_len!
    if len_img > 0:
        print("positive")
    else:
        print("negative")

    pytest.is_positive_len_has_ran = True


def test_pipeline_with_debug_node():

    pytest.my_len_has_ran = False
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    @to_dag
    def pipeline(img):
        img = n1(img)
        len_ = my_len(img)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert pytest.my_len_has_ran == True


def test_pipeline_without_debug_node():

    pytest.my_len_has_ran = False
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = False

    @to_dag
    def pipeline(img):
        img = n1(img)
        len_ = my_len(img)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert pytest.my_len_has_ran == False


def test_interdependant_debug_nodes():

    pytest.my_len_has_ran = False
    pytest.is_positive_len_has_ran = False
    import tawazi

    tawazi.Cfg.RUN_DEBUG_NODES = True

    @to_dag
    def pipeline(img):
        img = n1(img)
        len_ = my_len(img)
        is_positive_len(len_)
        return img

    assert [1, 2, 3] == pipeline([1, 2, 3])
    assert pytest.my_len_has_ran == True
    assert pytest.is_positive_len_has_ran == True


def test_wrongly_defined_pipeline():
    with pytest.raises(TawaziBaseException):

        @to_dag
        def pipeline(img):
            len_ = my_len(img)
            # wrongly defined dependency node!
            # a production node depends on a debug node!
            return n1(len_)
