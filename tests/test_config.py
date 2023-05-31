from subprocess import run


def test_tawazi_is_sequential_true() -> None:
    cmd = "TAWAZI_IS_SEQUENTIAL=True python -c 'from tawazi.config import cfg; assert cfg.TAWAZI_IS_SEQUENTIAL == True'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_is_sequential_false() -> None:
    cmd = "TAWAZI_IS_SEQUENTIAL=False python -c 'from tawazi.config import cfg; assert cfg.TAWAZI_IS_SEQUENTIAL == False'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_is_sequential_wrong() -> None:
    cmd = "TAWAZI_IS_SEQUENTIAL=twinkle python -c 'from tawazi.config import cfg; assert cfg.TAWAZI_IS_SEQUENTIAL == False'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 1


def test_tawazi_profile_all_nodes_true() -> None:
    cmd = "TAWAZI_PROFILE_ALL_NODES=True python -c 'from tawazi.config import cfg; assert cfg.TAWAZI_PROFILE_ALL_NODES == True'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_profile_all_nodes_false() -> None:
    cmd = "TAWAZI_PROFILE_ALL_NODES=False python -c 'from tawazi.config import cfg; assert cfg.TAWAZI_PROFILE_ALL_NODES == False'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_profile_all_nodes_wrong() -> None:
    cmd = "TAWAZI_PROFILE_ALL_NODES=twinkle python -c 'from tawazi.config import cfg; assert cfg.TAWAZI_PROFILE_ALL_NODES == False'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 1


def test_run_debug_nodes_true() -> None:
    cmd = "RUN_DEBUG_NODES=True python -c 'from tawazi.config import cfg; assert cfg.RUN_DEBUG_NODES == True'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_run_debug_nodes_false() -> None:
    cmd = "RUN_DEBUG_NODES=False python -c 'from tawazi.config import cfg; assert cfg.RUN_DEBUG_NODES == False'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_execnode_outside_dag_behavior_error() -> None:
    cmd = "TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR=error python -c 'from tawazi.config import cfg; from tawazi.consts import XNOutsideDAGCall; assert cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.error'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_execnode_outside_dag_behavior_warn() -> None:
    cmd = "TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR=warning python -c 'from tawazi.config import cfg; from tawazi.consts import XNOutsideDAGCall; assert cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.warning'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_execnode_outside_dag_behavior_ignore() -> None:
    cmd = "TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR=ignore python -c 'from tawazi.config import cfg; from tawazi.consts import XNOutsideDAGCall; assert cfg.TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR == XNOutsideDAGCall.ignore'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_execnode_outside_dag_behavior_wrong() -> None:
    cmd = "TAWAZI_EXECNODE_OUTSIDE_DAG_BEHAVIOR=twinkle python -c 'from tawazi.config import cfg'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 1


def test_tawazi_default_resource_thread() -> None:
    cmd = "TAWAZI_DEFAULT_RESOURCE=thread python -c 'from tawazi.config import cfg; from tawazi.consts import Resource; assert cfg.TAWAZI_DEFAULT_RESOURCE == Resource.thread'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_default_resource_main_thread() -> None:
    cmd = "TAWAZI_DEFAULT_RESOURCE=main-thread python -c 'from tawazi.config import cfg; from tawazi.consts import Resource; assert cfg.TAWAZI_DEFAULT_RESOURCE == Resource.main_thread'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 0


def test_tawazi_default_resource_wrong() -> None:
    cmd = "TAWAZI_DEFAULT_RESOURCE=twinkle python -c 'from tawazi.config import cfg'"
    proc = run(cmd, shell=True)  # noqa: S602, S603
    assert proc.returncode == 1
