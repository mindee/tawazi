# type: ignore
from subprocess import run


def test_correct_env_var():
    # Real test
    proc = run(
        "echo $(poetry env info --path)/python scripts/validate_config_options.py".split(" "),
        env={"TAWAZI_IS_SEQUENTIAL": "False"},
    )
    assert proc.returncode == 0
