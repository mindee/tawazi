from subprocess import run

"""Integration Test"""


def test_correct_env_var() -> None:
    # Real test
    proc = run(
        "echo $(poetry env info --path)/python scripts/validate_config_options.py".split(" "),
        env={"TAWAZI_IS_SEQUENTIAL": "False"},
    )
    assert proc.returncode == 0
