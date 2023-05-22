from subprocess import run


def test_correct_env_var() -> None:
    # Real test
    cmd = "echo $(poetry env info --path)/python scripts/validate_config_options.py".split(" ")
    env = {"TAWAZI_IS_SEQUENTIAL": "False"}
    proc = run(cmd, env=env)  # noqa: S603
    assert proc.returncode == 0
