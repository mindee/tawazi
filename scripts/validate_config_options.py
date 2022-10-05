from tawazi import Cfg


def validate() -> None:
    # Helper test not to be run by pytest because of pydantic behaviour wrt env vars
    assert not Cfg.TAWAZI_IS_SEQUENTIAL


if __name__ == "__main__":
    validate()
