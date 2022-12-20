from pydantic import BaseSettings, Field


class Config(BaseSettings):
    TAWAZI_IS_SEQUENTIAL: bool = Field(False, env="TAWAZI_IS_SEQUENTIAL")

    # Logger settings
    LOGURU_LEVEL: str = Field("CRITICAL", env="TAWAZI_LOGGER_LEVEL")
    LOGURU_BACKTRACE: bool = Field(False, env="TAWAZI_LOGGER_BT")
    # Caution: to set to False if used in prod (exposes variable names)
    LOGURU_DIAGNOSE: bool = Field(False, env="TAWAZI_LOGGER_DIAGNOSE")


Cfg = Config()
