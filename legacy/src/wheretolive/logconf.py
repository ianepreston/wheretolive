"""Configure logging for this project."""
import datetime as dt
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filename=Path(__file__).resolve().parents[2]
    / "data"
    / "logs"
    / f"{dt.date.today():%Y-%m-%d}_logs.txt",
    filemode="a",
)


def _console_logger() -> logging.StreamHandler:
    """Get a stdout logger."""
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    return console


def get_logger(name: str) -> logging.Logger:
    """Get a fully configured logger for the project."""
    logger = logging.getLogger(name)
    logger.addHandler(_console_logger())
    return logger
