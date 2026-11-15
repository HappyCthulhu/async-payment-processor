import sys

from loguru import logger


def configure_logging() -> None:
    """Send application logs to stdout without exception locals or diagnostic dumps."""
    logger.remove()
    logger.add(sys.stdout, format="{time:YYYY-MM-DDTHH:mm:ss.SSSZZ} {level} {message}", diagnose=False)
