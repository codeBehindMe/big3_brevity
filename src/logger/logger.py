import logging
from logging import Logger
from typing import NewType, Optional, Union

AppLogger = NewType("AppLogger", logging.Logger)

_app_logger: Union[AppLogger, None] = None


def get_or_create_logger(name: Optional[str] = "BIG3BREVITY") -> AppLogger:
    """
    Creates a custom logger with the specified name and format.

    Args:
    - name (str): The name of the logger.

    Returns:
    - logging.Logger: The configured logger.
    """

    global _app_logger

    if _app_logger is not None:
        return _app_logger
    if not name:
        raise ValueError("cannot create logger without name")
    # Create a logger
    logger = logging.getLogger(name)

    # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    logger.setLevel(logging.DEBUG)

    # Define the logging format
    formatter = logging.Formatter("%(asctime)s::%(name)s::%(levelname)s::%(message)s")

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)

    _app_logger = logger

    return _app_logger
