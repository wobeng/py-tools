import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(name):
    file_handler = TimedRotatingFileHandler(name, when="midnight")
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name=None):
    debug = os.environ.get("DEBUG", default="false").lower() == "true"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    had_handlers = logger.hasHandlers()

    # Remove existing handlers to avoid duplicate logs
    logger.handlers.clear()

    # Disable log propagation to avoid passing logs to root logger
    logger.propagate = False

    # Add the appropriate handler based on the debug setting
    if not debug:
        logger.addHandler(get_console_handler())
    else:
        # Log to file when debug is True
        log_dir = ".logs"
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        logger.addHandler(get_file_handler(os.path.join(log_dir, f"{logger_name}.log")))

    if not had_handlers:
        logger.info("Logger %s initialized with debug: %s", logger_name, debug)

    return logger
