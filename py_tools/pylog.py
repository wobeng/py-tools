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
    debug = os.environ.get("DEBUG", None)
    logger = logging.getLogger(logger_name)
    logger.propagate = True

    if debug is None:
        # log to console
        logger.setLevel(logging.INFO)
        logger.addHandler(get_console_handler())
    else:
        # log to file
        logger.setLevel(logging.DEBUG)
        log_dir = ".logs"
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        logger.addHandler(get_file_handler(os.path.join(log_dir, logger_name + ".log")))
    logger.info("Logger %s initialized with debug: %s", logger_name, debug)
    return logger
