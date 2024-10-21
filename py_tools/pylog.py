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

    if debug is None:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    if not logger.hasHandlers():
        if debug:
            # log to file
            log_dir = ".logs"
            if not os.path.exists(log_dir):
                os.mkdir(log_dir)
            logger.addHandler(
                get_file_handler(os.path.join(log_dir, logger_name + ".log"))
            )
            logger.propagate = False
        else:
            # log to console
            logger.propagate = True
            logger.addHandler(get_console_handler())
    return logger
