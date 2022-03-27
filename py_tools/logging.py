import logging
import sys
from logging.handlers import TimedRotatingFileHandler

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(name):
    file_handler = TimedRotatingFileHandler(name, when="midnight")
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name, logger_level=logging.DEBUG):
    logger = logging.getLogger(logger_name)
    if not logger.hasHandlers():
        logger.setLevel(logger_level)
        # logger.addHandler(get_console_handler())
        logger.addHandler(get_file_handler(logger_name + ".log"))
        logger.propagate = False
    return logger
