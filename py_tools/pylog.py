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


def get_logger(logger_name=None, logger_level="info", log_console=True, log_file=False):
    logger = logging.getLogger(logger_name)
    logger.propagate = True if not log_file else False
    if logger_level == "info":
        logger_level = logging.INFO
    else:
        logger_level = logging.DEBUG
    logger.setLevel(logger_level)
    if not logger.hasHandlers():
        if log_console:
            logger.addHandler(get_console_handler())
        if log_file:
            logger.addHandler(get_file_handler(logger_name + ".log"))
    return logger
