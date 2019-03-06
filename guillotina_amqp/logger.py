import logging
import sys


def getLogger(name):
    logger = logging.getLogger(name)
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %H:%M:%S'
    )
    stdout_hdlr.setFormatter(fmt)
    logger.addHandler(stdout_hdlr)
    return logger
