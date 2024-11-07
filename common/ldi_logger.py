import logging
import sys


class LdiLogger:
    """
    Get a logger for the app
    """

    @staticmethod
    def getlogger(logger_name: str, level: int = logging.DEBUG):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        if logger.handlers:
            # we keep adding handlers
            pass
        else:
            ch = logging.StreamHandler(sys.stderr)
            ch.setLevel(level)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger
