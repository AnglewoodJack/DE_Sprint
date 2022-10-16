import datetime
from pathlib import Path

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s %(name)-30s %(module)-30s %(funcName)-15s %(levelname)-8s %(message)s"},
    },
    "handlers": {
        "file": {
            "level": "DEBUG",
            "formatter": "default",
            "class": "logging.FileHandler",
            "filename": ''.join(
                [
                    str(Path.cwd().parent / 'logs') + '/',
                    datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                    '.log'
                ]
            ),
            "encoding": "utf-8",
        },
        "console": {
            "level": "INFO",
            "formatter": "default",
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "": {
            "handlers": ["file", "console"],
            "level": "DEBUG",
            "propagate": False
        },
    },
}
