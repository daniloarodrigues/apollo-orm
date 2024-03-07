from functools import wraps
from logging import getLogger
from datetime import datetime
from os import getenv
from typing import Callable

##############################################
## logging level according logging library: ##
##   CRITICAL = 50                          ##
##   ERROR = 40                             ##
##   WARNING = 30                           ##
##   INFO = 20                              ##
##   DEBUG = 10                             ##
##   NOTSET = 0                             ##
##############################################

logging_level = getenv("LOG_LEVEL") or "WARNING"
console_log = getenv("CONSOLE_LOG") == "true"


class Logger:

    def __init__(self, name: str):
        self.name = name
        self.log = getLogger(name)
        self.log.setLevel(level=logging_level)

    def __log(self, log_type: str, log_func, message: str) -> None:
        log_message = f"{datetime.now()} [{log_type}] {self.name}: {message}"
        log_func(log_message)
        if console_log:
            print(log_message)

    def debug(self, message: str) -> None:
        self.__log(log_type="DEBUG", log_func=self.log.debug, message=message)

    def error(self, message: str) -> None:
        self.__log(log_type="ERROR", log_func=self.log.error, message=message)

    def info(self, message: str) -> None:
        self.__log(log_type="INFO", log_func=self.log.info, message=message)

    def warn(self, message: str) -> None:
        self.__log(log_type="WARN", log_func=self.log.warning, message=message)

    def step(self, begin=None, end=None) -> Callable:

        def log_info(func) -> Callable:

            @wraps(func)
            def wrapper(*args, **kwargs):
                if begin:
                    self.info(message=begin)

                result = func(*args, **kwargs)

                if end:
                    self.info(message=end)

                return result

            return wrapper

        return log_info
