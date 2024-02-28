from apollo_orm.utils.logger.logger import Logger


class CommonBaseException(Exception):

    logger = Logger("CommonBaseException")

    def __init__(self, message=None, log=None, *args, **kwargs) -> None:

        log_name = kwargs.get("class_name", None)

        if log_name:
            self.logger.name = log_name

        if message:
            if log:
                if log == "error":
                    self.logger.error(message)
                elif log == "warn":
                    self.logger.warn(message)
                elif log == "info":
                    self.logger.info(message)
                elif log == "debug":
                    self.logger.debug(message)

            super().__init__(message)

        else:
            super().__init__()

    def __str__(self):
        if not self.args:
            return f"{self.__class__.__name__}"

        str_args = ', '.join([str(a) for a in self.args])

        return f"{self.__class__.__name__}: {str_args}"
