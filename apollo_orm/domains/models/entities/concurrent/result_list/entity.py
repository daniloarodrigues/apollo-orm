from typing import List

from apollo_orm.domains.models.entities.concurrent.result_process.entity import ResultProcess


class ResultList:
    def __init__(self, successful: List[ResultProcess], errors: List[ResultProcess]):
        self.successful = successful
        self.errors = errors
