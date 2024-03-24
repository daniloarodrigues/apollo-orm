from typing import Dict, Any, List, Tuple, Optional

from cassandra.query import PreparedStatement

from apollo_orm.domains.models.entities.concurrent.result_process.entity import ResultProcess


class PreProcessedInsertData:
    def __init__(self, statements_and_params: Optional[Dict[PreparedStatement, List[Tuple[Any]]]],
                 errors: Optional[List[ResultProcess]]):
        self.statements_and_params: Optional[Dict[PreparedStatement, List[Tuple[Any]]]] = statements_and_params
        self.errors: Optional[List[ResultProcess]] = errors
