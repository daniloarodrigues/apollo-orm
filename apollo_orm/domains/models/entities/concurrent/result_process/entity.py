from datetime import datetime
from typing import Any, Optional, Dict


class ResultProcess:
    def __init__(self, message: str, query: Optional[str], params: Any,
                 processed_date: datetime = datetime.now()):
        self.message = message
        self.query = query
        self.params = params
        self.processed_date = processed_date.strftime('%Y-%m-%d %H:%M:%S:%f')
