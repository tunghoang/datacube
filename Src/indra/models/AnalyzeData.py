from pydantic import BaseModel
from typing import List, Optional

class AnalyzeData(BaseModel):
    product: str
    gid: str
    level: int
    fromDate: str
    to: str
