from pydantic import BaseModel
from typing import List, Optional

class LocationData(BaseModel):
    action: str
    query: Optional[str] = None
    gid: Optional[str] = None
    level: Optional[int] = None
