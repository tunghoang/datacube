from pydantic import BaseModel
from typing import List

class LocationData(BaseModel):
    action: str
    query: str
