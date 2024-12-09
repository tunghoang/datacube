from pydantic import BaseModel
from datetime import datetime

class Dataset(BaseModel):
    product: str
    resolution: float
    frequency: str
    time: datetime
    file: File
