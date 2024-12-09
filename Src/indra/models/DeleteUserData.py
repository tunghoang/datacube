from pydantic import BaseModel
from typing import List

class DeleteUserData(BaseModel):
    userIds: List[int]
