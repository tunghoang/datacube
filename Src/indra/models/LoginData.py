from pydantic import BaseModel

class LoginData(BaseModel):
    u: str
    p: str
