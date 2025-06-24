from pydantic import BaseModel
from typing import List
import os


class DeltasByIDsModel(BaseModel):
    ids: List[str] | List[int] = []
