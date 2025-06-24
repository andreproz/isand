from pydantic import BaseModel
from typing import List
import os


class Subinfo(BaseModel):
    terms: List[str] | List[int]
    level: int
