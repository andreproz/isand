from pydantic import BaseModel
from typing import List
import os


class SearchByIDModel(BaseModel):
    id: str | int = ""
