from pydantic import BaseModel
from typing import List
import os


class DemoModel(BaseModel):
    phrase: str
    sort_by: str = "desc"
    sort_type: str = "relevance"
    search_field: str = ""
