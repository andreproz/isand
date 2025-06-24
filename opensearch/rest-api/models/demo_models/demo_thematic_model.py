from pydantic import BaseModel
from typing import List


class DemoThematicModel(BaseModel):
    phrases: List[str] | List[int]
    sort_by: str = "desc"
    sort_type: str = "relevance"
    search_field: str = ""
