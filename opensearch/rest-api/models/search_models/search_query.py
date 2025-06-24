from pydantic import BaseModel
from typing import List
import os


class SearchQueryModel(BaseModel):
    phrase: str
    sort_by: str = "desc"
    sort_type: str = "relevance"
    search_fields: List[str] = os.getenv(
        "TEXT_SEARCH_FIELDS").split(',')
    p_type: List[str] = []
