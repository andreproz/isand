from pydantic import BaseModel
from typing import List


class DemoScroll(BaseModel):
    scroll_id: str
    search_field: str
