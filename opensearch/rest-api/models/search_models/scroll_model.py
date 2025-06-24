from pydantic import BaseModel


class ScrollModel(BaseModel):
    scroll_id: str
    search_field: str
