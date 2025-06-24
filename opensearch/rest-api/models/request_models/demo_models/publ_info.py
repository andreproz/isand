from pydantic import BaseModel


class PublInfoModel(BaseModel):

    id: int = 0
    p_title: str = "",
    p_annotation: str = "",
    authors: list[dict] = [],
    p_type: str = ""
