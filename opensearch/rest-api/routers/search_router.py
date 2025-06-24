from typing import Any, Sequence
from fastapi import APIRouter, status, Depends
from starlette.responses import Response
from search.subsearches.search import Search
from models.search_models.search_query import SearchQueryModel
from models.search_models.scroll_model import ScrollModel
from models.search_models.search_by_id_model import SearchByIDModel

router = APIRouter()


@router.post("/scroll")
async def scroll_searcher(scroll_info: ScrollModel, search=Depends(Search.get_instance)):
    scroll_result = await search.scroll(scroll_info)
    return scroll_result


@router.post("/")
async def search_by_phrase(search_query: SearchQueryModel, search=Depends(Search.get_instance)):
    search_result = await search.search_by_phrase(search_query)
    return search_result


@router.get("/info_by_id/{document_id}")
async def get_by_id(document_id: str | int, search=Depends(Search.get_instance)):
    id_result = await search.get_by_id(document_id)
    return id_result
