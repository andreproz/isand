from typing import Any, Sequence
from fastapi import APIRouter, status, Depends
from starlette.responses import Response
from search.subsearches.deltas_search import DeltaSearch
from models.deltas_models.deltas_by_ids import DeltasByIDsModel

router = APIRouter()


@router.post("/deltas_by_id")
async def search_by_phrase(ids_info: DeltasByIDsModel, search=Depends(DeltaSearch.get_instance)):
    ids = ids_info.ids
    search_result = await search.search_by_ids(ids)
    return search_result
