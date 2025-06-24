from typing import Any, Sequence
from unittest import result
from fastapi import APIRouter, status, Depends
from starlette.responses import Response
from search.subsearches.demo_search import DemoSearch
from models.demo_models.demo_model import DemoModel
from models.demo_models.demo_scroll import DemoScroll
from models.demo_models.demo_get_subinfo import Subinfo
from models.demo_models.demo_thematic_model import DemoThematicModel
from models.request_models.demo_models.publ_info import PublInfoModel
router = APIRouter()


@router.post("/search")
async def scroll(info: DemoModel, search=Depends(DemoSearch.get_instance)):
    response = await search.normal_demo_search(info)
    return response


@router.post("/thematic_search")
async def scroll(info: DemoThematicModel, search=Depends(DemoSearch.get_instance)):
    response = await search.search_by_others(info)
    return response


@router.post("/scroll")
async def scroll(info: DemoScroll, search=Depends(DemoSearch.get_instance)):
    response = await search.normal_demo_scroll(info)
    return response


@router.get("/get_sections")
async def scroll(search=Depends(DemoSearch.get_instance)):
    response = await search.get_roots()
    return response


@router.post("/get_factors")
async def get_factors(info: Subinfo, search=Depends(DemoSearch.get_instance)):
    response = await search.get_factors(info, f_type="sections")
    return response


@router.post("/get_subfactors")
async def get_subfactors(info: Subinfo, search=Depends(DemoSearch.get_instance)):
    response = await search.get_factors(info, f_type="factors")
    return response


@router.post("/get_terms")
async def get_termins(info: Subinfo, search=Depends(DemoSearch.get_instance)):
    response = await search.get_factors(info, f_type="subfactors")
    return response


@router.post("/get_subset_terms")
async def get_termins(info: Subinfo, search=Depends(DemoSearch.get_instance)):
    response = {}
    try:
        if info.terms == []:
            return {"subset": []}
        if info.terms == [0]:
            response = await search.get_roots()
        else:
            response = await search.get_subset_terms(info)
    except:
        return {"subset": []}
    return response


@router.post("/thematic_searchv2")
async def scroll(info: DemoThematicModel, search=Depends(DemoSearch.get_instance)):
    if info.search_field == "publs":
        response = await search.thematic_search_publs(info)
    else:
        response = await search.thematic_search(info)
    return response


@router.get("/get_publ_info")
async def get_by_id(id: str | int, search=Depends(DemoSearch.get_instance)) -> PublInfoModel:
    result = await search.get_pub_by_id(id)
    return result


@router.get("/get_all_factors")
async def get_all_factors(search=Depends(DemoSearch.get_instance)):
    dict_sections = await search.get_sections()
    sections_names = Subinfo.model_validate({'terms': [i.get('name') for i in dict_sections.get(
        'subject_areas') if i.get('name')]})
    factors = await search.get_factors(sections_names, f_type="sections")
    return factors


@router.get("/get_all_factors")
async def get_all_factors(search=Depends(DemoSearch.get_instance)):
    dict_sections = await search.get_sections()
    sections_names = Subinfo.model_validate({'terms': [i.get('name') for i in dict_sections.get(
        'subject_areas') if i.get('name')]})
    factors = await search.get_factors(sections_names, f_type="sections")
    return factors


@router.get("/get_all_subfactors")
async def get_all_factors(search=Depends(DemoSearch.get_instance)):
    dict_sections = await search.get_sections()
    sections_names = Subinfo.model_validate({'terms': [i.get('name') for i in dict_sections.get(
        'subject_areas') if i.get('name')]})
    dict_factors = await search.get_factors(sections_names, f_type="sections")
    factors_names = Subinfo.model_validate({'terms': [i.get('name') for i in dict_factors.get(
        'factors') if i.get('name')]})
    subfactors = await search.get_factors(factors_names, f_type="factors")
    return subfactors
