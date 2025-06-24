from search.opensearch import OpenSearch
from opensearchpy import AsyncOpenSearch
from db_connectors.opensearch_connector import OpenSearchConnector
import os
import asyncio
from typing import Mapping, List
from models.search_models.search_query import SearchQueryModel
from models.search_models.scroll_model import ScrollModel
from models.search_models.search_by_id_model import SearchByIDModel


class Search(OpenSearch):

    def __init__(self, index: str, opensearch_client: AsyncOpenSearch):
        super().__init__(index, opensearch_client)

    async def search_by_phrase(self, searchQuery: SearchQueryModel) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index=self._opensearch_index)

        if not index_exist:
            return []

        phrase = searchQuery.phrase

        search_fields = searchQuery.search_fields
        if not search_fields:
            search_fields: List[str] = os.getenv(
                "TEXT_SEARCH_FIELDS").split(',')
        result_fields = os.getenv("TEXT_SEARCH_RESULT_FIELDS").split(',')
        required_fields = os.getenv("TEXT_SEARCH_REQUIRED_FIELDS").split(',')
        nested_fields = os.getenv("TEXT_SEARCH_NESTED_FIELDS").split(',')
        # Создаем список запросов для полей "authors" и остальных полей
        queries = []
        regular_fields = []
        for field in search_fields:
            if field in nested_fields:
                splitted_field = field.split('.')
                if len(splitted_field) == 2:
                    nested_query = {
                        "nested": {
                            "path": splitted_field[0],
                            "query": {
                                "multi_match": {
                                    "query": phrase,
                                    "fields": [field]
                                }
                            }
                        }
                    }
                    queries.append(nested_query)
            else:
                regular_fields.append(field)
        if regular_fields:
            regular_query = {
                "multi_match": {
                    "query": phrase,
                    "fields": regular_fields
                }
            }
            queries.append(regular_query)

        sort_by = searchQuery.sort_by
        query = {
            "_source": result_fields,
            "query": {
                "bool": {
                    "should": queries
                }
            },
            "sort": {"_score": sort_by}
        }

        scroll_id = ""
        total_hits = 0
        searches = []

        try:

            response = await self._opensearch_client.search(body=query, index=self._opensearch_index, scroll="10m")
            hits = response["hits"]["hits"]
            scroll_id = response["_scroll_id"]
            total_hits = response["hits"]["total"]["value"]

            for hit in hits:
                info = {}
                source = hit["_source"]
                for search_field in required_fields:
                    if search_field not in source:
                        break
                else:
                    source['id_publ'] = hit['_id']
                    searches.append(source)

        except Exception as e:
            print(e)
        search_result = {"scroll_id": scroll_id,
                         "total_hits": total_hits,
                         "hits": searches}
        return search_result

    async def scroll(self, scroll_info: ScrollModel) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index=self._opensearch_index)

        if not index_exist:
            return []

        required_fields = os.getenv("TEXT_SEARCH_REQUIRED_FIELDS").split(',')
        scroll_id = scroll_info.scroll_id
        searches = []

        try:
            response = await self._opensearch_client.scroll(scroll="30m", scroll_id=scroll_id)

            hits = response["hits"]["hits"]

            for hit in hits:
                info = {}
                source = hit["_source"]
                for search_field in required_fields:
                    if search_field not in source:
                        break
                else:
                    source['id_publ'] = hit['_id']  # только для публикаций
                    searches.append(source)
        except Exception as e:
            print(e)
        search_result = {"hits": searches}
        return search_result

    async def get_by_id(self, document_id: str):
        index_exist = await self._opensearch_client.indices.exists(index=self._opensearch_index)
        if not index_exist:
            return []

        required_fields = os.getenv("TEXT_SEARCH_REQUIRED_FIELDS").split(',')

        search_result = {}

        try:
            response = await self._opensearch_client.get(index=self._opensearch_index, id=document_id)
            source = response["_source"]
            fields = ["p_title", "p_annotation", "authors"]
            for field in fields:
                if field in source:
                    search_result[field] = source[field]

        except Exception as e:
            print(e)
        print(search_result)
        return search_result

    @staticmethod
    def get_instance():
        opensearch_client = OpenSearchConnector.get_opensearch_client()
        opensearch_index = str(os.getenv('TEXT_SEARCH_INDEX'))
        return Search(opensearch_index, opensearch_client)
