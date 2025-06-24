from search.opensearch import OpenSearch
from opensearchpy import AsyncOpenSearch
from db_connectors.opensearch_connector import OpenSearchConnector
from typing import Sequence, Mapping
import os


class DeltaSearch(OpenSearch):

    async def search_by_ids(self, ids: Sequence[str]) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index=self._opensearch_index)
        if not index_exist:
            return []

        size = len(ids)

        query = {
            "_source": ["termins"],
            "query": {
                "ids": {
                    "values": ids
                }
            }
        }

        deltas_result = {}
        try:
            response = await self._opensearch_client.search(body=query, index=self._opensearch_index, size=size)
            hits = response["hits"]["hits"]
            for hit in hits:
                id_publ = hit["_id"]
                source = hit["_source"]
                deltas_result[id_publ] = source["termins"]

        except Exception as e:
            print(e)
        return deltas_result

    @staticmethod
    def get_instance():
        opensearch_client = OpenSearchConnector.get_opensearch_client()
        opensearch_index = str(os.getenv('TEXT_SEARCH_INDEX'))
        return DeltaSearch(opensearch_index, opensearch_client)
