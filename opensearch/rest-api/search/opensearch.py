from typing import Any, Mapping, Sequence
from opensearchpy import AsyncOpenSearch
import json


class OpenSearch:

    def __init__(self, index: str, opensearch_client: AsyncOpenSearch):
        self._opensearch_client: AsyncOpenSearch = opensearch_client
        self._opensearch_index = index

    async def clear_collection(self):
        try:
            await self._opensearch_client.indices.delete(index=self._opensearch_index)
            await self._opensearch_client.indices.create(index=self._opensearch_index)
        except Exception as e:
            print(f"Unsuccesfull clear: {e}")
            return "Unsuccess"
        else:
            return "Success"

    async def create(self, obj_id: str, obj: Mapping[str, Any]):
        await self._opensearch_client.create(index=self._opensearch_index, id=obj_id, document=obj)

    async def create_many(self, objects_ids: list[str], objects: Sequence[Mapping[str, Any]]):
        bulk = []
        chunk_size = 10
        for i in range(len(objects)):
            index_operation = f' {{ "index" : {{ "_index": "{self._opensearch_index}", "_id": "{
                objects_ids[i]}" }} }} \n {json.dumps(objects[i])} \n'
            bulk.append(index_operation)
        chunks = [' '.join(bulk[i:i + chunk_size])
                  for i in range(0, len(bulk), chunk_size)]
        for i in range(len(chunks)):
            await self._opensearch_client.bulk(body=chunks[i])
            print(f"Chunk was added")
        print("Added to open")

    async def update(self, obj_id: str, obj):
        await self._opensearch_client.update(index=self._opensearch_index, id=obj_id, doc=dict(obj))

    async def delete(self, obj_id: str):
        await self._opensearch_client.delete(index=self._opensearch_index, id=obj_id)
