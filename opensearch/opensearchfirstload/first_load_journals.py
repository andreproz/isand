import psycopg2
from opensearch_handler.opensearch import OpenSearch
from db_connectors.opensearch_connector import OpenSearchConnector
from db_connectors.postgres_connector import PostgresConnector
from opensearchpy import AsyncOpenSearch
import asyncio
import requests
import json
import glob
from typing import Sequence, Mapping, Any, List


def get_index_body() -> dict[str, Any]:
    index_body: Mapping[str, Any] = {}
    settings = get_settings()
    mappings = get_mappings()
    index_body["settings"] = settings
    index_body["mappings"] = mappings
    return index_body


def get_settings():
    settings = {
        "index": {
            "number_of_replicas": 0
        },
        "analysis": {
            "analyzer": {
                "author_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "edge_ngram_filter"]
                }
            },
            "filter": {
                "edge_ngram_filter": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20
                }
            }
        }
    }
    return settings


def get_mappings():
    mappings = {
        "properties": {
            "journal": {
                "type": "text",
                "analyzer": "author_analyzer",
                "fielddata": "true",
            },
            "journal_keyword": {
                "type": "keyword"
            },
            "termins": {
                "type": "nested",
                "properties": {
                    "termin": {"type": "keyword"},
                    "count": {"type": "integer"}
                }
            }
        }
    }
    return mappings


async def connect_to_opensearch() -> OpenSearch:
    open_search_index = "journals"
    await OpenSearchConnector.connect_and_init_opensearch()
    open_search_client: AsyncOpenSearch = OpenSearchConnector.get_opensearch_client()
    indices = (await open_search_client.indices.get_alias()).keys()
    if open_search_index not in indices:
        index_body = get_index_body()
        await open_search_client.indices.create(index=open_search_index, body=index_body)
    open_search = OpenSearch(
        index=open_search_index, opensearch_client=open_search_client)
    return open_search


async def start():

    # Подключение к opensearch

    open_search = await connect_to_opensearch()

    counter = 0
    chunk = 100
    with open("/var/storages/data/publications/journal_name.json", "r") as info:
        ids: List[int] = []
        objs: List[Mapping] = []
        id = 1
        data = json.load(info)
        for i in data['1'].keys():
            obj = {}
            obj["journal"] = i
            obj["journal_keyword"] = i
            obj["termins"] = []
            for j in range(len(data.keys())):
                j_str = str(j)
                for k in data[j_str][i]:
                    term_info = {}
                    term_info["termin"] = k.split(":")[-1]
                    term_info["count"] = data[j_str][i][k]
                    obj["termins"].append(term_info)
            ids.append(id)
            objs.append(obj)
            id += 1
        await open_search.create_many(objects_ids=ids, objects=objs)
    await OpenSearchConnector.close_opensearch_connect()


async def main():
    await start()
asyncio.run(main=main())
