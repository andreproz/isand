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
from motor.motor_asyncio import AsyncIOMotorClient  # MongoDB Async Driver

MONGODB_URL = "mongodb://root:example@193.232.208.58:27017"
MONGODB_DB_NAME = "isand"
MONGODB_COLLECTION_NAME = "authors"


# Ваша текущая реализация функций get_settings и get_mappings остаётся без изменений

async def connect_to_mongodb() -> AsyncIOMotorClient:
    # Подключение к MongoDB
    client = AsyncIOMotorClient(MONGODB_URL)
    return client


async def start():

    client = await connect_to_mongodb()
    db = client[MONGODB_DB_NAME]
    collection = db[MONGODB_COLLECTION_NAME]

    counter = 0
    chunk = 100
    with open("/var/storages/data/publications/a_fio.json", "r") as info:
        ids: List[int] = []
        objs: List[Mapping] = []
        id = 1
        data = json.load(info)
        for i in data['1'].keys():
            obj = {}
            obj["name"] = i
            # obj["author_keyword"] = i
            obj["terms"] = []
            for j in range(0, len(data.keys())):
                j_str = str(j)
                for k in data[j_str][i]:
                    term_info = {}
                    term_info["term"] = k.split(":")[-1]
                    term_info["count"] = data[j_str][i][k]
                    obj["terms"].append(term_info)
            ids.append(id)
            objs.append(obj)
            id += 1
        ids = list(map(str, (await collection.insert_many(objs)).inserted_ids))


async def main():
    await start()
asyncio.run(main=main())
