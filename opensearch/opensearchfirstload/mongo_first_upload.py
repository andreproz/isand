import asyncio
import json
from typing import List, Sequence, Mapping, Any
from db_connectors.postgres_connector import PostgresConnector
from motor.motor_asyncio import AsyncIOMotorClient  # MongoDB Async Driver
from opensearchpy import AsyncOpenSearch
import glob
import psycopg2
from opensearch_handler.opensearch import OpenSearch
from db_connectors.opensearch_connector import OpenSearchConnector

import requests
import copy
# Дополнительно импортируйте модули для работы с MongoDB
# Не забудьте установить библиотеку pymongo и motor: pip install pymongo motor

# Добавьте конфигурацию для подключения к MongoDB
MONGODB_URL = "mongodb://root:example@193.232.208.58:27017"
MONGODB_DB_NAME = "isand"
MONGODB_COLLECTION_NAME = "publs"


# Ваша текущая реализация функций get_settings и get_mappings остаётся без изменений

async def connect_to_mongodb() -> AsyncIOMotorClient:
    # Подключение к MongoDB
    client = AsyncIOMotorClient(MONGODB_URL)
    return client


async def start():

    # Подключение к mongodb
    client = await connect_to_mongodb()
    db = client[MONGODB_DB_NAME]
    collection = db[MONGODB_COLLECTION_NAME]

    # Подключение к opensearch
    # open_search = await connect_to_opensearch()

    # Подключение к postgres
    postgres_client = PostgresConnector()
    cursor1:  psycopg2.extensions.cursor = postgres_client.get_cursor()
    cursor2:  psycopg2.extensions.cursor = postgres_client.get_cursor()

    sql_query_for_texts = "SELECT id_publ, publ_text FROM publ_text"
    cursor2.execute(sql_query_for_texts)
    counter = 0
    chunk = 100

    sql_query_for_id = "SELECT path FROM publication WHERE id_publ="
    while True:
        ids: Sequence[str] = []
        objects: Sequence[dict] = []
        postgres_chunk: Sequence[tuple] = cursor2.fetchmany(chunk)
        if not postgres_chunk:
            break
        for i in postgres_chunk:

            obj = {}
            text = i[1]
            id = i[0]
            cursor1.execute(sql_query_for_id+str(id))
            path = cursor1.fetchone()[0]
            segmentated = glob.glob(path + "/*segmentated.json")[0]
            if not segmentated:
                continue
            with open(segmentated, 'r') as segmentates:
                data = json.load(segmentates)
                if "publications" in data and data["publications"] and "publication" in data["publications"][0]:
                    publication = data["publications"][0]["publication"]
                    if publication["p_text"]:
                        obj['p_text'] = publication["p_text"]
                    elif publication["p_text_add"]:
                        obj['p_text'] = publication["p_text_add"]
                    if publication["p_annotation"]:
                        obj['p_annotation'] = publication["p_annotation"]
                    elif publication["p_annotation_add"]:
                        obj['p_annotation'] = publication["p_annotation_add"]
                    if publication["p_title"]:
                        obj['name'] = publication["p_title"]
                    elif publication["p_title_add"]:
                        obj['name'] = publication["p_title_add"]
                    if publication["p_lang"]:
                        obj['language'] = publication["p_lang"]
                    elif publication["p_lang_add"]:
                        obj['language'] = publication["p_lang_add"]
                    if publication["authors"]:
                        obj["authors"] = []
                        for author in publication["authors"]:
                            if "author" in author:
                                if "a_fio" in author["author"]:
                                    if author["author"]["a_fio"]:
                                        obj["authors"].append(
                                            {"a_fio": author["author"]["a_fio"]})
                                if obj["authors"] and author["author"]["a_affiliations"] != []:
                                    obj["authors"][-1]["a_aff_org_name"] = []
                                    for affiliation in author["author"]["a_affiliations"]:
                                        if "a_affiliation" in affiliation:
                                            if affiliation["a_affiliation"]["a_aff_raw"]:
                                                obj["authors"][-1]["a_aff_org_name"].append(
                                                    affiliation["a_affiliation"]["a_aff_raw"])

                                    if not obj["authors"][-1]["a_aff_org_name"]:
                                        del obj["authors"][-1]["a_aff_org_name"]
                        if not obj["authors"]:
                            del obj["authors"]
            deltas_list = glob.glob(path + "/*deltas.json")
            termins = []
            if deltas_list:
                deltas = deltas_list[0]
                with open(deltas, 'r') as terms:
                    data = json.load(terms)
                    if data:
                        termins = [{"term": term, "count": count}
                                   for term, count in data.items()]
            for i in range(1, 4):
                deltas_list = glob.glob(path + f"/*tree_delta_{i}.json")
                if deltas_list:
                    deltas = deltas_list[0]
                    with open(deltas, 'r') as terms:
                        data = json.load(terms)
                        for term, count in data.items():
                            termins.append(
                                {"term": term.split(":")[-1], "count": count})
            if termins:
                obj["terms"] = termins
            objects.append(obj)
        mongo_db_objects = copy.deepcopy(objects)
        ids = list(map(str, (await collection.insert_many(mongo_db_objects)).inserted_ids))
        # await open_search.create_many(objects_ids=ids, objects=objects)
        counter += chunk
        print(counter)
    postgres_client.close_connection()
    # mongodb_client.close()
    # await OpenSearchConnector.close_opensearch_connect()


async def main():
    await start()
asyncio.run(main=main())
