import psycopg2
from opensearch_handler.opensearch import OpenSearch
from db_connectors.opensearch_connector import OpenSearchConnector
from db_connectors.postgres_connector import PostgresConnector
from opensearchpy import AsyncOpenSearch
import asyncio
import requests
import json
import glob
from typing import Sequence, Mapping, Any


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
            "number_of_replicas": 0,
            "analysis": {
                "filter": {
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    },
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "edge_ngram_filter": {
                        "type": "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 20
                    }
                },
                "analyzer": {
                    "rebuilt_analyzer": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "russian_stop",
                            "russian_stemmer",
                            "english_stop",
                            "english_stemmer"
                        ]
                    },
                    "author_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "edge_ngram_filter"
                        ]
                    }
                }
            }
        }
    }

    return settings


def get_mappings():
    mappings = {
        "properties": {
            "p_title": {
                "type": "text",
                "analyzer": "author_analyzer",
                "fielddata": "true",
            },
            "p_title_keyword": {
                "type": "keyword",
            },
            "p_annotation": {
                "type": "text",
                "analyzer": "rebuilt_analyzer"
            },
            "p_text": {
                "type": "text",
                "analyzer": "rebuilt_analyzer"
            },
            "geo": {
                "type": "text",
                "analyzer": "rebuilt_analyzer"
            },
            "p_lang": {
                "type": "keyword",
            },
            "termins": {
                "type": "nested",
                "properties": {
                    "termin": {"type": "keyword"},
                    "count": {"type": "integer"}
                }
            },
            "authors": {
                "type": "nested",
                "properties": {
                    "a_fio": {"type": "text"},
                    "a_aff_org_name": {"type": "text"}
                }
            }
        }
    }
    return mappings


async def connect_to_opensearch() -> OpenSearch:
    open_search_index = "publications"
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
            try:
                obj = {}
                id = i[0]
                text = i[1]
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
                            obj['p_title'] = publication["p_title"]
                            obj['p_title_keyword'] = publication["p_title"]
                        elif publication["p_title_add"]:
                            obj['p_title'] = publication["p_title_add"]
                            obj['p_title_keyword'] = publication["p_title_add"]
                        if publication["p_lang"]:
                            obj['language'] = publication["p_lang"]
                        elif publication["p_lang_add"]:
                            obj['language'] = publication["p_lang_add"]
                        if publication["authors"]:
                            obj["authors"] = []
                            for author in publication["authors"]:
                                if author["a_fio"]:
                                    obj["authors"].append(
                                        {"a_fio": author["a_fio"]})
                                if obj["authors"] and author["a_affiliations"] != []:
                                    obj["authors"][-1]["a_aff_org_name"] = []
                                    for affiliation in author["a_affiliations"]:
                                        if "a_affiliation" in affiliation:
                                            if affiliation["a_affiliation"]["a_aff_org_name"]:
                                                obj["authors"][-1]["a_aff_org_name"].append(
                                                    affiliation["a_affiliation"]["a_aff_org_name"])

                                    if not obj["authors"][-1]["a_aff_org_name"]:
                                        del obj["authors"][-1]["a_aff_org_name"]
                            if not obj["authors"]:
                                del obj["authors"]
                deltas = glob.glob(path + "/*deltas.json")[0]
                termins = []
                if deltas:
                    with open(deltas, 'r') as terms:
                        data = json.load(terms)
                        if data:
                            termins = [{"termin": term, "count": count}
                                       for term, count in data.items()]
                for i in range(1, 4):
                    deltas = glob.glob(path + f"/*tree_delta_{i}.json")[0]
                    if deltas:
                        with open(deltas, 'r') as terms:
                            data = json.load(terms)
                            for term, count in data.items():
                                termins.append(
                                    {"termin": term.split(":")[-1], "count": count})
                if termins:
                    obj["termins"] = termins
                ids.append(id)
                objects.append(obj)
            except Exception as e:
                print(f"Unexpected error with prnd_id: {e}, {path}")
        await open_search.create_many(objects_ids=ids, objects=objects)
        counter += chunk
        print(counter)
    print(objects[0])
    postgres_client.close_connection()
    await OpenSearchConnector.close_opensearch_connect()


async def main():
    await start()
asyncio.run(main=main())
