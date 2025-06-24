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
            "sections": {
                "type": "nested",
                "properties": {
                    # Добавление поля для id
                    "id": {"type": "integer"},
                    "section_name": {"type": "keyword"},
                    "parent_id": {"type": "integer"},
                    "term_count": {"type": "integer"},
                    "factors": {
                        "type": "nested",
                        "properties": {
                            # Добавление поля для id
                            "id": {"type": "integer"},
                            "factor_name": {"type": "keyword"},
                            # Добавление поля для parent_id
                            "parent_id": {"type": "integer"},
                            "subfactors": {
                                "type": "nested",
                                "properties": {
                                    # Добавление поля для id
                                    "id": {"type": "integer"},
                                    "subfactor_name": {"type": "keyword"},
                                    # Добавление поля для parent_id
                                    "parent_id": {"type": "integer"},
                                    "terms": {
                                        "type": "nested",
                                        "properties": {
                                            # Добавление поля для id
                                            "id": {"type": "integer"},
                                            "term_name": {"type": "keyword"},
                                            "translations": {"type": "keyword"},
                                            # Добавление поля для parent_id
                                            "parent_id": {"type": "integer"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return mappings

    return mappings


async def connect_to_opensearch() -> OpenSearch:
    open_search_index = "thethaurus"
    await OpenSearchConnector.connect_and_init_opensearch()
    open_search_client: AsyncOpenSearch = OpenSearchConnector.get_opensearch_client()
    indices = (await open_search_client.indices.get_alias()).keys()
    if open_search_index not in indices:
        index_body = get_index_body()
        await open_search_client.indices.create(index=open_search_index, body=index_body)
    open_search = OpenSearch(
        index=open_search_index, opensearch_client=open_search_client)
    return open_search_client


def extract_terms(text):
    parts = text.split(';')
    russian_term = parts[-1].strip()
    english_term = parts[0].strip()
    return russian_term, english_term


async def start():

    open_search = await connect_to_opensearch()
    print(open_search)
    with open("/home/baozorp/projects/opensearch/opensearchfirstload/thesaurus.json", "r") as data:
        json_data = json.load(data)
        id_counter = 1
        sections = []

        for section_name, factors in json_data.items():
            section_id = id_counter
            id_counter += 1
            factors_list = []
            term_counter = 0  # Счетчик терминов для секции

            for factor_name, subfactors in factors.items():
                factor_id = id_counter
                id_counter += 1
                subfactors_list = []

                for subfactor_name, terms_list in subfactors.items():
                    subfactor_id = id_counter
                    id_counter += 1
                    terms = []

                    for term in terms_list:
                        russian_term, english_term = extract_terms(term)
                        term_id = id_counter
                        id_counter += 1
                        terms.append({
                            "id": term_id,
                            "term_name": russian_term,
                            "translations": english_term,
                            "parent_id": subfactor_id
                        })
                        term_counter += 1  # Увеличиваем счетчик терминов для секции

                    subfactors_list.append({
                        "id": subfactor_id,
                        "subfactor_name": subfactor_name,
                        "terms": terms,
                        "parent_id": factor_id
                    })

                factors_list.append({
                    "id": factor_id,
                    "factor_name": factor_name,
                    "subfactors": subfactors_list,
                    "parent_id": section_id
                })

            sections.append({
                "id": section_id,
                "section_name": section_name,
                "factors": factors_list,
                "parent_id": 0,
                "term_count": term_counter  # Добавляем счетчик терминов для секции
            })

        doc = {
            "sections": sections
        }
        await open_search.index(index='thethaurus', id=1, body=doc)
    await OpenSearchConnector.close_opensearch_connect()


async def main():
    await start()
asyncio.run(main=main())
