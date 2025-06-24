from search.opensearch import OpenSearch
from opensearchpy import AsyncOpenSearch
from db_connectors.opensearch_connector import OpenSearchConnector
from typing import Sequence, Mapping, List
from models.demo_models.demo_model import DemoModel
from models.demo_models.demo_scroll import DemoScroll
from models.demo_models.demo_get_subinfo import Subinfo
from models.demo_models.demo_thematic_model import DemoThematicModel
import os
import re
from models.request_models.demo_models.publ_info import PublInfoModel
from db_connectors.postgres_connector import PostgresConnector
from db_connectors.mongodb_connector import MongoDBConnection
from sql_generators.sql_query_generator import SQLQueryGenerator


class DemoSearch(OpenSearch):

    async def normal_demo_search(self, search_query: DemoModel) -> Mapping:

        if search_query.search_field == 'publ':
            search_query.search_field = 'publications'
            search_fields = os.getenv(
                "TEXT_SEARCH_FIELDS").split(',')
            source_fields = ["p_title", "authors"]
        else:
            search_fields = [search_query.search_field[:-1]]
        index_exist = await self._opensearch_client.indices.exists(index=search_query.search_field)

        if not index_exist:
            return []

        phrase = search_query.phrase
        if search_query.search_field == 'publications':
            query = self.get_pub_query(search_query)
        else:
            query = {
                "_source": [search_query.search_field[:-1]],
                "query": {
                    "multi_match": {
                        "query": phrase,
                        "type": "phrase",
                        "fields": search_fields
                    }
                },
                "sort": {
                    "_score": {
                        "order": search_query.sort_by
                    }
                }
            }
            if search_query.sort_type == "name":
                query["sort"] = {
                    f"{search_fields[0]}_keyword": {
                        "order": search_query.sort_by
                    }
                }
        scroll = "30m"
        scroll_id = ""
        total_hits = 0
        hits = []
        try:
            response = await self._opensearch_client.search(body=query, index=search_query.search_field, scroll=scroll)
            scroll_id = response["_scroll_id"]
            total_hits = response['hits']['total']['value']
            responsed_hits = response['hits']['hits']
            for responsed_hit in responsed_hits:
                responsed_hit_source = responsed_hit['_source']
                responsed_hit_id = responsed_hit['_id']
                if search_query.search_field[:-1] in responsed_hit_source or "p_title" in responsed_hit_source:
                    hit = {}
                    if "p_title" in responsed_hit_source:
                        hit["id_publ"] = responsed_hit_id
                        hit["p_title"] = re.sub(
                            r'[^\w\sа-яА-Яa-zA-Z0-9]', '', responsed_hit_source["p_title"])
                        if "authors" in responsed_hit_source:
                            # authors = [{"a_fio": re.sub(r'[^\w\sа-яА-Яa-zA-Z0-9]', '', author['a_fio'])}
                            #            for author in responsed_hit_source['authors']]
                            authors = [{"a_fio": author['a_fio']}
                                       for author in responsed_hit_source['authors']]
                            hit["authors"] = authors
                    else:
                        hit["id"] = responsed_hit_id
                        hit["name"] = responsed_hit_source[search_fields[0]]
                    hits.append(hit)
        except Exception as e:
            print(e)
            scroll_id = ""
            total_hits = 0
            hits = []
        search_result = {"scroll_id": scroll_id,
                         "total_hits": total_hits,
                         "hits": hits}
        return search_result

    def get_pub_query(self, search_query: DemoModel):
        search_fields: List[str] = os.getenv(
            "TEXT_SEARCH_FIELDS").split(',')
        nested_fields = os.getenv("TEXT_SEARCH_NESTED_FIELDS").split(',')
        result_fields = os.getenv("TEXT_SEARCH_RESULT_FIELDS").split(',')
        queries = []
        regular_fields = []
        phrase = search_query.phrase
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
                                    "fields": [field],
                                    "fuzziness": "AUTO"
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
                    "fields": regular_fields,
                    "fuzziness": "AUTO"
                }
            }
            queries.append(regular_query)

        sort_by = search_query.sort_by

        title_filter = {"exists": {"field": "p_title"}}

        query = {
            "_source": result_fields,
            "query": {
                "bool": {
                    "should": queries,
                    "filter": title_filter
                }
            },
            "sort": {"_score": sort_by}
        }
        if search_query.sort_type == "name":
            query["sort"] = [
                {"p_title_keyword": {"order": sort_by}}]
        return query

    async def normal_demo_scroll(self, scroll_info: DemoScroll) -> Mapping:
        if scroll_info.search_field == 'publ':
            search_field = "p_title"
        else:
            search_field = scroll_info.search_field[:-1]
        scroll_id = scroll_info.scroll_id
        searches = []
        scroll = "30m"
        hits = []
        # try:
        #     response = await self._opensearch_client.scroll(scroll="30m", scroll_id=scroll_id)

        #     responsed_hits = response['hits']['hits']
        #     for responsed_hit in responsed_hits:
        #         responsed_hit_source = responsed_hit['_source']
        #         responsed_hit_id = responsed_hit['_id']
        #         if scroll_info.search_field[:-1] in responsed_hit_source or "p_title" in responsed_hit_source:
        #             hit = {
        #                 "id": responsed_hit_id
        #             }

        #             if "p_title" in responsed_hit_source:
        #                 hit["name"] = re.sub(
        #                     r'[^\w\sа-яА-Яa-zA-Z0-9]', '', responsed_hit_source["p_title"])
        #                 if "authors" in responsed_hit_source:
        #                     authors = [re.sub(r'[^\w\sа-яА-Яa-zA-Z0-9]', '', author['a_fio'])
        #                                for author in responsed_hit_source['authors']]
        #                     hit["authors"] = authors
        #             else:
        #                 hit["name"] = responsed_hit_source[search_field]
        #             hits.append(hit)
        # except Exception as e:
        #     print(e)

        try:
            response = await self._opensearch_client.scroll(scroll="30m", scroll_id=scroll_id)
            responsed_hits = response['hits']['hits']
            for responsed_hit in responsed_hits:
                responsed_hit_source = responsed_hit['_source']
                responsed_hit_id = responsed_hit['_id']
                if search_field in responsed_hit_source or "p_title" in responsed_hit_source:
                    hit = {}
                    if "p_title" in responsed_hit_source:
                        hit["id_publ"] = responsed_hit_id
                        hit["p_title"] = re.sub(
                            r'[^\w\sа-яА-Яa-zA-Z0-9]', '', responsed_hit_source["p_title"])
                        if not hit["p_title"]:
                            continue
                        if "authors" in responsed_hit_source:
                            authors = [{"a_fio": re.sub(r'[^\w\sа-яА-Яa-zA-Z0-9]', '', author['a_fio'])}
                                       for author in responsed_hit_source['authors']]
                            hit["authors"] = authors
                    else:
                        hit["id"] = responsed_hit_id
                        hit["name"] = responsed_hit_source[search_field]
                    hits.append(hit)
        except Exception as e:
            print(e)
            hits = []
        search_result = {"hits": hits}
        return search_result

    async def search_by_pub(self, searchQuery: DemoModel) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index=self._opensearch_index)

        if not index_exist:
            return []

        phrase = searchQuery.phrase

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

        title_filter = {"exists": {"field": "p_title"}}

        query = {
            "_source": result_fields,
            "query": {
                "bool": {
                    "should": queries,
                    "filter": title_filter
                }
            },
            "sort": {"_score": sort_by}
        }
        if searchQuery.sort_type == "name":
            query["sort"] = [
                {"p_title": {"order": sort_by}}]
        scroll_id = ""
        total_hits = 0
        searches = []
        scroll = "30m"
        try:

            response = await self._opensearch_client.search(body=query, index=self._opensearch_index, scroll=scroll)
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

    async def scroll_by_pub(self, scroll_info: DemoScroll) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index=self._opensearch_index)

        if not index_exist:
            return []

        required_fields = os.getenv("TEXT_SEARCH_REQUIRED_FIELDS").split(',')
        search_fields: List[str] = os.getenv(
            "TEXT_SEARCH_FIELDS").split(',')

        scroll_id = scroll_info.scroll_id
        searches = []
        scroll = "30m"

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

    async def scroll_by_others(self, searchQuery: DemoScroll) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index=searchQuery.search_field)

        if not index_exist:
            return []

        scroll_id = searchQuery.scroll_id
        searches = []
        scroll = "30m"
        request = {}
        try:
            response = await self._opensearch_client.scroll(scroll="30m", scroll_id=scroll_id)
            hits = response['hits']['hits']
            request["hits"] = []
            for hit in hits:
                obj = {}
                obj["name"] = hit['_source'][searchQuery.search_field[:-1]]
                inner = hit['inner_hits']
                for level_hit in inner.keys():
                    value = inner[level_hit]['hits']['total']['value']
                    if value > 0:
                        inner_hits = inner[level_hit]['hits']['hits']
                        source = inner_hits[0]["_source"]
                        obj['termin'] = source['termin']
                        obj['count'] = int(source['count'])
                        break
                request["hits"].append(obj)
        except Exception as e:
            print(e)
        search_result = {"hits": searches}
        return search_result

    async def search_by_others(self, searchQuery: DemoModel) -> Mapping:
        index_exist = await self._opensearch_client.indices.exists(index="publications" if searchQuery.search_field == "publ" else searchQuery.search_field)
        if not index_exist:
            return []

        phrases = searchQuery.phrases
        sort_by = searchQuery.sort_by
        scroll = "30m"

        should_queries = []
        sort_queries = []
        for phrase in phrases:
            should_queries.append({
                "nested": {
                    "path": "termins",
                    "inner_hits": {
                        "_source": ["termins.termin", "termins.count"],
                        "name": f"termins_hits_{phrase}",
                        "size": 10
                    },
                    "query": {
                        "match": {"termins.termin": {"query": phrase, "fuzziness": "AUTO"}}
                    }
                }
            })
            sort_queries.append({
                "termins.count": {
                    "order": sort_by,
                    "nested": {
                        "path": "termins",
                        "filter": {
                            "match": {"termins.termin": {"query": phrase, "fuzziness": "AUTO"}}
                        }
                    }
                }
            })

        query = {
            "_source": "p_title" if searchQuery.search_field == "publ" else [searchQuery.search_field[:-1]],
            "size": 10000,
            "query": {
                "bool": {
                    "must": should_queries
                }
            },
            "sort": sort_queries
        }

        # If there's only one parameter, sort by it
        if searchQuery.sort_type == "name":
            query["sort"] = [
                {searchQuery.search_field[:-1]: {"order": sort_by}}]

        request = {}
        response = await self._opensearch_client.search(body=query, index="publications" if searchQuery.search_field == "publ" else searchQuery.search_field, scroll=scroll)
        request["scroll_id"] = response["_scroll_id"]
        request["total_hits"] = response['hits']["total"]["value"]
        hits = response['hits']['hits']
        request["hits"] = []
        # print(response)
        for hit in hits:
            obj = {}
            break
            if searchQuery.search_field == "publ" and "p_title" not in hit['_source']:
                continue
            obj["name"] = hit['_source']["p_title" if searchQuery.search_field ==
                                         "publ" else searchQuery.search_field[:-1]]
            obj['id'] = hit['_id']
            inner = hit['inner_hits']
            for level_hit in inner.keys():
                value = inner[level_hit]['hits']['total']['value']
                if value > 0:
                    inner_hits = inner[level_hit]['hits']['hits']
                    source = inner_hits[0]["_source"]
                    obj['termin'] = source['termin']
                    obj['count'] = int(source['count'])
            request["hits"].append(obj)

        return request

    async def get_sections(self):
        query = {
            "_source": ["sections.section_name", "sections.id", "sections.parent_id"],
            "query": {
                "nested": {
                    "path": "sections",
                    "query": {
                        "match_all": {}
                    },
                }
            }
        }
        scroll = "10m"
        sections = []
        try:
            response = await self._opensearch_client.search(body=query, index="thethaurus", scroll=scroll)
            hits = response.get('hits', {}).get('hits', [])
            for hit in hits:
                source = hit.get('_source', {})
                sections_data = source.get('sections', [])
                for section_data in sections_data:
                    section_info = {}
                    section_info["name"] = section_data.get('section_name', '')
                    section_info["id"] = section_data.get('id', '')
                    section_info["parent_id"] = section_data.get(
                        'parent_id', '')
                    match(section_info["name"]):
                        case "Общенаучная проблематика":
                            section_info["total_terms"] = 208
                        case "Предметная область":
                            section_info["total_terms"] = 1375
                        case "Сфера применения":
                            section_info["total_terms"] = 685
                        case "Математический аппарат":
                            section_info["total_terms"] = 1034
                        case _:
                            section_info["total_terms"] = 1
                    section_info["total_terms"]
                    sections.append(section_info)
        except Exception as e:
            print(e)
        result = {"subject_areas": sections, "total_hits": len(sections)}
        return result

    async def get_factors(self, subinfo: Subinfo, f_type: str):
        search_criteria = {
            "sections": subinfo.terms,
            "factors": subinfo.terms,
            "subfactors": subinfo.terms,
            "terms": subinfo.terms
        }

        criteria = {f_type: search_criteria.get(f_type, {})}
        query = self.generate_search_query(**criteria)
        scroll = "10m"
        result = {}
        types = {
            "sections": "factors",
            "factors": "subfactors",
            "subfactors": "terms",
        }

        f_type = types[f_type]

        try:
            response = await self._opensearch_client.search(body=query, index="thethaurus", scroll=scroll)
            result[f_type] = self.parse_response(response)
            result['total_hits'] = len(result[f_type])
        except Exception as e:
            print(e)
        return result

    def parse_response(self, response):
        hits = response.get('hits', {}).get('hits', [])
        parsed_data = []
        for hit in hits:
            inner_hits = hit.get('inner_hits', {})
            for key, inner_hit in inner_hits.items():
                hits_data = inner_hit.get('hits', {}).get('hits', [])
                for hit_data in hits_data:
                    source = hit_data.get('_source', {})
                    if 'factors' in source:
                        factors = self.parse_factors(hit_data, 'factors')
                        parsed_data.extend(factors)
                    elif 'subfactors' in source:
                        subfactors = self.parse_factors(
                            hit_data, 'subfactors')
                        parsed_data.extend(subfactors)
                    elif 'terms' in source:
                        termins = self.parse_factors(hit_data, 'terms')
                        parsed_data.extend(termins)
        return parsed_data

    def parse_factors(self, hit_data, level_type):
        factors = []
        source = hit_data.get('_source', {})

        factors_data = source.get(level_type, [])
        what_gets = f'{level_type[:-1]}_name'
        for factor_data in factors_data:
            factor_info = {}
            factor_info["name"] = factor_data.get(what_gets, '')
            factor_info["id"] = factor_data.get('id', '')
            factor_info["parent_id"] = factor_data.get('parent_id', '')
            factors.append(factor_info)
        return factors

    def generate_search_query(self, sections=None, factors=None, subfactors=None, terms=None):
        query = {
            "_source": "",
            "query": {
                "bool": {
                    "must": []
                }
            }
        }

        if sections:
            sections_query = {
                "nested": {
                    "path": "sections",
                    "query": {
                        "bool": {
                            "should": [{"match": {"sections.section_name": section}} for section in sections]
                        }
                    },
                    "inner_hits": {"name": "sections", "_source": ["sections.factors.factor_name", "sections.factors.parent_id", "sections.factors.id"]}
                }
            }
            query["query"]["bool"]["must"].append(sections_query)

        if factors:
            factors_query = {
                "nested": {
                    "path": "sections.factors",
                    "query": {
                        "bool": {
                            "should": [{"match": {"sections.factors.factor_name": factor}} for factor in factors]
                        }
                    },
                    "inner_hits": {"name": "factors", "_source": ["sections.factors.subfactors.subfactor_name", "sections.factors.subfactors.parent_id", "sections.factors.subfactors.id"]}
                }
            }
            query["query"]["bool"]["must"].append(factors_query)

        if subfactors:
            subfactors_query = {
                "nested": {
                    "path": "sections.factors.subfactors",
                    "query": {
                        "bool": {
                            "should": [{"match": {"sections.factors.subfactors.subfactor_name": subfactor}} for subfactor in subfactors]
                        }
                    },
                    "inner_hits": {"name": "subfactors", "_source": ["sections.factors.subfactors.terms.term_name", "sections.factors.subfactors.terms.parent_id", "sections.factors.subfactors.terms.id"]}
                }
            }
            query["query"]["bool"]["must"].append(subfactors_query)

        if terms:
            terms_query = {
                "nested": {
                    "path": "sections.factors.subfactors.terms",
                    "query": {
                        "bool": {
                            "should": [{"match": {"sections.factors.subfactors.terms.term_name": term}} for term in terms]
                        }
                    },
                    "inner_hits": {"name": "terms"}
                }
            }
            query["query"]["bool"]["must"].append(terms_query)

        return query

    async def thematic_search(self, searchQuery: DemoThematicModel) -> Mapping:
        if searchQuery.search_field == "publ":
            searchQuery.search_field = "publs"
        collection = await MongoDBConnection.get_collection(collection_name=searchQuery.search_field)

        print(searchQuery)
        terms = list(set(searchQuery.phrases))
        print(terms)
        query = [{"terms.term": query} for query in terms]
        print(query)
        pipeline = [
            {"$match": {
                "$and": query
            }},
            {"$unwind": "$terms"},  # Раскрываем вложенные документы
            {"$match": {"name": {"$ne": None}}},
            # Фильтруем только нужные термины
            {"$match": {"$or": query}},
            {"$group": {
                "_id": "$_id",
                "name": {"$first": "$name"},
                "total_terms": {"$sum": "$terms.count"},
                "terms": {
                    "$push": {
                        "term": "$terms.term",
                        "count": "$terms.count"
                    }
                }
            }},
            {"$sort": {"total_terms": -1}},
        ]

        aggr_result = await collection.aggregate(pipeline, allowDiskUse=True).to_list(None)
        # print(aggr_result)
        for i in aggr_result:
            # i["id"] = str(i["_id"])
            i["id"] = 0
            unique_termins = [dict(t)
                              for t in {tuple(d.items()) for d in i["terms"]}]
            i["terms"] = unique_termins
            del i["_id"]
        # print(aggr_result[0])
        request = {
            "scroll_id": "",
            "total_hits": len(aggr_result),
            "hits": aggr_result
        }

        return request

    def get_all_factors():
        pass

    async def get_roots(self):
        cursor = await PostgresConnector().get_cursor()
        sql = """
            select fgr.root_id, fnv.variant from factor_graph_roots fgr
            join factor_name_variants fnv on fgr.root_id = fnv.factor_id
            group by fgr.root_id, fnv.variant
        """
        await cursor.execute(sql)
        terms = []
        for i in await cursor.fetchall():
            term: dict = {}
            term['parent_id'] = 0
            term['id'] = i[0]
            term['name'] = i[1]
            match (term["name"]):
                case "Общенаучная проблематика":
                    term["total_terms"] = 208
                case "Предметная область":
                    term["total_terms"] = 1375
                case "Сфера применения":
                    term["total_terms"] = 685
                case "Математический аппарат":
                    term["total_terms"] = 1034
                case _:
                    term["total_terms"] = 1
            terms.append(term)
        result = {
            "subject_areas": terms, "total_hits": len(terms)
        }
        return result

    async def get_subset_terms(self, info: Subinfo):
        cursor = await PostgresConnector().get_cursor()
        print(info)
        variants = info.terms
        variants_str = ', '.join(str(v) for v in variants)
        sql = f"""
            select fge.predecessor_id, fge.successor_id, fnv.variant, factors.level from factor_graph_edges fge
            join factor_name_variants fnv on fge.successor_id = fnv.factor_id
            join factors on factors.id = fge.successor_id
            where fge.predecessor_id in ({variants_str})
            group by fge.predecessor_id, fge.successor_id, fnv.variant, factors.level
        """
        await cursor.execute(sql)
        terms = []
        sub_terms = set()
        levels = set()
        for i in await cursor.fetchall():
            term: dict = {}
            term['parent_id'] = i[0]
            term['id'] = i[1]
            term['name'] = i[2]
            if info.level == 2 and i[3] == 3:
                sub_terms.add(term['parent_id'])
                levels.add(i[3])
                continue
            terms.append(term)
        if info.level == 2 and 3 in levels:
            print(sub_terms)
            variants_str = ', '.join(str(x) for x in sub_terms)
            sql = f"""
                select fge.predecessor_id, fge.successor_id, fnv.variant from factor_name_variants fnv
                join factor_graph_edges fge on fge.successor_id = fnv.factor_id
                where fnv.factor_id in ({variants_str})
                group by fge.predecessor_id, fge.successor_id, fnv.variant
            """
            await cursor.execute(sql)
            for i in await cursor.fetchall():
                term: dict = {}
                term['parent_id'] = i[1]
                term['id'] = i[1]
                term['name'] = i[2]
                terms.append(term)
        result = {
            'subset': terms
        }
        return result

    async def thematic_search_publs(self, info: DemoThematicModel):
        cursor = await PostgresConnector().get_cursor()
        values = info.phrases
        request = {
            "scroll_id": "",
            "total_hits": 0,
            "hits": [],
            "offset": 0
        }
        if values:
            if isinstance(values[0], str):
                print("Стринга!")
                sql, count_sql = SQLQueryGenerator(
                ).generate_get_sorted_publs_from_string(values=values)
            elif isinstance(values[0], int):
                print("Инта!")
                sql, count_sql = SQLQueryGenerator().generate_get_sorted_publs(values=values)
            else:
                return request
        else:
            return request

        total_hits = (await (await cursor.execute(count_sql)).fetchone())[0]
        request["total_hits"] = total_hits

        await cursor.execute(sql)
        hits = []
        for i in await cursor.fetchall():
            hit = {}
            hit["id"] = i[0]
            hit["name"] = i[1]
            hit["total_count"] = int(i[-1])
            hit["terms"] = []
            for j in range(2, len(i)-1, 3):
                term = {}
                # term["id"] = i[j]
                term["term"] = i[j+1]
                term["count"] = int(i[j+2])
                hit["terms"].append(term)
            request["hits"].append(hit)
        return request

    async def get_pub_by_id(self, document_id: int) -> PublInfoModel:
        try:
            request = {
            }
            cursor = await PostgresConnector().get_cursor()
            sql = f"""
                SELECT p.id, p.title, pt.name FROM publications p
                LEFT JOIN publication_type pt on pt.id = p.publication_type_id
                WHERE p.id = {document_id}
            """
            await cursor.execute(query=sql)
            sql_request = await cursor.fetchone()
            if sql_request:
                request["id"] = sql_request[0]
                request["p_title"] = sql_request[1]
                if sql_request[2]:
                    request["p_type"] = sql_request[2]
            publ_info_model = PublInfoModel.model_validate(request)
        except Exception as e:
            print(e)
            publ_info_model = PublInfoModel.model_validate(request)
            return {}

        return publ_info_model

    @staticmethod
    def get_instance():
        opensearch_client = OpenSearchConnector.get_opensearch_client()
        opensearch_index = str(os.getenv('TEXT_SEARCH_INDEX'))
        return DemoSearch(opensearch_index, opensearch_client)
