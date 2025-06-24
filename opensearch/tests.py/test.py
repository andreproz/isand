# from db_connectors.postgres_connector import PostgresConnector
import requests
# connector = PostgresConnector(db_name="account_db")
# cursor = connector.get_cursor()
# sql = """
# select factors.id, factor_name_variants.variant from factor_graph_roots
# join factors ON factors.id = factor_graph_roots.root_id
# join factor_name_variants on factors.id = factor_name_variants.factor_id
# """
# cursor.execute(sql)
# sections = []
# for section_data in cursor.fetchall():
#     section_info = {}
#     section_info["name"] = section_data[1]
#     section_info["id"] = section_data[0]
#     section_info["parent_id"] = 0
#     match(section_info["id"]):
#         case 51341:
#             section_info["total_terms"] = 208
#         case 51551:
#             section_info["total_terms"] = 1375
#         case 53009:
#             section_info["total_terms"] = 685
#         case 53748:
#             section_info["total_terms"] = 1034
#         case _:
#             section_info["total_terms"] = 1
#     section_info["total_terms"]
#     sections.append(section_info)
# result = {"subject_areas": sections, "total_hits": len(sections)}
# print(result)
# print(requests.get("http://localhost:9199/search-api/demo/get_sections").json())
# data = {
#     "phrases": [1, 211, 1540, 2223],
#     "search_field": "publs"
# }
# print(requests.post(
#     "https://kb-isand.ipu.ru/search-api/demo/thematic_searchv2", json=data).json())
# data = {
#     "terms": [1, 211, 1540, 2223],
#     "level": 1
# }
# print(requests.post(
#     "http://localhost:9000/search-api/demo/get_subset_terms", json=data).json())
print(requests.get(
    "http://localhost:9000/search-api/demo/get_publ_info?id=12").json())
