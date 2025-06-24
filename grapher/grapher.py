from fastapi import FastAPI, Query, Request, HTTPException
import networkx as nx
import numpy as np
import itertools
from collections import Counter
import requests
import time
import json

from tqdm import tqdm

import umap
import numba

from fastapi.middleware.cors import CORSMiddleware

# это временное, пока нету api к базе данных
from postgres.connector import PostgresConnector
import psycopg2

# -----------
new_postgres_client = PostgresConnector(db = 'account_db')
new_cursor: psycopg2.extensions.cursor = new_postgres_client.get_cursor()
# -----------
def q_exec(client: PostgresConnector, cursor : psycopg2.extensions.cursor, quiery : str):
    try:
        cursor.execute(quiery)
    except psycopg2.ProgrammingError as exc:
        # если транзакция отменена
        print(exc.message)
        client.rollback()
        cursor.execute(quiery)
    except psycopg2.InterfaceError as exc:
        # если курсор закрыли
        print(exc.message)
        client = PostgresConnector(db = 'account_db')
        cursor = client.get_cursor()
        cursor.execute(quiery)
    return client, cursor
    

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    # Можно указать конкретные источники, например ["https://example.com"]
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Разрешить все методы HTTP
    allow_headers=["*"],  # Разрешить все заголовки
)

common_terms = set()
url = 'http://193.232.208.58:5001/delter/produce_classificator_subtree' # это метод который уже берет из бд
response = requests.post(url, json = {'root_ids' : [1]})
result = json.loads(response.text)
for lvl in result:
    common_terms.update(set([e['name'] for e in result[lvl]]))

@app.get('/grapher/ping')
async def grapher_ping():
    pass

@app.get('/grapher/get_all_available_authors')
async def get_all_available_authors(request : Request, sort_mode = None, recalculate = False, keep_not_russian = False): 
    
    global new_postgres_client, new_cursor
      
    if bool(recalculate):
        print("recalculating")
        # запрос за данными
        q = "select distinct last_name, first_name, middle_name, authors.id, author_to_publications.publication_id from authors\
                                join author_to_publications on authors.id = author_to_publications.author_id\
                                join deltas on author_to_publications.publication_id = deltas.publication_id;"
        new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)

        result = new_cursor.fetchall()
        # парсинг и сбор инфо о кол-ве публикаций
        true_result = {}
        for rec in result:
            if not keep_not_russian:
                if rec[0][0] not in [l for l in 'ЙЦУКЕНГШЩЗХЪЁФЫВАПРОЛДЖЭЯЧСМИТЬБЮ']:
                    continue
            true_result.setdefault(rec[3], [rec[0] + ' ' + rec[1] + ' ' + rec[2], 0])
            true_result[rec[3]][1] += 1
        true_result = [{'prnd_author_id' : int (rec), 'fio' : str(true_result[rec][0]), 'publs_count' : int(true_result[rec][1])} for rec in true_result]
        # здесь кэшируем
        #=================
        dump_list = json.dumps(true_result)
        q = f"INSERT INTO graph_cache\
                        (id, verify, contents)\
                        VALUES ({-6}, {0}, {"'" + dump_list + "'"})\
                        ON CONFLICT (id) DO UPDATE\
                        SET verify= excluded.verify, contents= excluded.contents;"
        new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
        new_postgres_client.commit()
        #=================
    else:
        print("using cache")
        q = f"SELECT * FROM graph_cache WHERE id={-6};"
        new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
        cache_request = new_cursor.fetchall()
        true_result = json.loads(cache_request[0][2])

    if sort_mode is None:
        sort_mode = 'fio'

    if sort_mode == 'id':
        true_result = sorted(true_result, key = lambda x : x['prnd_author_id'])
    if sort_mode == 'fio':
        true_result = sorted(true_result, key = lambda x : x['fio'])
    if sort_mode == 'pubs':
        true_result = sorted(true_result, key = lambda x : x['publs_count'], reverse = True)

    final_result = [" "]
    for elm in true_result:
        if elm['prnd_author_id'] == 589:
            final_result[0] = elm
        else:
            final_result.append(elm)

    return final_result

@app.get('/grapher/get_conf_authors')
async def get_conf_authors(conf_id : str, request : Request, min_max_year = [2018,2024], verbouse : bool = True):

    global new_postgres_client, new_cursor
    
    pubs_auth = {}
    print(type(conf_id))
    print(conf_id)
    if conf_id == '':
        return []

    if verbouse:
        print(f"\t\t\tgetting pubs for {conf_id} in range {min_max_year}")
    for year in range(min_max_year[0], min_max_year[1]):
        _t_st = time.monotonic_ns()
        q = f"SELECT DISTINCT * FROM publication_sources\
                JOIN publications ON publication_sources.id = publications.publication_source_id\
                JOIN author_to_publications ON publications.id = author_to_publications.publication_id\
                WHERE conference_id = {conf_id}"
        q += f" AND publication_date >= '{min_max_year[0]}-01-01' AND publication_date < '{min_max_year[0] + 1}-01-01' OR publication_date is null;" if min_max_year is not None else ";"
        
        new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)

        auth_result = new_cursor.fetchall()
        _t_nd = time.monotonic_ns()
        if verbouse:
            print(f"\t\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest to prnd db")
        _t_st = time.monotonic_ns()
        
        
        for entry in auth_result:
            pubs_auth.setdefault(str(entry[0]), 0)
            pubs_auth[entry] += 1
        _t_nd = time.monotonic_ns()
    if verbouse:
        print(f"\t\t{(_t_nd - _t_st)/ 1e9 :.5f}\tparse responce")
        print(len(list(pubs_auth.keys())))
        print(pubs_auth)
    
    return list(pubs_auth.keys())

@app.get('/grapher/get_conf_publications')
async def get_conf_publications(conf_id : str, request : Request, min_max_year = [1900,3000], current_user_id = None, verbouse : bool = False):

    global new_postgres_client, new_cursor

    if verbouse:
        print(f"\t\t\tgetting pubs for conf {conf_id} in range {min_max_year}")
        
    _t_st = time.monotonic_ns()
    q = f"SELECT publications.id, publication_date FROM publication_sources JOIN publications ON publication_sources.id = publication_source_id\
            WHERE conference_id = {conf_id} AND verified = true"
    q += f" AND (publication_date >= '{min_max_year[0]}-01-01' AND publication_date < '{min_max_year[1]}-01-01' OR publication_date is null);" if min_max_year is not None else ";"

    if verbouse:
        print(f"\t\t\t{q}")
    
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)

    result = set(new_cursor.fetchall())
    print("got ", len(result), "verified publications")

    # 2. берем публикации, загруженные данным пользователем
    if current_user_id is not None:
        print(f"\t\t\tgetting uploads for user {current_user_id}")
        q = f"SELECT pub_id, publication_date FROM uploads AS u\
                JOIN publications as p ON p.id = u.pub_id\
                JOIN publication_sources AS s ON p.publication_source_id = s.id\
                    WHERE user_id = {int(current_user_id)} AND conference_id = {int(conf_id)}"
        new_cursor.execute(q)
        result.update(set(new_cursor.fetchall()))
        print("got ", len(result), "publications overall")
    result = list(result)

    _t_nd = time.monotonic_ns()
    
    if verbouse:
        print(f"\t\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest to db")
    if result == []:
        return []
        #raise HTTPException(status_code=201, detail="Author not found")
    else:
        for i in range(len(result)): # переделываем даты в года
            result[i] = (result[i][0], int(str(result[i][1])[0:4]) if result[i][1] is not None else None )
        return result


@app.get('/grapher/get_journal_publications')
async def get_journal_publications(conf_id : str, request : Request, min_max_year = [1900,3000], current_user_id = None, verbouse : bool = False):

    global new_postgres_client, new_cursor
    
    if verbouse:
        print(f"\t\t\tgetting pubs for journal {conf_id} in range {min_max_year}")
        
    _t_st = time.monotonic_ns()
    q = f"SELECT publications.id, publication_date FROM publication_sources JOIN publications ON publication_sources.id = publication_source_id\
            WHERE journal_id = {conf_id} AND verified = true"
    q += f" AND (publication_date >= '{min_max_year[0]}-01-01' AND publication_date < '{min_max_year[1]}-01-01' OR publication_date is null);" if min_max_year is not None else ";"

    if verbouse:
        print(f"\t\t\t{q}")
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    result = set(new_cursor.fetchall())
    print("got ", len(result), "verified publications")

    # 2. берем публикации, загруженные данным пользователем
    if current_user_id is not None:
        print(f"\t\t\tgetting uploads for user {current_user_id}")
        q = f"SELECT pub_id, publication_date FROM uploads AS u\
                JOIN publications as p ON p.id = u.pub_id\
                JOIN publication_sources AS s ON p.publication_source_id = s.id\
                    WHERE user_id = {int(current_user_id)} AND journal_id = {int(conf_id)}"
        new_cursor.execute(q)
        result.update(set(new_cursor.fetchall()))
        print("got ", len(result), "publications overall")
    result = list(result)

    _t_nd = time.monotonic_ns()
    print("got ", len(result), " publications")

    if verbouse:
        print(f"\t\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest to db")
    if result == []:
        return []
        #raise HTTPException(status_code=201, detail="Author not found")
    else:
        for i in range(len(result)): # переделываем даты в года
            result[i] = (result[i][0], int(str(result[i][1])[0:4]) if result[i][1] is not None else None)
        return result

# версия, которая использует postgres
@app.get('/grapher/get_author_publications')
async def new_get_author_publications(auth_prnd_id : str, request : Request, min_max_year = None, current_user_id = None, verbouse : bool = False):
    auth_id = auth_prnd_id
    global new_postgres_client, new_cursor

    # 1. получаем подтвержденные публикации автора
    q = f"SELECT publications.id, publication_date FROM author_to_publications\
    JOIN publications ON author_to_publications.publication_id = publications.id\
    WHERE author_id = {auth_id}"# AND verified = true"
    q += f"AND (publication_date >= '{min_max_year[0]}-01-01' AND publication_date < '{min_max_year[0] + 1}-01-01');" if min_max_year is not None else ";"
    
    if verbouse:
        print(f"\t\t\tgetting pubs for {auth_id} in range {min_max_year}")
        print(q)         
    
    _t_st = time.monotonic_ns()
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    result = set(new_cursor.fetchall())
    print("got ", len(result), "verified publications")
    _t_nd = time.monotonic_ns()

    # 2. берем публикации, загруженные данным пользователем
    if current_user_id is not None:
        print(f"\t\t\tgetting uploads for user {current_user_id}")
        q = f"SELECT pub_id, publication_date FROM uploads AS u\
                JOIN author_to_publications AS a_p ON u.pub_id = a_p.publication_id\
                JOIN publications as p ON p.id = u.pub_id\
                    WHERE user_id = {int(current_user_id)} AND author_id = {int(auth_id)}"
        new_cursor.execute(q)
        result.update(set(new_cursor.fetchall()))
        print("got ", len(result), "publications overall")
    result = list(result)
    
    if verbouse:
        print(f"\t\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest to db")
    if result == []:
        return []
        #raise HTTPException(status_code=201, detail="Author not found")
    else:
        for i in range(len(result)): # переделываем даты в года
            result[i] = (result[i][0], int(str(result[i][1])[0:4]) if result[i][1] is not None else None)
        return result

# версия, которая использует postgres
@app.get('/grapher/get_author_min_max_year')
async def new_get_author_min_max_year(auth_prnd_id : str, request : Request, verbouse = True, current_user_id = None):
    auth_id = auth_prnd_id
    __t_st = time.monotonic_ns()
    
    _t_st = time.monotonic_ns()
    pubs_id_and_years = await new_get_author_publications(auth_id, request, verbouse = verbouse, current_user_id = current_user_id)
    _t_nd = time.monotonic_ns()

    unique_dates = set([pair[1] for pair in pubs_id_and_years])

    if verbouse:
        print(f"\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest for pubs")
        print(unique_dates)
    # возвращаем минимальную и максимальную дату
    __t_nd = time.monotonic_ns()
    if verbouse:
        print(f"\t{(__t_nd - __t_st)/ 1e9 :.5f}\toverall time")        
    return {"min" : min(unique_dates), "max" : max(unique_dates)}


# по prnd_id получить дельты всех публикаций автора
# сначла делает запрос к бд Евгения за списком публикаций, затем к дельтеру за списком дельт
# это нужно использовать при рассчете наложения автора на тезаурус
# использует новую бд
@app.get('/grapher/get_author_deltas')
async def new_get_entity_deltas(auth_prnd_id : str, request : Request,  factor_level = 3, 
                                                                        mark_common_terms = False,
                                                                        freq_cutoff = None, 
                                                                        mode : str = 'merged', 
                                                                        min_max_year = None, 
                                                                        verbouse : bool = True, 
                                                                        return_stats : bool = False,
                                                                        publcation_deliver = new_get_author_publications,
                                                                        current_user_id = None,
                                                                        was_called_as_request : bool = True):
    auth_id = auth_prnd_id
    __t_st = time.monotonic_ns()
    # получаем список с prnd id и датами
    _t_st = time.monotonic_ns()
    pubs_id_and_years = await publcation_deliver(auth_id, request, verbouse = verbouse, current_user_id = current_user_id)
    pubs_id_to_years = {i:y for i, y in pubs_id_and_years}
    _t_nd = time.monotonic_ns()
    if verbouse:
        print(f"\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest for pubs")
    # формирование списка id пбликаций, дельты которых надо получить

    pubs_overall = len(pubs_id_and_years)
    # фильтрация по годам
    pubs_filtered_by_year = 0
    pubs_ids_for_deltas = []
    for pub in pubs_id_and_years: # pub[1] is None - пропускаем, если у публикации не указан год
        if min_max_year is None or pub[1] is None or (int(pub[1]) >= min_max_year[0] and int(pub[1]) <= min_max_year[1]):
            pubs_ids_for_deltas.append(int(pub[0]))
        else:
            pubs_filtered_by_year += 1

    if verbouse:
        print(pubs_filtered_by_year)
    # запрос к дельтеру за дельтами
    _t_st = time.monotonic_ns()
    #url = 'http://193.232.208.58:5001/delter/do_the_delting' # поменять
    url = 'http://193.232.208.58:5001/delter/get_publs_deltas' # это метод который уже берет из бд
    myobj = {'publ_ids': pubs_ids_for_deltas,
            'id_type' : 'local',
            'format' : 'names',
            'level' : factor_level,
            'common_terms' : 'mark' if mark_common_terms else 'leave',
            'result' : mode}
    response = requests.post(url, json = myobj)
    _t_nd = time.monotonic_ns()
    if verbouse:
        print(f"\t{(_t_nd - _t_st)/ 1e9 :.5f}\trequest to delter")
    if response.status_code != 200:
        print("\t delter request error")
        raise HTTPException(status_code=201, detail="delter request error")
        
    # указываем в словаре с дельтами год для каждой публикации
    deltas_dict = json.loads(response.text)
    if mode == 'list':
        for publ in deltas_dict:
            deltas_dict[publ] = {'delta' : deltas_dict[publ], 'year' : pubs_id_to_years[int(publ)]}
        pubs_with_deltas = len(deltas_dict)
    else:
        pubs_with_deltas = None

    __t_nd = time.monotonic_ns()
    if verbouse:
        print(f"\t{(__t_nd - __t_st)/ 1e9 :.5f}\toverall time")
    if return_stats: # это формат вывода, если функция была вызвана не как запрос
        return deltas_dict, pubs_overall, pubs_filtered_by_year, pubs_with_deltas

    if was_called_as_request and mode != 'list':
        if freq_cutoff is not None:
            # cutoff поддерживается только если вызываем как реквест и мерджим результат
            # смотрим cutoff-значение    
            node_freq_list = sorted([deltas_dict[t] for t in deltas_dict])
            cutoff_elm_pos = round(len(node_freq_list) * 0.01 * int(freq_cutoff))
            # по нулевому элементу ничего не делаем, чтобы работало нулевое отсечение
            # а еще если резать пустой граф, то будет ошибка
            if cutoff_elm_pos != 0:
                if cutoff_elm_pos >= len(node_freq_list):
                    cutoff_elm_pos = len(node_freq_list) - 1
                node_freq_cutoff = node_freq_list[cutoff_elm_pos] + 1  
            else:
                node_freq_cutoff = 0
        # если запрашиваем в режиме слияния, (с сайта запрос), то возвращаем прикольным словарем
        deltas_list = []
        for term in deltas_dict:
            if freq_cutoff is not None:
                if int(deltas_dict[term]) <= int(node_freq_cutoff):
                    continue
            deltas_list.append({"term_name" : str(term), "term_freq" : int(deltas_dict[term])}) 
        return deltas_list
    
    return deltas_dict

# удалить из графа все вершины (filter_nodes) и ребра (filter_edges) у которых значение по ключу key_name
#       меньше key_min_value
#       больше key_max_value
#       если ключ - это набор из нескольких значений (key_is_list) то смотрится, все ли значения меньше или больше ссотв. пределов
def filter_graph(G : nx.Graph, key_name : str, key_min_value = None, key_max_value = None, key_is_list = False, filter_nodes = True, filter_edges = False):
    if filter_nodes:
        nodes_to_rm_min = [n for n in G.nodes if (max(G.nodes[n][key_name]) if key_is_list else G.nodes[n][key_name]) < key_min_value] if key_min_value is not None else []
        nodes_to_rm_max = [n for n in G.nodes if (min(G.nodes[n][key_name]) if key_is_list else G.nodes[n][key_name]) > key_max_value] if key_max_value is not None else []
        
        print(f"\tfiltering out {len(nodes_to_rm_min) + len(nodes_to_rm_max)} nodes (out of {len(G.nodes)})\t|  by\t{key_max_value} < \t{key_name}\t < {key_min_value}")
        G.remove_nodes_from(nodes_to_rm_min)
        G.remove_nodes_from(nodes_to_rm_max)
    
    if filter_edges:
        edges_to_rm_min = [n for n in G.edges if (max(G.edges[n][key_name]) if key_is_list else G.edges[n][key_name]) < key_min_value] if key_min_value is not None else []
        edges_to_rm_max = [n for n in G.edges if (min(G.edges[n][key_name]) if key_is_list else G.edges[n][key_name]) > key_max_value] if key_max_value is not None else []
        
        print(f"\tfiltering out {len(edges_to_rm_min) + len(edges_to_rm_max)} edges (out of {len(G.edges)})\t|  by\t{key_max_value} < \t{key_name}\t < {key_min_value}")
        G.remove_edges_from(edges_to_rm_min)
        G.remove_edges_from(edges_to_rm_max)

    return G

# по prnd_id получить граф связности для автора
# финальный функционал должен быть таким:
#   Обратиться к бд в таблицу с закешированными графами 
#   Обратиться к бд и узнать, сколько у автора публикаций
#       Если verivicaton совпадает с кол-вом публикаций автора, взять граф из таблицы
#           Применить к полученному графу отсечения
#       Если не совпадают, то пересчитать граф, как в функции ниже, вычислить layout, записать в таблицу
#           В поле verivicaton указать кол-во статей автора  
# Сами запросы должны кэшироваться редисом
# версия, которая использует новую бд
@app.post('/grapher/produce_author_connectivity_graph')
async def new_produce_author_connectivity_graph(request : Request):
    global new_postgres_client, new_cursor

    __t_st = time.monotonic_ns() 

    use_cache = "Normal"

    # # считывание доп. параметров запроса
    request_details = await request.json() # сюда можно давать параметры (пока параметров нет)
    print(request_details)
    auth_id = str(request_details["author_prnd_id"])
        
    min_node_count = request_details["min_node_count"] if 'min_node_count' in request_details else None
    # минимальное число, сколько раз термин встретился в статьях автора
    node_cutoff_mode = request_details["node_cutoff_mode"] if 'node_cutoff_mode' in request_details else 'overall'
    # по статьям в отдельности или по суммарному графу
    min_edge_count = request_details["min_edge_count"] if 'min_edge_count' in request_details else None
    # минимальное число, сколько раз связь встретился в графе
    min_node_neighbors = request_details["min_node_neighbors"] if 'min_node_neighbors' in request_details else None
    # минимальное число соседей термина
    min_max_year = request_details["years_range"] if 'years_range' in request_details else None
    # использовать для построения статьи в данном промежутке годов
    keep_data_in_graph = request_details["keep_data_in_graph"] if 'keep_data_in_graph' in request_details else False
    # возвращать граф с доп. информацией в вершиных и ребрах (в первую очередь частоты)
    use_common_terms = request_details["use_common_terms"] if 'use_common_terms' in request_details else False
    # включать ли общенаучные термины 
    factor_level = request_details["factor_level"] if 'factor_level' in request_details else 3
    # уровень термина (не работает)
    subtree_root_ids = request_details["subtree_root_ids"] if 'subtree_root_ids' in request_details else None
    # корень поддерева классификатора
    scale_cutoff_by_paper_num = request_details["scale_cutoff_by_paper_num"] if 'scale_cutoff_by_paper_num' in request_details else False

    # использовать ли кеш
    use_cache = request_details["use_cache"] if "use_cache" in request_details else "AsumeOk"
    # None, 'Normal', 'AsumeOk'

    # если пользователь зарегестрирован, нужно передать его id, чтобы учитывалиь неподтвержденные публикации, загруженные им
    current_user_id = request_details["current_user_id"] if "current_user_id" in request_details else None
    
    # для чего строится граф (автор / конференция / журнал)
    entity_type = request_details["entity_type"] if "entity_type" in request_details else "author"
    # от этого зависит метод запроса дельта
    if entity_type == "author":
        publication_deliver = new_get_author_publications
    elif entity_type == "conference":
        publication_deliver = get_conf_publications
    elif entity_type == "journal":
        publication_deliver = get_journal_publications
    else:
        return "wrong entity type, must be 'author', 'conference' or 'journal'"

    

    if factor_level != 3:
        use_cache = None
    if factor_level == 0:
        min_node_count = 0
    if entity_type == "author" or entity_type == "conference" or entity_type == "journal":
        pass
    else:
        return "wrong entity type, must be 'author', 'conference' or 'journal'"

    # получаем дельты автора
    _t_st = time.monotonic_ns()
    deltas, pubs_cnt, filtered_by_years, deltas_cnt = await new_get_entity_deltas(auth_id, request, 
                                                                                    factor_level = factor_level,
                                                                                    mark_common_terms = False,
                                                                                    mode = 'list',
                                                                                    min_max_year = min_max_year, 
                                                                                    return_stats = True,
                                                                                    was_called_as_request = False,
                                                                                    publcation_deliver= publication_deliver,
                                                                                    current_user_id = current_user_id
                                                                                    )
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\trequest for deltas")  

    print(f"got info for      \t{pubs_cnt}\t papers")
    print(f"filtered by years \t{filtered_by_years}\t papers")
    print(f"got deltas for    \t{deltas_cnt}\t papers")

    _t_st = time.monotonic_ns()
    # создание объекта графа
    Graph = nx.Graph()
    # добавление узлов и ребер в граф
    # указание в узлах и ребрах дат
    
    all_nodes = Counter()
    all_edges = Counter() if min_edge_count is not None else set()
    for pub in deltas:
        # per paper это вариант который эмулирует поведение старого алгоритма построения графов. Он плохо работает с кэшем
        # но получает более легкие графы
        pub_delta = deltas[pub]['delta'] if node_cutoff_mode != 'per_paper' else {t:f for t,f in deltas[pub]['delta'].items() if f >= min_node_count}
        all_nodes.update(pub_delta)
        if min_edge_count is not None:
            all_edges.update(set(itertools.combinations(pub_delta.keys(), r = 2)))
        else:
            all_edges |= set(itertools.combinations(pub_delta.keys(), r = 2))
        
    all_nodes = dict(all_nodes)
    if min_edge_count is not None:
        all_edges = dict(all_edges)

    Graph.add_nodes_from(all_nodes)
    Graph.add_edges_from(all_edges)

    # for n in all_nodes:
    #     if n[-1] == chr(0x01):
    #         Graph.nodes[n]['common'] = True
    #     else:
    #         Graph.nodes[n]['common'] = False
    for n in all_nodes:
        Graph.nodes[n]['count'] = all_nodes[n]
    if min_edge_count is not None:
        for e in all_edges:
            Graph.edges[e]['count'] = all_edges[e]
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t build graph")
    
    _t_st = time.monotonic_ns()
    # ТУТ КЭШ
    # положительные id для авторов
    # отрицательные от -20 до 0 для разных вещей типа тезауруса и прочего
    # отрицательные четные до -20 для конференций
    # отрицательные нечетные до -20 для журналов
   
    if use_cache is not None:
        auth_id = int(auth_id)
        if entity_type == "conference":
            auth_id *= -2
            auth_id -= 200
        if entity_type == "journal":
            auth_id *= -2
            auth_id -= 1
            auth_id -= 200
        print("using cache")
        print(f"will save cache at id {auth_id}")

        q  = f"SELECT * FROM graph_cache WHERE id={auth_id};"
        new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
        cache_request = new_cursor.fetchall()

        if len(cache_request) == 0:
            print("\tno cache found")
            # кэша для этого автора нет

            if filtered_by_years == 0:
                print("\t\trecalculating...")
                # если берем по всему спектру дат, то генерируем новый кэш
                start_time = time.time()
                pos = nx.kamada_kawai_layout(Graph)
                print(f"\t\t\tlayout computed \t {time.time() - start_time}")
                start_time = time.time()
                pos = [{"term" : k, "pos" : p.tolist()} for k, p in pos.items()]
                print(f"\t\t\tlayout converted\t {time.time() - start_time}")
                # тут сохраняем раскладку в бд
                print("\t\tsaving...")

                q = f"INSERT INTO graph_cache\
                                            (id, verify, contents)\
                                            VALUES ({auth_id}, {deltas_cnt}, {"'" + json.dumps(pos) + "'"})\
                                            ON CONFLICT (id) DO UPDATE\
                                                SET verify= excluded.verify, contents= excluded.contents;"
                new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
                new_postgres_client.commit()

                print("\t\tdone!")
            else:
                print("\t\t\tcurrent graph insufficient to compute cache layout")
                # в противном случае строим layout после фильтрации графа
                use_cache = None
        else:
            print("\tloaded cache")
            # кэш есть.            
            if cache_request[0][1] == deltas_cnt or filtered_by_years != 0 or use_cache == 'AsumeOk':
                # проверяем кэш только если отсечение по годам равно 0, иначе считаем, что кэш правильный
                if filtered_by_years == 0:
                    print("\t\tcache ok")
                else:
                    print("\t\tnot checking cache, assuming ok")
                # кэш построен по тому же количеству публикаций, что и текущий граф. Просто возврашаем кэш
                pos = json.loads(cache_request[0][2])
            else:
                print("\t\tcache depricated")
                # кэш устарел.
                if filtered_by_years == 0 and node_cutoff_mode != 'per_paper':
                    print("\t\t\trecalculating...")
                    # Если строим по полному спектру дат, генерируем новый кэш
                    start_time = time.time()
                    pos = nx.kamada_kawai_layout(Graph)
                    print(f"\t\t\t\tlayout computed \t {time.time() - start_time}")
                    start_time = time.time()
                    pos = [{"term" : k, "pos" : p.tolist()} for k, p in pos.items()]
                    print(f"\t\t\t\tlayout converted\t {time.time() - start_time}")
                    # тут сохраняем раскладку в бд
                    print("\t\t\tsaving...")

                    q = f"INSERT INTO graph_cache\
                                                (id, verify, contents)\
                                                VALUES ({auth_id}, {deltas_cnt}, {"'" + json.dumps(pos) + "'"})\
                                                ON CONFLICT (id) DO UPDATE\
                                                    SET verify= excluded.verify, contents= excluded.contents;"
                    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
                    new_postgres_client.commit()

                    print("\t\t\tdone!")
                else:
                    print("\t\t\tcurrent graph insufficient to compute cache layout")
                    # в противном случае строим layout после фильтрации графа
                    use_cache = None
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t cache interacion complete") 

    for adj in Graph.adjacency():
        Graph.nodes[adj[0]]['adjacency'] = len(adj[1].keys())
    # тут будет загрузка графа из бд кэша, если обновлять граф не надо
    # фильтрация графа по ключу (отсечение по частоте)
    _t_st = time.monotonic_ns()
    if min_node_count is not None:
        # при cutoff_mode per paper эта часть не нужна вообще
        if node_cutoff_mode == 'overall':
            Graph = filter_graph(Graph, "count", key_min_value = min_node_count if not scale_cutoff_by_paper_num else min_node_count*deltas_cnt*float(scale_cutoff_by_paper_num))
        if node_cutoff_mode == 'percent':
            print("  cutting py percent")
            # число, которое приходит на cutoff в таком случае - процент
            node_freq_list = sorted([Graph.nodes[node]['count'] for node in Graph.nodes])
            cutoff_elm_pos = round(len(node_freq_list) * 0.01 * min_node_count)
            # по нулевому элементу ничего не делаем, чтобы работало нулевое отсечение
            # а еще если резать пустой граф, то будет ошибка
            if cutoff_elm_pos != 0:
                if cutoff_elm_pos >= len(node_freq_list):
                    cutoff_elm_pos = len(node_freq_list) - 1
                node_freq_cutoff = node_freq_list[cutoff_elm_pos] + 1  
                # натоящий cutoff frq, отсечение по которому удовлетворит требуемому проценту отсеченных узлов
                print(f"  cutoff set as {node_freq_cutoff} (element {round(len(node_freq_list) * 0.01 * min_node_count)})")
                Graph = filter_graph(Graph, "count", key_min_value = node_freq_cutoff)

    # фильтрация для сужения по тезаурусу
    if subtree_root_ids is not None:
        allowed_terms = set()
        url = 'http://193.232.208.58:5001/delter/produce_classificator_subtree' # это метод который уже берет из бд
        response = requests.get(url, params = {'root_ids' : subtree_root_ids})
        result = json.loads(response.text)
        for lvl in result:
            allowed_terms.update(set([e['name'] for e in result[lvl]]))
        # тут очистка от отсеченных по классификатору терминов
        nodes_to_remove = [n for n in Graph.nodes if n not in allowed_terms]
        print(f"\tfiltering out {len(nodes_to_remove)} nodes (out of {len(Graph.nodes)})\t|  by\tclasificator cutoff")
        Graph.remove_nodes_from(nodes_to_remove)
    if min_node_neighbors is not None:
        nodes_to_remove = [n for n in Graph.nodes if len(list(Graph.neighbors(n))) < min_node_neighbors]
        print(f"\tfiltering out {len(nodes_to_remove)} nodes (out of {len(Graph.nodes)})\t|  by\tnum neighbors < \t{min_node_neighbors}")
        Graph.remove_nodes_from(nodes_to_remove)
    if not use_common_terms:
        # тут очистка от общенаучных терминов
        nodes_to_remove = [n for n in Graph.nodes if n in common_terms]
        print(f"\tfiltering out {len(nodes_to_remove)} nodes (out of {len(Graph.nodes)})\t|  by\tbeing common terms")
        Graph.remove_nodes_from(nodes_to_remove)
    #else:
        #nx.relabel_nodes(Graph, {node : node[0:-1] for node in Graph.nodes if Graph.nodes[node]['common']}, copy = False)
    if min_edge_count is not None:
        Graph = filter_graph(Graph, "count", key_min_value = min_edge_count,filter_nodes = False, filter_edges = True)

    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t filter graph") 


    
    if not keep_data_in_graph:
        _t_st = time.monotonic_ns()
        tmp = nx.Graph()
        tmp.add_nodes_from(list(Graph.nodes))
        tmp.add_edges_from(list(Graph.edges))
        Graph = tmp
        _t_nd = time.monotonic_ns()
        print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t clean graph") 

    if not use_cache: # если не используем кэш, то формируем layout здесь
        start_time = time.time()
        pos = nx.kamada_kawai_layout(Graph)
        print(f"\tlayout computed \t {time.time() - start_time}")
        start_time = time.time()
        pos = [{"term" : k, "pos" : p.tolist()} for k, p in pos.items()]
        print(f"\tlayout converted\t {time.time() - start_time}")

    __t_nd = time.monotonic_ns()
    print(f"{(__t_nd - __t_st)/ 1e9 :.5f}\toverall time")

    # удалям все вершины, для которых нет координат в pos
    pos_keys = set([elm["term"] for elm in pos])
    graph_nodes = [n for n in Graph.nodes]
    for elm in graph_nodes:
        if elm not in pos_keys:
            Graph.remove_node(elm)
    # ===================================================

    # это нужно, чтобы можно было, итерируясь по ребрам просмотреть все вершины
    for node in Graph.nodes:
        Graph.add_edge(node,node)

    _t_st = time.monotonic_ns()
    _ = nx.node_link_data(Graph)
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t estimated time to dump")
    return {"graph" : nx.node_link_data(Graph), "layout" : pos}

@app.get('/grapher/get_all_available_conferences')
async def produce_avail_confs(request : Request):

    global new_postgres_client, new_cursor

    # тут надо брать список ВСЕХ конференций, прокидать запросы Евгению, посмотреть, на что придет ненулевой ответ
    q = f"SELECT * FROM conferences;"
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    confs = new_cursor.fetchall()
    return [{   'name_full' : elm[2] if elm[2] is not None else "" + " (" + elm[1] + ")",
                'name_disp' : elm[1], 
                'name_req'  : elm[0]} for elm in confs]

@app.get('/grapher/get_all_available_journals')
async def produce_avail_confs(request : Request):

    global new_postgres_client, new_cursor

    # тут надо брать список ВСЕХ конференций, прокидать запросы Евгению, посмотреть, на что придет ненулевой ответ
    q = f"SELECT * FROM journals;"
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    confs = new_cursor.fetchall()
    return [{   'name_full' : elm[2] if elm[2] is not None else "" + " (" + elm[1] + ")",
                'name_disp' : elm[1], 
                'name_req'  : elm[0]} for elm in confs]

# функция для измерения расстояний при обучении umap
@numba.njit()
def dist_2(lhs, rhs):
    return 0.5*np.sum(np.abs(lhs - rhs))
# построить карту с двумерной проекцией профилей авторов (и закешировать в бд)
@app.get('/grapher/build_profile_map')
async def build_profile_map(request : Request):
    global new_postgres_client, new_cursor
    __t_st = time.monotonic_ns()
    # сначала получаем список всех доступных авторов
    _t_st = time.monotonic_ns()
        # ТУТ НЕДОПИЛЕНО !!   
    authors = await get_all_available_authors(request)
    auth_dict = {elm['prnd_author_id'] : elm['fio'] for elm in authors}
    print(f"got info on {len(auth_dict)} authors")
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t request for authors")
    # теперь для каждого автора запрашиваем профиль единым словарем
    _t_st = time.monotonic_ns()
    author_profiles = dict()
    # for c_n in tqdm(avali_confs):
    #     deltas = await get_entity_deltas(c_n, request, mode = 'merged', verbouse = False, publcation_deliver = get_conf_publications, was_called_as_request = False)
    #     author_profiles[c_n] = deltas

    for ind in tqdm(auth_dict):
        q  = f"\
                SELECT variant, deltas.factor_id, SUM(value) FROM author_to_publications AS atp\
                JOIN deltas ON atp.publication_id = deltas.publication_id\
                JOIN factor_name_variants ON deltas.factor_id = factor_name_variants.factor_id\
                JOIN\
                    (SELECT  1.0/COUNT(coauth.author_id) AS mul, \
                        author_to_publications.publication_id FROM author_to_publications\
                    JOIN author_to_publications coauth ON author_to_publications.publication_id= \
                                        coauth.publication_id\
                    WHERE author_to_publications.author_id = {ind}\
                    GROUP BY author_to_publications.publication_id) AS coauth_mod\
                    ON deltas.publication_id = coauth_mod.publication_id\
                WHERE atp.author_id = {ind} AND language_id = 1\
                GROUP BY variant, deltas.factor_id\
        ;"
        new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
        deltas = new_cursor.fetchall()
        #get_conf_publications
        author_profiles[ind] = {factor[0] : factor[2] for factor in deltas if factor[0] not in common_terms}

    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t requests for deltas")
    # приводим список профилей к двумерному виду
    print(len(author_profiles))
    return author_profiles

    _t_st = time.monotonic_ns()
    unique_terms = {}
    unique_terms_cnt = Counter()
    for a in author_profiles:
        unique_terms_cnt.update(author_profiles[a])
        for t in author_profiles[a]:
            unique_terms[t] = 0
    unique_terms_cnt = dict(unique_terms_cnt)
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t find unique terms")

    _t_st = time.monotonic_ns()
    normalized_author_profiles = {}
    for a in tqdm(author_profiles):
        normalized_author_profiles[a] = unique_terms | author_profiles[a]
        for t in normalized_author_profiles[a]:
            normalized_author_profiles[a][t] = normalized_author_profiles[a][t] / unique_terms_cnt[t]
    _t_nd = time.monotonic_ns()
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t calculate normalized author profiles")

    _t_st = time.monotonic_ns()
    authors = {}
    normalized_author_profiles_array = []
    i = 0
    for a in tqdm(normalized_author_profiles):
        if sum(normalized_author_profiles[a].values()) != 0:
            authors[a] = i
            normalized_author_profiles_array.append(list(normalized_author_profiles[a].values()))
            i += 1
    normalized_author_profiles_array = np.array(normalized_author_profiles_array)
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t convert normalized author profiles")

    _t_st = time.monotonic_ns()
    transformer = umap.UMAP(
        n_neighbors = 5,
        min_dist = 0.2,
        n_components = 2,
        metric = dist_2,
        random_state = 35
    )
    embeding = transformer.fit_transform(normalized_author_profiles_array)
    print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\t transform to 2d")

    # сохраняем результат в бд
    pos_like = json.dumps([{"ent" : str(a), "pos" : [float(embeding[authors[a]][0]), float(embeding[authors[a]][1])]} for a in authors])

    # тут сохраняем раскладку в бд
    print("\t\tsaving...")
    cache_cursor.execute(f"INSERT INTO graph_cache_tmp\
                                (id, verify, contents)\
                                VALUES ({-1}, {0}, {"'" + pos_like + "'"})\
                                ON CONFLICT (id) DO UPDATE\
                                    SET verify= excluded.verify, contents= excluded.contents;")
    postgres_client.commit()
    print("\t\tdone!")

    __t_nd = time.monotonic_ns()
    print(f"{(__t_nd - __t_st)/ 1e9 :.5f}\toverall time")

# вернуть закешированную карту
@app.get('/grapher/produce_profile_map')
async def produce_profile_map(request : Request):
    global new_postgres_client, new_cursor
    print("using cache")

    q = f"SELECT * FROM graph_cache WHERE id={-1};"
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    cache_request = new_cursor.fetchall()

    return json.loads(cache_request[0][2])

@app.post('/grapher/build_and_save_thesaurus_graph')
async def build_thesaurus_graph(request : Request):
    global new_postgres_client, new_cursor
    __t_st = time.monotonic_ns()

    request_details = await request.json() # сюда можно давать параметры (пока параметров нет)
    
    thesaurus_id = request_details["thesaurus_id"]
    terms_table =  request_details["terms_table"]
    adjacency_table = request_details["adjacency_table"]

    if thesaurus_id >= 0:
        return None

    nodes = []
    terms = [item["term"].strip() for item in terms_table]
    for term in terms:
        nodes.append(term)
    edges = []
    for i in range(len(adjacency_table)):
        for j in range(len(adjacency_table[0])):
            if adjacency_table[i][j] == 1 and i != j:
                if terms[i] in nodes and terms[j] in nodes:
                    edges.append([terms[j], terms[i]])

    print(thesaurus_id)
    print(len(nodes))
    print(len(edges))

    # создание объекта графа
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    
    for node in G.nodes:
        G.nodes[node].setdefault("dp", len(set(G.neighbors(node))) / len(set(G.predecessors(node))) if len(set(G.predecessors(node))) != 0 else -1)
    graph = nx.node_link_data(G)
    print("constructed graph")
    pos = nx.kamada_kawai_layout(G)
    pos = [{"term" : k, "pos" : p.tolist()} for k, p in pos.items()]
    graph_dict = json.dumps({"graph" : graph, "layout" : pos})

    print("\t\tsaving...")

    q = f"INSERT INTO graph_cache\
                                (id, verify, contents)\
                                VALUES ({thesaurus_id}, {0}, {"'" + graph_dict + "'"})\
                                ON CONFLICT (id) DO UPDATE\
                                SET verify= excluded.verify, contents= excluded.contents;"
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    new_postgres_client.commit()

    print("\t\tdone!")

    __t_nd = time.monotonic_ns()
    print(f"{(__t_nd - __t_st)/ 1e9 :.5f}\toverall time")
    

@app.post('/grapher/produce_thesaurus_graph')
async def post_produce_thesaurus_graph(request : Request):
    global new_postgres_client, new_cursor

    request_details = await request.json()
    thesaurus_type = request_details["thesaurus_type"] if "thesaurus_type" in request_details else 'new'
    use_root = request_details["use_root"] if "use_root" in request_details else None
    sg_depth = request_details["sg_depth"] if "sg_depth" in request_details else 2
    subtree_root_ids = request_details["subtree_root_ids"] if "subtree_root_ids" in request_details else None
    remove_common_terms = request_details["remove_common_terms"] if "remove_common_terms" in request_details else False
    if subtree_root_ids == [0]:
        subtree_root_ids = None

    __t_st = time.monotonic_ns()
    if thesaurus_type == 'new':
        thes_id = -101
    elif thesaurus_type == 'old':
        thes_id = -102
    else:
        return None

    sg_depth = int(sg_depth)

    q = f"SELECT * FROM graph_cache WHERE id={thes_id};"
    new_postgres_client, new_cursor = q_exec(new_postgres_client, new_cursor, q)
    cache_request = new_cursor.fetchall()

    thesaurus_dict = json.loads(cache_request[0][2])

    # если нужно построить подграф от заданого корня, то считываем граф здесь и модифицируем его
    if use_root is not None:
        _t_st = time.monotonic_ns()
        thesaurus_graph = nx.node_link_graph(thesaurus_dict['graph'])
        if use_root in thesaurus_graph: # проверяем, есть ли этот корень в графе вообще
            print(f"\troot {use_root} in graph")
            cropped_thesaurus_graph_nodes = set([use_root])
            cur_fornteer = set([use_root])
            new_fronteer = set()
            steps = 0
            print(f"\tproducing {sg_depth} generations of successors")
            while steps < int(sg_depth):
                for n in cur_fornteer:
                    new_fronteer.update(set(thesaurus_graph.neighbors(n)))
                cropped_thesaurus_graph_nodes.update(new_fronteer)
                cur_fornteer = set(new_fronteer)
                steps += 1
            cropped_thesaurus_graph = thesaurus_graph.subgraph(cropped_thesaurus_graph_nodes)
            thesaurus_dict['graph'] = nx.node_link_data(cropped_thesaurus_graph)
        else:
            print(f"\troot {use_root} not in graph")
        _t_nd = time.monotonic_ns()
        print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\tcrop graph")    

    if subtree_root_ids is not None:
        _t_st = time.monotonic_ns()
        thesaurus_graph = nx.node_link_graph(thesaurus_dict['graph'])
        #========================================
        allowed_terms = set()
        url = 'http://193.232.208.58:5001/delter/produce_classificator_subtree' # это метод который уже берет из бд
        response = requests.post(url, json = {'root_ids' : subtree_root_ids})
        result = json.loads(response.text)
        for lvl in result:
            allowed_terms.update(set([e['name'] for e in result[lvl]]))
        #========================================
        # тут очистка от отсеченных по классификатору терминов
        print(f"\tallowed {allowed_terms} terms")
        nodes_to_remove = [n for n in thesaurus_graph.nodes if n not in allowed_terms]
        print(f"\tremoving {nodes_to_remove} terms")
        print(f"\tfiltering out {len(nodes_to_remove)} nodes (out of {len(thesaurus_graph.nodes)})\t|  by\tclasificator cutoff")
        thesaurus_graph.remove_nodes_from(nodes_to_remove)
        #========================================
        thesaurus_dict['graph'] = nx.node_link_data(thesaurus_graph)

        _t_nd = time.monotonic_ns()
        print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\tcrop graph via subtree")    

    if remove_common_terms:
        _t_st = time.monotonic_ns()
        thesaurus_graph = nx.node_link_graph(thesaurus_dict['graph'])
        
        nodes_to_remove = [n for n in thesaurus_graph.nodes if n in common_terms]
        print(f"\tfiltering out {len(nodes_to_remove)} nodes (out of {len(thesaurus_graph.nodes)})\t|  by\tbeing common terms")
        thesaurus_graph.remove_nodes_from(nodes_to_remove)

        thesaurus_dict['graph'] = nx.node_link_data(thesaurus_graph)
        _t_nd = time.monotonic_ns()
        print(f"{(_t_nd - _t_st)/ 1e9 :.5f}\tremoved common terms")    



    thesaurus_graph = nx.node_link_graph(thesaurus_dict['graph'])
    for node in thesaurus_graph.nodes:
        thesaurus_graph.add_edge(node,node)
    thesaurus_dict['graph'] = nx.node_link_data(thesaurus_graph)

    __t_nd = time.monotonic_ns()
    print(f"{(__t_nd - __t_st)/ 1e9 :.5f}\toverall time")
    return thesaurus_dict

# ретрансляция для запроса корней классификатора
@app.get('/grapher/produce_classificator_roots')
async def produce_classificator_subtree(request : Request):
    url = 'http://193.232.208.58:5001/delter/produce_classificator_roots' # это метод который уже берет из бд
    response = requests.get(url)
    return json.loads(response.text)

# ретрансляция для запроса части классификатора
@app.post('/grapher/produce_classificator_subtree')
async def produce_classificator_subtree(request : Request):

    request_details = await request.json()

    url = 'http://193.232.208.58:5001/delter/produce_classificator_subtree' # это метод который уже берет из бд
    response = requests.post(url, json = request_details)
    return json.loads(response.text)