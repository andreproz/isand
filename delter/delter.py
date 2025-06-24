from fastapi import FastAPI, Query, Request, HTTPException
from postgres.connector import PostgresConnector
import psycopg2
import time
import json
from collections import Counter

from fastapi.middleware.cors import CORSMiddleware

postgres_client = PostgresConnector()
old_db_cursor:  psycopg2.extensions.cursor = postgres_client.get_cursor()

new_db_client = PostgresConnector(db = 'account_db')
new_dp_cursor:  psycopg2.extensions.cursor = new_db_client.get_cursor()

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

print("initializing...")
# эта таблица нужна, пока мы получаем публикации автора из прнд, а не из своей бд
prnd_to_new_translation_table = {}
# таблица для трансляции id терминов в названия
factor_id_to_factor_names = {}
# множество общенаучных терминов
common_terms = set()

def initialize_papers_id_translation_table():
    global prnd_to_new_translation_table
    # берем все публикации из старой бд
    new_dp_cursor.execute("SELECT * FROM publication_mapping_prnd")
    result = new_dp_cursor.fetchall()
    for elm in result:
        prnd_to_new_translation_table[elm[1]] = elm[0]

def initialize_factor_id_to_name_translation_table():
    global factor_id_to_factor_names
    new_dp_cursor.execute("SELECT factor_id, variant FROM factor_name_variants")
    factor_names_req = new_dp_cursor.fetchall()
    for var in factor_names_req:
        factor_id_to_factor_names[var[0]] = str(var[1]).lower()

def make_common_terms_set():
    global common_terms
    
    cur_fronteer = set([1])
    next_fronteer = set('start')

    while len(next_fronteer) != 0:
        next_fronteer = set()
        for ind in cur_fronteer:
            new_dp_cursor.execute(f"SELECT successor_id FROM factor_graph_edges WHERE predecessor_id = {ind}")
            res = new_dp_cursor.fetchall()
            next_fronteer.update(set(sum(res, ())))
        cur_fronteer = set(next_fronteer)
        common_terms.update(cur_fronteer)


print("\tconstructing prnd-to-local id translation table")
initialize_papers_id_translation_table()
print("\tconstructing factor id-to-name translation table")
initialize_factor_id_to_name_translation_table()
print("\tconstructing set of common terms")
make_common_terms_set()

print("\t\tdone")
for i,f in enumerate(prnd_to_new_translation_table):
    print(f"\t\t\t{f} : {prnd_to_new_translation_table[f]}")
    if i > 4:
        break
print("\t\t\t...")
print()
for i,f in enumerate(factor_id_to_factor_names):
    print(f"\t\t\t{f} : {factor_id_to_factor_names[f]}")
    if i > 4:
        break
print("\t\t\t...")

# сделать метод, который будет возвращать статус трансляционных таблиц

@app.get('/delter/ping')
async def delter_ping():
    pass

@app.get('/delter/translator')
async def grapher_ping():
    return factor_id_to_factor_names
    
# из этого сделать метод, который возвращает статистику по бд
@app.get('/delter/check_new_db')
async def check_new_db():
    sql_query = f"SELECT id FROM publications;"
    t_st = time.monotonic_ns()
    new_dp_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = new_dp_cursor.fetchall()
    
    print(f"got info on {len(publ_result)} publications")
    print(publ_result[0:10])
    
    sql_query = f"SELECT publication_id FROM deltas;"
    t_st = time.monotonic_ns()
    new_dp_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = new_dp_cursor.fetchall()

    print(f"got deltas for {len(set([p[0] for p in publ_result]))} publications")
    print(list(set([p[0] for p in publ_result]))[0:10])

    sql_query = f"SELECT id, level FROM factors;"
    t_st = time.monotonic_ns()
    new_dp_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = new_dp_cursor.fetchall()
    
    if publ_result != []:
        print(f"got info on {len(publ_result)} factors")
        print(f"\tgot info on {len([f for f in publ_result if f[1] == 0])} lvl 0 factors")
        print(f"\tgot info on {len([f for f in publ_result if f[1] == 1])} lvl 1 factors")
        print(f"\tgot info on {len([f for f in publ_result if f[1] == 2])} lvl 2 factors")
        print(f"\tgot info on {len([f for f in publ_result if f[1] == 3])} lvl 3 factors")
    
    # sql_query = f"SELECT factors.id, variant, language_id FROM factors JOIN factor_name_variants ON factors.id = factor_name_variants.factor_id;"
    # t_st = time.monotonic_ns()
    # new_dp_cursor.execute(sql_query)
    # t_end = time.monotonic_ns()
    # print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    # publ_result = new_dp_cursor.fetchall()
    
    # if publ_result != []:
    #     print(publ_result)
        #print(f"got info on {len(publ_result)} factors")

# по локальному id получить id в прнд (если есть)
@app.get('/delter/get_publ_prnd_id/{publ_id}')
async def delter_publ_txt(publ_id : str, request : Request):
    sql_query = f"SELECT id_prime FROM publication WHERE id_publ='{publ_id}';"
    t_st = time.monotonic_ns()
    old_db_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = old_db_cursor.fetchall()

    if publ_result != []: 
        return publ_result[0][0]
    raise HTTPException(status_code=404, detail="Item not found")

# по prnd_id получить профиль публикации
@app.get('/delter/get_publ_deltas/{publ_prnd_id}')
async def get_publ_deltas(publ_prnd_id : str, request : Request):
    sql_query = f"SELECT deltas FROM publication \
                         JOIN deltas ON publication.id_publ = deltas.id_publ \
                         WHERE id_prime='{publ_prnd_id}';"
    t_st = time.monotonic_ns()
    old_db_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = old_db_cursor.fetchall()

    print(len(publ_result))
    
    if publ_result != []:    
        return publ_result[0][0]
    raise HTTPException(status_code=404, detail="Item not found")

# по локальному id получить профиль публикации
@app.get('/delter/get_publ_deltas_local_id/{publ_id}')
async def get_publ_deltas(publ_id : str, request : Request):
    sql_query = f"SELECT deltas FROM deltas WHERE id_publ='{publ_id}';"
    t_st = time.monotonic_ns()
    old_db_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = old_db_cursor.fetchall()
    
    if publ_result != []:    
        return publ_result[0][0]
    raise HTTPException(status_code=404, detail="Item not found")

# по локальному id получить профиль публикации
@app.get('/delter/get_publ_text/{publ_id}')
async def get_publ_deltas(publ_id : str, request : Request):
    sql_query = f"SELECT p_text FROM publication \
                         JOIN publ_text ON publication.id_publ = publ_text.id_publ \
                         WHERE id_prime='{publ_id}';"
    t_st = time.monotonic_ns()
    old_db_cursor.execute(sql_query)
    t_end = time.monotonic_ns()
    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = old_db_cursor.fetchall()
    
    if publ_result != []:    
        return publ_result[0][0]
    raise HTTPException(status_code=201, detail="Item not found")

# по списку id в прнд получить совместный профиль
# 2 параметра, список id и режим, либо возвращаем все профили по отдельности, либо как один слитый профиль
# еще может быть параметр, какие id юзать, локальные или прнд
@app.post('/delter/get_publs_deltas')
async def get_publs_deltas(request : Request):
    global new_db_client, new_dp_cursor

    request_details = await request.json()
    print(request_details)
    publ_id_list = request_details['publ_ids']
    id_type = request_details['id_type'] if 'id_type' in request_details else 'local' # prnd or local
    result_struct = request_details['result'] if 'result' in request_details else 'list' # list or merged
    factor_format = request_details['format'] if 'format' in request_details else 'ids' # ids or names
    factor_level = request_details['level'] if 'level' in request_details else 3 # 0 для терминов, 1 для подфакторов, 2 для факторов
    profile_format = request_details['profile'] if 'profile' in request_details else 'value' # value or stochastic
    common_terms_action = request_details['common_terms'] if 'common_terms' in request_details else 'leave' # leave / mark / remove | mark работает только с текстом

    # если id prnd,то тут надо конвертировать в локальные здесь

    if id_type == 'prnd':
        publ_id_list_formated = ','.join(["\'" + str(prnd_to_new_translation_table[publ_id]) + "\'" for publ_id in publ_id_list if publ_id in prnd_to_new_translation_table])
    else:
        publ_id_list_formated = ','.join(["\'" + str(publ_id) + "\'" for publ_id in publ_id_list])
    
    if len(publ_id_list_formated) != 0:
        sql_query = f"SELECT publication_id, factor_id, value\
                                        FROM deltas\
                                        JOIN factors ON deltas.factor_id = factors.id\
                                        WHERE publication_id IN ({publ_id_list_formated}) AND level= {str(factor_level)};"

        t_st = time.monotonic_ns()
        new_db_client, new_dp_cursor = q_exec(new_db_client, new_dp_cursor, sql_query)
        t_end = time.monotonic_ns()
        publ_result = new_dp_cursor.fetchall()
        print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    else:
        publ_result = []

    t_st = time.monotonic_ns()
    result_dict = {}
    if result_struct == 'merged':
        # строим единый профиль для всего набора публикаций
        for pub, fact, val in publ_result:
            # здесь будем проверять на общенаучность
            if factor_format == 'names':
                # конверсия id факторов в названия
                fact = factor_id_to_factor_names[fact]
            result_dict.setdefault(fact,0)
            result_dict[fact] += val
    if result_struct == 'list':
        # строим по профилю на каждую публикацию
        for pub, fact, val in publ_result:
            if common_terms_action == 'remove' and fact in common_terms:
                continue # убираем общенаучные термины
                         # как нормально маркировать общенаучные термины хз
            # здесь будем проверять на общенаучность
            if factor_format == 'names':
                # конверсия id факторов в названия
                if common_terms_action == 'mark' and fact in common_terms:
                    fact = factor_id_to_factor_names[fact] + chr(0x01)
                else:
                    fact = factor_id_to_factor_names[fact]
            result_dict.setdefault(pub, {})
            result_dict[pub].setdefault(fact,0)
            result_dict[pub][fact] += val
    t_end = time.monotonic_ns()
    print(f"parse responce {(t_end - t_st)/ 1e9} s")

    print(f"\trequested deltas for {len(publ_id_list)} publications")
    print(f"\tgot deltas for {len(result_dict)} publications")

    return result_dict

    #return json.dumps({}, ensure_ascii=False).encode('utf8')


# по списку id в прнд получить совместный профиль
# 2 параметра, список id и режим, либо возвращаем все профили по отдельности, либо как один слитый профиль
# еще может быть параметр, какие id юзать, локальные или прнд
@app.post('/delter/do_the_delting')
async def get_publs_deltas_old(request : Request):

    request_details = await request.json()
    publ_id_list = request_details['publ_ids']
    id_type = request_details['id_type'] if 'id_type' in request_details else 'prnd' # prnd or local
    result_structure = request_details['result'] if 'result' in request_details else 'list' # list or merged

    if len(publ_id_list) == 0:
        return json.dumps({}, ensure_ascii=False).encode('utf8')

    publ_id_list_formated = ','.join(["\'" + str(publ_id) + "\'" for publ_id in publ_id_list])
    
    if id_type == 'prnd':

        sql_query = f"SELECT id_prime, deltas FROM publication \
                                        JOIN deltas ON publication.id_publ = deltas.id_publ \
                                        WHERE id_prime IN ({publ_id_list_formated});"
    else: # если используем локальные id то не нужна таблица publication
        sql_query = f"SELECT deltas FROM deltas WHERE id_publ IN ({publ_id_list_formated});"


    t_st = time.monotonic_ns()
    old_db_cursor.execute(sql_query)
    t_end = time.monotonic_ns()

    print(f"SQL quiery in {(t_end - t_st)/ 1e9} s")
    publ_result = old_db_cursor.fetchall()

    print(len(publ_id_list))
    print(len(publ_result))

    if publ_result != []:
        if result_structure == 'list':
            # возвращаем словарь, где ключи это индексы публикаций, а значения - их дельты
            res_dict = {}
            for i in range(len(publ_result)):
                res_dict[int(json.loads(publ_result[i][0]))] = json.loads(publ_result[i][1])
            return json.dumps(res_dict, ensure_ascii=False).encode('utf8')
        else:
            # возвращаем словарь, где ключи это термины, а значения - их частоты
            res_dict = Counter()
            for i in range(len(publ_result)):
                res_dict.update(json.loads(publ_result[i][1]))
            res_dict = dict(res_dict)
            return json.dumps(res_dict, ensure_ascii=False).encode('utf8')

    return json.dumps({}, ensure_ascii=False).encode('utf8')


@app.get('/delter/produce_classificator_roots')
async def produce_classificator_roots(request : Request):
    global new_db_client, new_dp_cursor
    sql_query = f"SELECT root_id FROM factor_graph_roots "
    new_db_client, new_dp_cursor = q_exec(new_db_client, new_dp_cursor, sql_query)
    res = new_dp_cursor.fetchall()
    return [{'id' : 0, 'name' : 'все', 'id_parent' : -1}] + [{'id' : int(e[0]), 'name' : str(factor_id_to_factor_names[e[0]]), 'id_parent' : -1} for e in res]

@app.post('/delter/produce_classificator_subtree')
async def produce_classificator_subtree(request : Request):
    global new_db_client, new_dp_cursor
    t_st = time.monotonic_ns()
    q_count = 0

    request_details = await request.json()
    print(request_details)
    root_ids = request_details['root_ids']
    req_lvl = request_details['lvl'] if 'lvl' in request_details else None

    if root_ids == [0]:
        root_ids = [1, 211, 1540, 2223]

    result = {
        '0' : [{'id' : 0, 'name' : 'все', 'id_parent' : -1}],
        '1' : [{'id' : 0, 'name' : 'все', 'id_parent' : -1}],
        '2' : [{'id' : 0, 'name' : 'все', 'id_parent' : -1}],
        '3' : [{'id' : 0, 'name' : 'все', 'id_parent' : -1}]
    }

    cur_fronteer = set()
    for root_id in root_ids:
        sql_query = (f"SELECT id, level FROM factors WHERE id= {root_id}")
        new_db_client, new_dp_cursor = q_exec(new_db_client, new_dp_cursor, sql_query)
        q_count += 1
        res = new_dp_cursor.fetchall()

        cur_level = int(res[0][1])
        result[str(cur_level)].append({'id' : int(res[0][0]), 'name' : str(factor_id_to_factor_names[res[0][0]]), 'id_parent' : -1})
    
        cur_fronteer.update(set([(root_id, cur_level)]))

    next_fronteer = set('start')

    while len(next_fronteer) != 0:
        
        next_fronteer = set()
        next_fronteer_lvls = set()
        for ind in cur_fronteer:
            sql_query = f"SELECT successor_id, level, predecessor_id FROM factor_graph_edges \
                                    JOIN factors ON successor_id= factors.id WHERE predecessor_id = {ind[0]}"
            new_db_client, new_dp_cursor = q_exec(new_db_client, new_dp_cursor, sql_query)
            q_count += 1
            res = new_dp_cursor.fetchall()
            next_fronteer.update(set(res))
    
        cur_fronteer = []
        for term in next_fronteer:
            if int(term[0]) == 32: # чтобы предметная область не попадала в общенаучные термины
                continue
            try:
                result[str(term[1])].append({'id' : int(term[0]), 'name' : str(factor_id_to_factor_names[term[0]]), 'id_parent' : int(term[2])})
            except KeyError:
                pass
            if req_lvl is None or term[1] < req_lvl:
                cur_fronteer.append(term)
        cur_fronteer = set(cur_fronteer)

    # result['0'] += [{'id' : 0, 'name' : 'все', 'id_parent' : -1}]
    # result['1'] += [{'id' : 0, 'name' : 'все', 'id_parent' : -1}]
    # result['2'] += [{'id' : 0, 'name' : 'все', 'id_parent' : -1}]
    # result['3'] += [{'id' : 0, 'name' : 'все', 'id_parent' : -1}]
    t_end = time.monotonic_ns()
    print(f"Request completed in {(t_end - t_st)/ 1e9} s, with {q_count} SQL quieries")
    return result[str(req_lvl)] if req_lvl is not None else result