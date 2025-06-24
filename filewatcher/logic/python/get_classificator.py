import json
from psycopg2 import connect as psycopg2Connect

def get_factors(cursor):
    factors = dict()
    cursor.execute("SELECT id, level FROM factors")
    for record in cursor.fetchall():
        factors[record[0]] = {'level': record[1]}
    return factors

def get_factor_graph_edges(cursor):
    factor_graph_edges_predecessors = dict()
    factor_graph_edges_successors = dict()

    cursor.execute("SELECT predecessor_id, successor_id FROM factor_graph_edges")
    for record in cursor.fetchall():
        # Добавляем потомков
        factor_graph_edges_successors.setdefault(record[0], []).append(record[1])
        # Добавляем предков
        factor_graph_edges_predecessors.setdefault(record[1], []).append(record[0])

    return factor_graph_edges_predecessors, factor_graph_edges_successors

def get_factor_name_variants(cursor):
    factor_name_variants = dict()
    cursor.execute("SELECT factor_id, variant, language_id FROM factor_name_variants")
    for record in cursor.fetchall():
        if record[0] not in factor_name_variants:
            factor_name_variants[record[0]] = {}
        factor_name_variants[record[0]][record[2]] = record[1]
    return factor_name_variants

def filter_factors(factors, factor_graph_edges_predecessors, factor_graph_edges_successors, factor_name_variants):
    factor_ids = set(factors.keys()) & set(factor_name_variants.keys())
    new_factors = {fid: factors[fid] for fid in factor_ids}
    new_factor_graph_edges_predecessors = {fid: factor_graph_edges_predecessors.get(fid, None) for fid in factor_ids}
    new_factor_graph_edges_successors = {fid: factor_graph_edges_successors.get(fid, None) for fid in factor_ids}
    new_factor_name_variants = {fid: factor_name_variants[fid] for fid in factor_ids}
    
    return new_factors, new_factor_graph_edges_predecessors, new_factor_graph_edges_successors, new_factor_name_variants

def get_classificator():
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )
    cursor = conn_account.cursor()

    factors = get_factors(cursor)
    factor_graph_edges_predecessors, factor_graph_edges_successors = get_factor_graph_edges(cursor)
    factor_name_variants = get_factor_name_variants(cursor)

    factors, predecessors, successors, name_variants = filter_factors(
        factors, factor_graph_edges_predecessors, factor_graph_edges_successors, factor_name_variants
    )

    # Создание JSON
    json_classificator = {}
    for fid in factors:
        json_classificator[fid] = [
            factors[fid]['level'],
            predecessors.get(fid),
            successors.get(fid),
            name_variants[fid]
        ]

    cursor.close()
    conn_account.close()

    return json.dumps(json_classificator, ensure_ascii=False)

if __name__ == '__main__':
    json_classificator = get_classificator()
    #print("json_classificator", json_classificator)