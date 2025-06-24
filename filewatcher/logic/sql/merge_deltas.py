import postgres
import requests
import csv
from psycopg2 import connect as psycopg2Connect

def merge_deltas(db_cursor, factor_ids, result_id):
    # Создаем временную таблицу temp_deltas
    db_cursor.execute("CREATE TEMPORARY TABLE temp_deltas (LIKE sim0n_deltas INCLUDING ALL);")

    # Копируем данные из deltas в temp_deltas и объединяем их по терминам
    query = f"""
        INSERT INTO temp_deltas (publication_id, factor_id, value, stochastic)
        SELECT publication_id, {result_id}, value, SUM(stochastic)
        FROM sim0n_deltas
        WHERE factor_id IN {tuple(factor_ids)}
        GROUP BY publication_id, value;
    """
    db_cursor.execute(query)

    # Удаляем объединенные записи из deltas
    db_cursor.execute(f"DELETE FROM sim0n_deltas WHERE factor_id IN {tuple(factor_ids)};")

    # Обновляем таблицу deltas данными из temp_deltas
    db_cursor.execute("INSERT INTO sim0n_deltas SELECT * FROM temp_deltas;")

    # Удаляем временную таблицу temp_deltas
    db_cursor.execute("DROP TABLE temp_deltas;")

    # Фиксируем изменения
    conn_account.commit()

if __name__ == '__main__':  
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )
    db_cursor = conn_account.cursor()  
    
    '''
    factor_ids = [467, 468, 470, 471]    
    result_id = 470
    '''
    factor_ids = [2574, 2583]    
    result_id = 2583

    merge_deltas(db_cursor, factor_ids, result_id)

    db_cursor.close()
    conn_account.close()