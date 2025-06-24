import postgres
import requests
import csv
from psycopg2 import connect as psycopg2Connect
#from python.deltas import deltas_run
#from python.filecore import getDirByID

if __name__ == '__main__':

    #account_db
    connection_account_db = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    ) 
    
    cursor_account_db = connection_account_db.cursor()   

    print("CREATE TABLE temp_deltas")
    cursor_account_db.execute("""
    CREATE TABLE temp_deltas (
        id SERIAL PRIMARY KEY,
        publication_id INTEGER,
        factor_id INTEGER,
        value DOUBLE PRECISION,
        stochastic DOUBLE PRECISION
    );
    """) 

    print("INSERT INTO temp_deltas")
    cursor_account_db.execute("""
    INSERT INTO temp_deltas (publication_id, factor_id, value, stochastic)
    SELECT publication_id, factor_id, value, stochastic FROM sim0n_deltas;
    """) 

    print("TRUNCATE deltas;")
    cursor_account_db.execute("""
    TRUNCATE deltas;
    """) 

    print("INSERT INTO deltas;")
    cursor_account_db.execute("""
    INSERT INTO deltas (publication_id, factor_id, value, stochastic)
    SELECT publication_id, factor_id, value, stochastic FROM temp_deltas;
    """) 

    print("DROP TABLE temp_deltas;")
    cursor_account_db.execute("""
    DROP TABLE temp_deltas;
    """)   

    connection_account_db.commit()

    cursor_account_db.close() 
    connection_account_db.close()