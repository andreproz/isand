from psycopg2 import connect as psycopg2Connect
import pandas as pd
import csv

def setColumnsValues(zip_col_val: tuple[str, str]) -> str:
    set_string: str = ''
    for cond in zip_col_val:
        if type(cond[1]) == str and cond[1][-1] != '$':
            set_string += f'{cond[0]} = $${cond[1]}$$, '
        else:
            set_string += f'{cond[0]} = {cond[1]}, '
    return set_string[:-2]

class SQLQuery:
    def __init__(self, dbname, user, host, port, password=''):
        self.dbname = dbname
        self.user = user
        self.host = host
        self.port = port
        self.password = password
        self.conn = psycopg2Connect(
            dbname=self.dbname,
            user=self.user,
            host=self.host,
            port=self.port,
            password=self.password
        )
    
    def __del__(self):
        self.conn.close()
    
    def insert(self, table, columns, values, returning=['*']):
        placeholders = ', '.join(['%s' for _ in values])
        columns_str = ', '.join(columns)
        returning_str = ', '.join(returning)
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) RETURNING {returning_str};"
        values = [None if v == '' else v for v in values]
        self.cur = self.conn.cursor()
        self.cur.execute(query, tuple(values))
        result = self.cur.fetchone()
        self.conn.commit()
        self.cur.close()
        return result
        
    def select(self, table, columns=['*'], where_keys=None, where_values=None, where_operator='OR'):
        columns = ', '.join([str(x) for x in columns])
        query = f"SELECT {columns} FROM {table}"
        if where_keys and where_values and len(where_keys) == len(where_values):
            where_clauses = [f"{key} = %s" for key in where_keys]
            where_clause = f" {where_operator} ".join(where_clauses)
            query += f" WHERE {where_clause}"
        else:
            None
        self.cur = self.conn.cursor()
        self.cur.execute(query, tuple(where_values))
        result = self.cur.fetchall()
        self.conn.commit()
        self.cur.close()
        return result
            
    def update(self, table, columns, values, returning=['*'], where=''):
        set_clauses = ', '.join([f"{col} = %s" for col in columns])
        returning_str = ', '.join(returning)
        query = f"UPDATE {table} SET {set_clauses} {where} RETURNING {returning_str};"
        values = [None if v == '' else v for v in values]
        self.cur = self.conn.cursor()
        self.cur.execute(query, tuple(values))
        result = self.cur.fetchone()
        self.conn.commit()
        self.cur.close()
        return result
    
    def delete(self, table, columns, values, returning=['*']):
        columns = [str(x) for x in columns]
        values = [str(x) for x in values]
        returning = [str(x) for x in returning]
        self.cur = self.conn.cursor()
        self.cur.execute(f"DELETE FROM {table}{where};")
        result = self.cur.fetchone()
        self.conn.commit()
        self.cur.close()
        return result
    
'''
postgres = SQLQuery(
    dbname='account_db',
    user='isand',
    host='193.232.208.58',
    port='5432',
    password='sf3dvxQFWq@!'
)
'''
    
if __name__ == '__main__':

    #isandDB
    connection_isandDB = psycopg2Connect(
        dbname='isandDB',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )
    cursor_isandDB = connection_isandDB.cursor()  

    #account_db
    connection_account_db = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    ) 
    
    cursor_account_db = connection_account_db.cursor()        
    
    cursor_account_db.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'publication_mapping_dk' AND table_schema = 'public'")
    response = cursor_account_db.fetchall()
    print("response", response)

    cursor_isandDB.close() 
    cursor_account_db.close() 
    connection_account_db.close()   
    connection_isandDB.close()   