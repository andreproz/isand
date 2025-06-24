import postgres
import requests
import csv
from psycopg2 import connect as psycopg2Connect
#from python.deltas import deltas_run
#from python.filecore import getDirByID

if __name__ == '__main__':
    #isandDB
    #account_db
    # Connect to the account_db database
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )  

    # Создание курсора
    cursor = conn_account.cursor()   

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'") # таблицы
    publications_response = cursor.fetchall()
    print(publications_response)

    for res in publications_response:
        print(res)

    # Закрытие курсора и соединения с базой данных
    cursor.close()
    conn_account.close()