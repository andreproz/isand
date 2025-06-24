import postgres
import requests
import csv
from psycopg2 import connect as psycopg2Connect
#from python.deltas import deltas_run

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

    # Execute the query
    cursor.execute("SELECT * FROM journals")

    # Open the CSV file for writing
    print("open csv")
    with open('/home/unreal_dodic/filewatcher/logic/sql/backups/deltas_journals_202406170337.csv', 'w', newline='') as f:
        writer = csv.writer(f)

        # Write the header row
        writer.writerow([desc[0] for desc in cursor.description])

        # Write the data rows
        writer.writerows(cursor)

 
    cursor.close()
    conn_account.close()