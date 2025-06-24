import json
import requests
import sys
import os

from psycopg2 import connect as psycopg2Connect

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql.config import DBNAME, USER, HOST, PORT, PASSWORD

def getData(prnd_id):
    #http://193.232.208.28/api/v1.0/publications/list?id=25685
    url = 'http://193.232.208.28/api/v1.0/publications/list?prnd_id='
    url += str(prnd_id)
    if not url: return None
    
    data = None
    response = requests.get(url)
    if not response: return None
    data = response.json()
    return data


def main():
    # Подключение к базе данных
    conn = psycopg2Connect(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )

    cur = conn.cursor()

    cur.execute("SELECT id FROM publications")

    ids = [row[0] for row in cur.fetchall()]

    for id in ids:
        cur.execute("SELECT prnd_id FROM publication_mapping_prnd WHERE publication_id=%s", (id,))

        prnd_id = cur.fetchone()

        if prnd_id:
            prnd_id = prnd_id[0]
            url = f'http://193.232.208.28/api/v1.0/publications/list?prnd_id={prnd_id}'
            #print(f'Sending request to {url}')
            response = requests.get(url)

            if response.ok:
                data = response.json()
                publ_type = data[0]['publ_type']
                print("publ_type", publ_type)

                # Проверяем, существует ли уже тип публикации в таблице publication_type
                cur.execute("SELECT * FROM publication_type WHERE name=%s", (publ_type,))
                row = cur.fetchone()
                if not row:
                    # Тип публикации не существует, добавляем его в таблицу publication_type
                    cur.execute("INSERT INTO publication_type (name) VALUES (%s)", (publ_type,))
                    conn.commit()
                    print(f'Publication type {publ_type} added to table publication_type')

                # Получаем id типа публикации из таблицы publication_type
                cur.execute("SELECT id FROM publication_type WHERE name=%s", (publ_type,))
                publ_type_id = cur.fetchone()[0]

                # Обновляем поле publication_type_id в таблице publications
                cur.execute("UPDATE publications SET publication_type_id=%s WHERE id=%s", (publ_type_id, id,))
                conn.commit()
                print(f'Publication type id for id {id} updated to {publ_type_id}')

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()

   
    
