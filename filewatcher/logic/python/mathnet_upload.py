import requests
import psycopg2
import io
from typing import *
import spacy
from psycopg2 import connect as psycopg2Connect
import json


from deltas import get_deltas_stochastic_from_mathnet


# Подключение к базе данных
connection_account_db = psycopg2.connect(
    dbname='account_db',
    user='isand',
    host='193.232.208.58',
    port='5432',
    password='sf3dvxQFWq@!'
)

cursor_account_db = connection_account_db.cursor()

# Функция для получения данных из API и вставки в таблицу
def insert_publication_mapping(journal_id):
    url = f"https://www.mathnet.ru/api/journals/{journal_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        papers = data.get('papers', [])
        
        for paper in papers:
            jrnid = paper.get('jrnid')
            paperid = paper.get('paperid')
            pub_id = None  # Пока оставляем как None

            cursor_account_db.execute("""
                INSERT INTO publication_mapping_mathnet (jrnid, paperid, pub_id)
                VALUES (%s, %s, %s)
            """, (jrnid, paperid, pub_id))
        
        connection_account_db.commit()
        print(f"Data for journal {journal_id} inserted successfully.")
    else:
        print(f"Failed to fetch data for journal {journal_id}. Status code: {response.status_code}")

def update_publications():
    # Извлечение всех записей из publication_mapping_mathnet
    cursor_account_db.execute("SELECT id, jrnid, paperid, pub_id FROM publication_mapping_mathnet")
    records = cursor_account_db.fetchall()

    for record in records:
        print(record)
        mapping_id, jrnid, paperid, pub_id = record
        url = f"https://www.mathnet.ru/api/journals/{jrnid}{paperid}"
        response = requests.get(url)
        
        if response.status_code == 200:
            try:
                data = response.json()
            except json.decoder.JSONDecodeError as e:
                print(f"JSON decoding failed for {url}: {e}")  # Логируем ошибку
                continue  # Продолжаем с следующей записью

            # Если pub_id равно None, создаём новую запись в таблице publications
            if pub_id is None:
                cursor_account_db.execute("""
                    INSERT INTO publications (mathnet_id)
                    VALUES (%s) RETURNING id
                """, (mapping_id,))
                pub_id = cursor_account_db.fetchone()[0]
                cursor_account_db.execute("""
                    UPDATE publication_mapping_mathnet
                    SET pub_id = %s
                    WHERE id = %s
                """, (pub_id, mapping_id))
                connection_account_db.commit()
                print("pub_id", pub_id) 
            

            # Определение заголовка публикации
            title = data.get('title_tex_rus') or data.get('title_tex_eng')

            # Определение аннотации публикации
            annotation = data.get('abstract_tex_rus') or data.get('abstract_tex_eng')

            # Обработка авторов и аффилиаций
            for author in data.get('authors', []):
                print(f"author['lname_rus']: {author['lname_rus']}, author['sname_rus']: {author['sname_rus']}, author['fname_rus']: {author['fname_rus']}")
                cursor_account_db.execute("""
                    SELECT id FROM authors WHERE last_name = %s AND first_name = %s AND middle_name = %s
                """, (author['lname_rus'], author['fname_rus'], author['sname_rus']))
                author_record = cursor_account_db.fetchone()

                if author_record is None:
                    cursor_account_db.execute("""
                        INSERT INTO authors (last_name, first_name, middle_name)
                        VALUES (%s, %s, %s) RETURNING id
                    """, (author['lname_rus'], author['fname_rus'], author['sname_rus']))
                    author_id = cursor_account_db.fetchone()[0]
                else:
                    author_id = author_record[0]

                for affiliation in author.get('affiliations', []):
                    cursor_account_db.execute("""
                        SELECT id FROM organizations WHERE base_name = %s
                    """, (affiliation['orgname_rus'],))
                    org_record = cursor_account_db.fetchone()

                    if org_record:
                        organization_id = org_record[0]
                        cursor_account_db.execute("""
                            INSERT INTO affiliations (organizations_id, department)
                            VALUES (%s, '') RETURNING id
                        """, (organization_id,))
                        affiliation_id = cursor_account_db.fetchone()[0]

                        cursor_account_db.execute("""
                            INSERT INTO author_to_publications (author_id, publication_id, place, affiliation_id)
                            VALUES (%s, %s, 1, %s)
                        """, (author_id, pub_id, affiliation_id))

            year = data['russian_version'].get('year')
            formatted_date = f"{year}-01-01" if year else None  # предполагаем 1 января, если есть только год

            # Обновление записи в таблице publications
            cursor_account_db.execute("""
                UPDATE publications
                SET title = %s, annotation = %s, publication_date = %s, doi = %s
                WHERE id = %s
            """, (title, annotation, formatted_date, data['russian_version'].get('doi'), pub_id))            

            # Обновление publication_sources
            volume = int(data['russian_version']['volume']) if data['russian_version']['volume'] and data['russian_version']['volume'].isdigit() else None
            issue = int(data['russian_version']['issue']) if data['russian_version']['issue'] and data['russian_version']['issue'].isdigit() else None
            
            cursor_account_db.execute("""
                UPDATE publication_sources SET volume = %s, issue = %s
                WHERE id = (SELECT publication_source_id FROM publications WHERE id = %s)
            """, (volume, issue, pub_id))

            connection_account_db.commit()
            print(f"Updated publication with id {pub_id}")
        else:
            print(f"Failed to fetch data for jrnid {jrnid} and paperid {paperid}. Status code: {response.status_code}") 


# Запуск обновления
update_publications()

'''
mathnet_journals = ["tvp", "mm", "ia", "pp", "ssi", "umj", "da", "crm", "vmumm", "vspua", "vspui", "ipmp", "mbb", "ps", "trspy", "mgta", "itvs"]

for math_journal in mathnet_journals:
    # Пример использования функции для конкретного журнала
    insert_publication_mapping(math_journal)
'''

# Закрытие соединения с базой данных
cursor_account_db.close()
connection_account_db.close()
