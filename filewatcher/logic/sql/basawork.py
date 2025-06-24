from datetime import datetime
from os.path import join, exists, dirname, basename, relpath,splitext, isdir
from psycopg2.extras import Json
from psycopg2 import connect as psycopg2Connect
import json

from .config import DBNAME, USER, HOST, PORT, PASSWORD
from .postgres import SQLQuery 
    
def add_new_key(journal):
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )

    id_user = None
    id_res = postgres.select(table='user_isand', 
                            columns=['id_user'], 
                            where_keys=['org_name'],
                            where_values=[journal.lower()]) 
    if id_res: id_user = id_res[0][0]
    else: 
        id_res = postgres.insert(table='user_isand', 
                    columns=['org_name'], 
                    values=[journal.lower()])
        id_user = id_res[0]
    return id_user

def upload2DB(parametres, is_article_good = False):
    conn_account = psycopg2Connect(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    cur = conn_account.cursor()

    print("parametres", parametres)
    grobid_authors = parametres.get("grobid_authors", None)
    grobid_title = parametres.get('grobid_title', None)
    prnd_key = parametres.get("prnd_key", None)
    pub_authors = parametres.get('authors', None)
    pub_creation_date = parametres.get('creation_date', None)
    pub_doi = parametres.get('doi', None)
    pub_title = parametres.get('title', None)
    pub_year = parametres.get('publication_date', None)
    pub_conference = parametres.get('conference_name', None)
    pub_journal = parametres.get('journal_name', None) 
    pub_deltas = parametres.get('deltas', None) 

    if prnd_key:
            print(f"Check {prnd_key}")   
            cur.execute("SELECT prnd_id FROM publication_mapping_prnd WHERE id=%s;", (prnd_key,))
            result = cur.fetchone()

            prnd_id = result[0] if result else None

            if prnd_id: 
                print(f"{prnd_id} already exist in DB")
                return None

    pub_id = None
    # Заливка нового id-шника, если его нет.
    if is_article_good == True:
        cur.execute("SELECT id, raw_data_id FROM publications WHERE title = %s;", (pub_title,))
        result = cur.fetchone()
        print("result", result)

        pub_id = result[0] if result else None
        raw_data_id = result[1] if result else None

        print("INSERT:", "pub_title", pub_title, "pub_doi", pub_doi)
        if not pub_id:        
            if  pub_title:
                if pub_doi:
                    cur.execute("INSERT INTO publications (title, doi) VALUES (%s, %s) RETURNING id;", (pub_title, pub_doi))
                else:  
                    cur.execute("INSERT INTO publications (title) VALUES (%s) RETURNING id;", (pub_title,))     
            conn_account.commit()
            pub_id = cur.fetchone()[0]

        # Загрузка новых в raw_publication_data или обновление существующих
        if raw_data_id:
            cur.execute("UPDATE raw_publication_data SET title=%s, creation_date=%s, doi=%s WHERE id=%s;",
                        (pub_title, pub_creation_date, pub_doi, raw_data_id))
        else:
            cur.execute("INSERT INTO raw_publication_data (title, creation_date, doi) VALUES (%s, %s, %s) RETURNING id;",
                        (pub_title, pub_creation_date, pub_doi))    
            raw_data_id = cur.fetchone()[0]
        conn_account.commit()

        # Обновление raw_data_id в таблице publications
        cur.execute("UPDATE publications SET raw_data_id=%s WHERE id=%s;", (raw_data_id, pub_id))

        # Добавление и обновление записей в raw_author_data
        for author in pub_authors:
            author_fio = author[0]
            author_last = author[1]
            author_first = author[2]
            author_sec = author[3]

            cur.execute("SELECT id FROM raw_author_data WHERE raw_publication_data_id=%s AND name=%s;", (raw_data_id, author_fio))
            result = cur.fetchone()

            author_id = result[0] if result else None

            if not author_id:
                cur.execute("INSERT INTO raw_author_data (raw_publication_data_id, name) VALUES (%s, %s) RETURNING id;", (raw_data_id, author_fio))
                author_id = cur.fetchone()[0]
            else:
                cur.execute("UPDATE raw_author_data SET name=%s WHERE id=%s;", (author_fio, author_id))
            conn_account.commit()

            # Добавление и обновление записей в authors и author_to_publications
            cur.execute("SELECT id FROM authors WHERE first_name=%s AND last_name=%s AND middle_name=%s;", (author_first, author_last, author_sec))
            result = cur.fetchone()

            author_id = result[0] if result else None

            if not author_id:
                cur.execute("INSERT INTO authors (first_name, last_name, middle_name) VALUES (%s, %s, %s) RETURNING id;", (author_first, author_last, author_sec))
                conn_account.commit()
                author_id = cur.fetchone()[0]
            conn_account.commit()

            cur.execute("SELECT id FROM author_to_publications WHERE author_id=%s AND publication_id=%s;", (author_id, pub_id))
            result = cur.fetchone()

            atp_id = result[0] if result else None

            if not atp_id:
                cur.execute("INSERT INTO author_to_publications (author_id, publication_id, place) VALUES (%s, %s, %s) RETURNING id;", (author_id, pub_id, 0))
            else:
                cur.execute("UPDATE author_to_publications SET author_id=%s, publication_id=%s WHERE id=%s;", (author_id, pub_id, atp_id))
            conn_account.commit()
    else:
        # Добавление в raw_publication_data
        print("INSERT:", "grobid_title", grobid_title, "pub_creation_date", pub_creation_date)
        creation_date = pub_creation_date 
        if len(grobid_title) > 0:
            cur.execute("INSERT INTO raw_publication_data (title, creation_date) VALUES (%s, %s) RETURNING id;", (grobid_title, creation_date))     
        elif pub_creation_date:
            cur.execute("INSERT INTO raw_publication_data (creation_date) VALUES (%s) RETURNING id;", (creation_date, ))     
        conn_account.commit()

        result = cur.fetchone()
        raw_data_id = result[0] if result else None
                    
        # Добавление в publications 
        cur.execute("SELECT id FROM publications WHERE raw_data_id=%s;", (raw_data_id,))
        result = cur.fetchone()
        
        pub_id = result[0] if result else None
        if not pub_id:
                if raw_data_id:
                    cur.execute("INSERT INTO publications (raw_data_id) VALUES (%s) RETURNING id;", (raw_data_id,))
                elif prnd_key:
                    cur.execute("INSERT INTO publications (prnd_id) VALUES (%s) RETURNING id;", (prnd_key,))
                pub_id = cur.fetchone()[0]
        conn_account.commit()

        # Добавление и обновление записей в raw_author_data
        for author in grobid_authors:
            cur.execute("SELECT id FROM raw_author_data WHERE raw_publication_data_id=%s AND name=%s;", (raw_data_id, author))
            result = cur.fetchone()

            author_id = result[0] if result else None

            if not author_id:
                cur.execute("INSERT INTO raw_author_data (raw_publication_data_id, name) VALUES (%s, %s) RETURNING id;", (raw_data_id, author))
                author_id = cur.fetchone()[0]
            else:
                cur.execute("UPDATE raw_author_data SET name=%s WHERE id=%s;", (author, author_id))
            conn_account.commit()
        
        # Добавление в publication_mapping_prnd
        if prnd_key:   
            cur.execute("SELECT prnd_id FROM publication_mapping_prnd WHERE id=%s;", (prnd_key,))
            result = cur.fetchone()

            prnd_id = result[0] if result else None

            if not prnd_id:
                cur.execute("INSERT INTO publication_mapping_prnd (id, prnd_id) VALUES (%s, %s) RETURNING prnd_id;", (pub_id, prnd_key))
                prnd_id = cur.fetchone()[0]
            else:
                cur.execute("UPDATE publication_mapping_prnd SET id=%s WHERE prnd_id=%s;", (pub_id, prnd_key))
            #conn_account.commit()
            print("prnd_id", prnd_id) 

        # Заполнение publication_date
        if pub_year: 
            #creation_date = datetime.strptime(pub_creation_date, '%d.%m.%Y %H:%M:%S') 
            bd_pub_year = datetime.strptime(str(pub_year), '%Y')  
            cur.execute("UPDATE publications SET publication_date=%s WHERE id=%s;", (bd_pub_year, pub_id))
            #conn_account.commit() 
    
        # Заполнение источника-конференций
        if pub_conference: 
            cur.execute("SELECT id FROM conferences WHERE name=%s;", (pub_conference,))
            result = cur.fetchone()

            pub_conf_id = result[0] if result else None
            if not pub_conf_id:
                cur.execute("INSERT INTO conferences (name, full_name) VALUES (%s, %s) RETURNING id;", (pub_conference,""))
                pub_conf_id = cur.fetchone()[0]
                #conn_account.commit()

            cur.execute("INSERT INTO publication_sources (conference_id) VALUES (%s) RETURNING id;", (pub_conf_id,))
            pub_source_id = cur.fetchone()[0]
            #conn_account.commit()

            cur.execute("UPDATE publications SET publication_source_id=%s WHERE id=%s;", (pub_source_id, pub_id))
            #conn_account.commit()

        # Заполнение источника-журнала
        if pub_journal: 
            cur.execute("SELECT id FROM journals WHERE name=%s;", (pub_journal,))
            result = cur.fetchone()

            pub_journal_id = result[0] if result else None
            if not pub_journal_id:
                cur.execute("INSERT INTO journals (name, full_name) VALUES (%s, %s) RETURNING id;", (pub_journal,""))
                pub_journal_id = cur.fetchone()[0]
                #conn_account.commit()

            cur.execute("INSERT INTO publication_sources (journal_id) VALUES (%s) RETURNING id;", (pub_journal_id,))
            pub_source_id = cur.fetchone()[0]
            #conn_account.commit()

            cur.execute("UPDATE publications SET publication_source_id=%s WHERE id=%s;", (pub_source_id, pub_id))
            #conn_account.commit()

        # Заполнение дельт
        if pub_deltas:
            # Итерация по словарю с результатами подсчета
            for factor_id, value in pub_deltas.items():
                print("factor_id", factor_id, "value", value)
                cur.execute("SELECT * FROM deltas WHERE publication_id=%s AND factor_id=%s", (pub_id, factor_id))
                result = cur.fetchone()
                if result is None:
                    cur.execute("INSERT INTO deltas (publication_id, factor_id, value, stochastic) VALUES (%s, %s, %s, %s)", (pub_id, factor_id, value, 0))
                else:
                    cur.execute("UPDATE deltas SET value=%s WHERE publication_id=%s AND factor_id=%s", (value, pub_id, factor_id))

        conn_account.commit()
    
    cur.close() 
    conn_account.close()   

    return pub_id
 
    
if __name__ == '__main__':
    journal_name = "test_journal"
    id_user = add_new_key(journal_name)
    pass