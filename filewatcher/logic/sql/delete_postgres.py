import postgres
import requests
from psycopg2 import connect as psycopg2Connect

def delete_pubs(prnd_id=None, pub_id=None, source_name=None):
    print("prnd_id", prnd_id, "pub_id", pub_id, "source_name", source_name)

    # Connect to the account_db database
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    cur = conn_account.cursor()

    if prnd_id:
        cur.execute("""
            DELETE FROM deltas
            WHERE publication_id IN (
                SELECT publications.id FROM publications WHERE prnd_id = ANY(%s)
            )
        """, (prnd_id,))
    if pub_id:
        cur.execute("""
            DELETE FROM deltas
            WHERE publication_id IN (
                SELECT publications.id FROM publications WHERE id = ANY(%s)
            )
        """, (pub_id,))
    if source_name:
        cur.execute("""
            DELETE FROM deltas
            WHERE publication_id IN (
                SELECT publications.id FROM publications
                JOIN publication_sources ON publications.publication_source_id = publication_sources.id
                JOIN conferences ON publication_sources.conference_id = conferences.id
                WHERE conferences.name = ANY(%s)
            )
        """, (source_name,))

    # Удаление записей из таблицы publication_mapping_prnd, которые ссылаются на удаляемые публикации
    if prnd_id:
        cur.execute("""
            DELETE FROM publication_mapping_prnd
            WHERE prnd_id = ANY(%s)
        """, (prnd_id,))
  
    # Удаление записей из таблицы author_to_publications, которые ссылаются на удаляемые публикации
    if prnd_id:
        cur.execute("""
            DELETE FROM author_to_publications
            WHERE publication_id IN (
                SELECT publications.id FROM publications WHERE prnd_id = ANY(%s)
            )
        """, (prnd_id,))
    if pub_id:
        cur.execute("""
            DELETE FROM author_to_publications
            WHERE publication_id IN (
                SELECT publications.id FROM publications WHERE id = ANY(%s)
            )
        """, (pub_id,))
    if source_name:
        cur.execute("""
            DELETE FROM author_to_publications
            WHERE publication_id IN (
                SELECT publications.id FROM publications
                JOIN publication_sources ON publications.publication_source_id = publication_sources.id
                JOIN conferences ON publication_sources.conference_id = conferences.id
                WHERE conferences.name = ANY(%s)
            )
        """, (source_name,))


    # Удаление записей из таблицы publications, которые соответствуют условиям удаления
    if prnd_id:
        cur.execute("""
            DELETE FROM publications WHERE prnd_id = ANY(%s)
        """, (prnd_id,))
    if pub_id:
        cur.execute("""
            DELETE FROM publications WHERE id = ANY(%s)
        """, (pub_id,))
    if source_name:
        cur.execute("""
            DELETE FROM publications
            WHERE publication_source_id IN (
                SELECT id FROM publication_sources
                WHERE conference_id IN (
                    SELECT id FROM conferences WHERE name = ANY(%s)
                )
            )
        """, (source_name,))

    # Удаление записей из таблицы publication_sources, которые ссылаются на удаляемые публикации
    if prnd_id:
        cur.execute("""
            DELETE FROM publication_sources
            WHERE id IN (
                SELECT publication_source_id FROM publications WHERE prnd_id = ANY(%s)
            )
        """, (prnd_id,))
    if pub_id:
        cur.execute("""
            DELETE FROM publication_sources
            WHERE id IN (
                SELECT publication_source_id FROM publications WHERE id = ANY(%s)
            )
        """, (pub_id,))
    if source_name:
        cur.execute("""
            DELETE FROM publication_sources
            WHERE conference_id IN (
                SELECT conferences.id FROM conferences WHERE name = ANY(%s)
            )
        """, (source_name,))

    # Удаление записей из таблицы raw_author_data, которые ссылаются на удаляемые raw_publication_data
    if prnd_id:
        cur.execute("""
            DELETE FROM raw_author_data
            WHERE raw_publication_data_id IN (
                SELECT raw_publication_data.id FROM raw_publication_data
                WHERE id IN (
                    SELECT raw_data_id FROM publications WHERE prnd_id = ANY(%s)
                )
            )
        """, (prnd_id,))
    if pub_id:
        cur.execute("""
            DELETE FROM raw_author_data
            WHERE raw_publication_data_id IN (
                SELECT raw_publication_data.id FROM raw_publication_data
                WHERE id IN (
                    SELECT raw_data_id FROM publications WHERE id = ANY(%s)
                )
            )
        """, (pub_id,))
    if source_name:
        cur.execute("""
            DELETE FROM raw_author_data
            WHERE raw_publication_data_id IN (
                SELECT raw_publication_data.id FROM raw_publication_data
                WHERE id IN (
                    SELECT raw_data_id FROM publications
                    JOIN publication_sources ON publications.publication_source_id = publication_sources.id
                    JOIN conferences ON publication_sources.conference_id = conferences.id
                    WHERE conferences.name = ANY(%s)
                )
            )
        """, (source_name,))

    # Удаление записей из таблицы raw_publication_data, которые ссылаются на удаляемые публикации
    if prnd_id:
        cur.execute("""
            DELETE FROM raw_publication_data
            WHERE id IN (
                SELECT raw_data_id FROM publications WHERE prnd_id = ANY(%s)
            )
        """, (prnd_id,))
    if pub_id:
        cur.execute("""
            DELETE FROM raw_publication_data
            WHERE id IN (
                SELECT raw_data_id FROM publications WHERE id = ANY(%s)
            )
        """, (pub_id,))
    if source_name:
        cur.execute("""
            DELETE FROM raw_publication_data
            WHERE id IN (
                SELECT raw_data_id FROM publications
                JOIN publication_sources ON publications.publication_source_id = publication_sources.id
                JOIN conferences ON publication_sources.conference_id = conferences.id
                WHERE conferences.name = ANY(%s)
            )
        """, (source_name,))

    # Commit the changes and close the connection
    conn_account.commit()
    cur.close()
    conn_account.close()

if __name__ == '__main__':
    prnd_id = [76832, 1998, 76828]
    conf_names = ['dccn', 'pubss', 'mkpu', 'avtprom', 'stab']
    delete_pubs(prnd_id=prnd_id, source_name=conf_names)
