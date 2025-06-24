from typing import *
import requests
import sys
import os
import numpy as np
import json
from functools import wraps
import time
import psycopg2
from psycopg2 import pool
PATH_TO_PUB = '/home/isand_user/isand/web_application/back/struct_pub/'

connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 5, dbname='account_db', user='isand', host='193.232.208.58', port='5432', password='sf3dvxQFWq@!'
)

# Функция для получения isand_id по author_id из базы данных
def get_author_isand_id(author_id):
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT isand_id FROM author_mapping_prnd WHERE author_id = %s", (author_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        connection_pool.putconn(connection)

# Функция для получения isand_id по conference_id из базы данных
def get_conference_isand_id(conference_id):
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT isand_id FROM conferences WHERE id = %s", (conference_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        connection_pool.putconn(connection)

# Функция для получения isand_id по journal_id из базы данных
def get_journal_isand_id(journal_id):
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT isand_id FROM journals WHERE id = %s", (journal_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        connection_pool.putconn(connection)

# Функция для получения isand_id по organization_id из базы данных
def get_organization_isand_id(organization_id):
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT isand_id FROM organizations WHERE id = %s", (organization_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        connection_pool.putconn(connection)

# Функция для получения isand_id по city_id из базы данных
def get_city_isand_id(city_id):
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT isand_id FROM cities WHERE id = %s", (city_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        connection_pool.putconn(connection)

# Функция для получения prnd_id публикации из базы данных
def get_prnd_id(dk_id):
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT p.prnd_id
            FROM publications p
            JOIN publication_mapping_dk pmd ON p.id = pmd.publication_id
            WHERE pmd.dk_id = %s
        """, (dk_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        connection_pool.putconn(connection)

def get_labs_pubs_dict(labs: Iterable[str], years: list = None) -> Iterable:
    if not years:
        years = [1900, 2100]
    labs_pubs_dict: Mapping[str, int] = {}
    pubs_names_dict: Mapping[int, str] = {}
    for lab in labs:
        pub_request = requests.get(
            f'http://193.232.208.28/api/v2.0/division/get_publications?id={lab}')
        pubs_list = []
        if pub_request.status_code == 200:
            try:
                response_data = pub_request.json()
                for entry in response_data:
                    publ_year: int = int(entry['year'])
                    #print(publ_year, years)
                    if publ_year < years[0] or publ_year > years[1]:
                        continue
                    id: int = int(entry["prnd_id"])
                    pub_name: str = entry['publ_name']
                    if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                        deltas_file = os.path.join(os.path.join(
                            PATH_TO_PUB, str(id)), 'deltas.csv')
                        deltas: np.ndarray = np.loadtxt(
                            deltas_file, dtype=np.int32)
                        if np.any(deltas != 0):
                            pubs_list.append(id)
                            pubs_names_dict[id] = pub_name
            except json.JSONDecodeError:
                print("Error decoding JSON response PUBS")
            labs_pubs_dict[str(lab)] = pubs_list
        else:
            print(
                f"Request failed with status code: {pub_request.status_code}")
    return labs_pubs_dict, pubs_names_dict


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


@timeit
def get_authors_pubs_dict(authors: Iterable[str], selected_pubs, years=None) -> Iterable:
    if not years:
        years = [1900, 2100]
    authors_pubs_dict: Mapping[str, int] = {}
    pubs_names_dict: Mapping[int, str] = {}
    # with open("/home/isand_user/isand/web_application/back/cached_files/prndid_to_id.json") as json_file:
    #     prnd_to_id: dict = json.load(json_file)
    sess = requests.Session()
    for author in authors:
        if selected_pubs.count('Все работы'):
            # pub_request = requests.get(
            #     f'http://193.232.208.28/api/v2.0/authors/get_period?id={str(prnd_to_id[str(author)])}&begin_year={years[0]}&end_year={years[1]}')

            # Получаем isand_id (id в графовой бд) из базы данных по author_id
            isand_id = get_author_isand_id(author)

            pub_request = requests.get(
                f'http://193.232.208.28/api/v2.5/authors/card/get_publications?id={isand_id}')
            pubs_list = []
            if pub_request.status_code == 200:
                try:
                    response_data = pub_request.json()
                    for entry in response_data:
                        #print(entry, file=sys.stderr)
                        publ_year: int = int(entry['year'])
                        if publ_year < years[0] or publ_year > years[1]:
                            continue

                        request_id = entry['publ_isand_id']
                        prnd_id = get_prnd_id(request_id)
                        pub_name: str = entry['publ_name']

                        if prnd_id and os.path.exists(os.path.join(PATH_TO_PUB, str(prnd_id))):
                            id: int = int(prnd_id)
                            deltas_file = os.path.join(os.path.join(
                                PATH_TO_PUB, str(id)), 'deltas.csv')
                            deltas: np.ndarray = np.loadtxt(
                                deltas_file, dtype=np.int32)
                            if np.any(deltas != 0):
                                pubs_list.append(id)
                                pubs_names_dict[id] = pub_name
                except json.JSONDecodeError:
                    print("Error decoding JSON response PUBS")
                authors_pubs_dict[author] = pubs_list
            else:
                print(
                    f"Request failed with status code: {pub_request.status_code}")
        else:
            authors_pubs_dict[author] = list(map(int, selected_pubs))
    return authors_pubs_dict, pubs_names_dict

@timeit
def get_conferences_pubs_dict(conferences: Iterable[str], selected_pubs, years=None) -> Iterable:
    if not years:
        years = [1900, 2100]
    conferences_pubs_dict: Mapping[str, int] = {}
    pubs_names_dict: Mapping[int, str] = {}
    # with open("/home/isand_user/isand/web_application/back/cached_files/prndid_to_id.json") as json_file:
    #     prnd_to_id: dict = json.load(json_file)
    sess = requests.Session()
    for conference in conferences:
        if selected_pubs.count('Все работы'):
            # Получаем isand_id (id в графовой бд) из базы данных по conference_id
            isand_id = get_conference_isand_id(conference)

            pub_request = requests.get(
                f'http://193.232.208.28/api/v2.5/conferences/card/get_publications?id={isand_id}')
            pubs_list = []
            if pub_request.status_code == 200:
                try:
                    response_data = pub_request.json()
                    for entry in response_data:
                        #print(entry, file=sys.stderr)
                        publ_year: int = int(entry['year'])
                        if publ_year < years[0] or publ_year > years[1]:
                            continue

                        request_id = entry['publ_isand_id']
                        prnd_id = get_prnd_id(request_id)
                        pub_name: str = entry['publ_name']

                        if prnd_id and os.path.exists(os.path.join(PATH_TO_PUB, str(prnd_id))):
                            id: int = int(prnd_id)
                            deltas_file = os.path.join(os.path.join(
                                PATH_TO_PUB, str(id)), 'deltas.csv')
                            deltas: np.ndarray = np.loadtxt(
                                deltas_file, dtype=np.int32)
                            if np.any(deltas != 0):
                                pubs_list.append(id)
                                pubs_names_dict[id] = pub_name
                except json.JSONDecodeError:
                    print("Error decoding JSON response PUBS")
                conferences_pubs_dict[conference] = pubs_list
            else:
                print(
                    f"Request failed with status code: {pub_request.status_code}")
        else:
            conferences_pubs_dict[conference] = list(map(int, selected_pubs))
    return conferences_pubs_dict, pubs_names_dict

@timeit
def get_journals_pubs_dict(journals: Iterable[str], selected_pubs, years=None) -> Iterable:
    if not years:
        years = [1900, 2100]
    journals_pubs_dict: Mapping[str, int] = {}
    pubs_names_dict: Mapping[int, str] = {}
    # with open("/home/isand_user/isand/web_application/back/cached_files/prndid_to_id.json") as json_file:
    #     prnd_to_id: dict = json.load(json_file)
    sess = requests.Session()
    for journal in journals:
        if selected_pubs.count('Все работы'):
            # Получаем isand_id (id в графовой бд) из базы данных по journal_id
            isand_id = get_journal_isand_id(journal)

            pub_request = requests.get(
                f'http://193.232.208.28/api/v2.5/journals/card/get_publications?id={isand_id}')
            pubs_list = []
            if pub_request.status_code == 200:
                try:
                    response_data = pub_request.json()
                    for entry in response_data:
                        #print(entry, file=sys.stderr)
                        publ_year: int = int(entry['year'])
                        if publ_year < years[0] or publ_year > years[1]:
                            continue

                        request_id = entry['publ_isand_id']
                        prnd_id = get_prnd_id(request_id)
                        pub_name: str = entry['publ_name']

                        if prnd_id and os.path.exists(os.path.join(PATH_TO_PUB, str(prnd_id))):
                            id: int = int(prnd_id)
                            deltas_file = os.path.join(os.path.join(
                                PATH_TO_PUB, str(id)), 'deltas.csv')
                            deltas: np.ndarray = np.loadtxt(
                                deltas_file, dtype=np.int32)
                            if np.any(deltas != 0):
                                pubs_list.append(id)
                                pubs_names_dict[id] = pub_name
                except json.JSONDecodeError:
                    print("Error decoding JSON response PUBS")
                journals_pubs_dict[journal] = pubs_list
            else:
                print(
                    f"Request failed with status code: {pub_request.status_code}")
        else:
            journals_pubs_dict[journal] = list(map(int, selected_pubs))
    return journals_pubs_dict, pubs_names_dict

@timeit
def get_organizations_pubs_dict(organizations: Iterable[str], selected_pubs, years=None) -> Iterable:
    if not years:
        years = [1900, 2100]
    organizations_pubs_dict: Mapping[str, int] = {}
    pubs_names_dict: Mapping[int, str] = {}
    # with open("/home/isand_user/isand/web_application/back/cached_files/prndid_to_id.json") as json_file:
    #     prnd_to_id: dict = json.load(json_file)
    sess = requests.Session()
    for organization in organizations:
        if selected_pubs.count('Все работы'):
            # Получаем isand_id (id в графовой бд) из базы данных по organization_id
            isand_id = get_organization_isand_id(organization)

            pub_request = requests.get(
                f'http://193.232.208.28/api/v2.5/organizations/card/get_publications?id={isand_id}')
            pubs_list = []
            if pub_request.status_code == 200:
                try:
                    response_data = pub_request.json()
                    for entry in response_data:
                        #print(entry, file=sys.stderr)
                        publ_year: int = int(entry['year'])
                        if publ_year < years[0] or publ_year > years[1]:
                            continue

                        request_id = entry['publ_isand_id']
                        prnd_id = get_prnd_id(request_id)
                        pub_name: str = entry['publ_name']

                        if prnd_id and os.path.exists(os.path.join(PATH_TO_PUB, str(prnd_id))):
                            id: int = int(prnd_id)
                            deltas_file = os.path.join(os.path.join(
                                PATH_TO_PUB, str(id)), 'deltas.csv')
                            deltas: np.ndarray = np.loadtxt(
                                deltas_file, dtype=np.int32)
                            if np.any(deltas != 0):
                                pubs_list.append(id)
                                pubs_names_dict[id] = pub_name
                except json.JSONDecodeError:
                    print("Error decoding JSON response PUBS")
                organizations_pubs_dict[organization] = pubs_list
            else:
                print(
                    f"Request failed with status code: {pub_request.status_code}")
        else:
            organizations_pubs_dict[organization] = list(map(int, selected_pubs))
    return organizations_pubs_dict, pubs_names_dict

@timeit
def get_cities_pubs_dict(cities: Iterable[str], selected_pubs, years=None) -> Iterable:
    if not years:
        years = [1900, 2100]
    cities_pubs_dict: Mapping[str, int] = {}
    pubs_names_dict: Mapping[int, str] = {}
    # with open("/home/isand_user/isand/web_application/back/cached_files/prndid_to_id.json") as json_file:
    #     prnd_to_id: dict = json.load(json_file)
    sess = requests.Session()
    for city in cities:
        if selected_pubs.count('Все работы'):
            # Получаем isand_id (id в графовой бд) из базы данных по city_id
            isand_id = get_city_isand_id(city)

            pub_request = requests.get(
                f'http://193.232.208.28/api/v2.5/geo/card/get_publications?id={isand_id}')
            pubs_list = []
            if pub_request.status_code == 200:
                try:
                    response_data = pub_request.json()
                    for entry in response_data:
                        #print(entry, file=sys.stderr)
                        publ_year: int = int(entry['year'])
                        if publ_year < years[0] or publ_year > years[1]:
                            continue

                        request_id = entry['publ_isand_id']
                        prnd_id = get_prnd_id(request_id)
                        pub_name: str = entry['publ_name']

                        if prnd_id and os.path.exists(os.path.join(PATH_TO_PUB, str(prnd_id))):
                            id: int = int(prnd_id)
                            deltas_file = os.path.join(os.path.join(
                                PATH_TO_PUB, str(id)), 'deltas.csv')
                            deltas: np.ndarray = np.loadtxt(
                                deltas_file, dtype=np.int32)
                            if np.any(deltas != 0):
                                pubs_list.append(id)
                                pubs_names_dict[id] = pub_name
                except json.JSONDecodeError:
                    print("Error decoding JSON response PUBS")
                cities_pubs_dict[city] = pubs_list
            else:
                print(
                    f"Request failed with status code: {pub_request.status_code}")
        else:
            cities_pubs_dict[city] = list(map(int, selected_pubs))
    return cities_pubs_dict, pubs_names_dict
