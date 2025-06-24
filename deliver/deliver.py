import time
from flask import Flask, request, jsonify, Response, send_file
from flask_cors import CORS
import numpy as np
import os
from typing import *
import json
from deltas import *
import requests
import pandas as pd
import networkx as nx
import pickle
import sys
from pubs_getter import *
from copy import deepcopy as cp
from scipy.stats import norm, ttest_ind, pearsonr, spearmanr

from datetime import datetime
from threading import Thread
from tqdm import tqdm


from sklearn.decomposition import PCA



app = Flask(__name__)
CORS(app)

PATH_TO_PUB = '/home/isand_user/isand/web_application/back/struct_pub/'
PATH_TO_TERMS = '/home/isand_user/isand/web_application/back/terms/terms.json'

with open("../cached_files/available_authors.json", 'r') as file:
    authors_dict = json.load(file)

labs_request = requests.get('http://193.232.208.28/api/v2.0/organization?id=1')
labs_dict = {}
if labs_request.status_code == 200:
    try:
        labs_data = labs_request.json()
        arr = []
        for i in labs_data:
            if i['div_id'] in ['16', '61', '84', '108', '85', '109', '87', '96', '67', '32', '83', '18', '58', '79', '127', '166', '95']:
                continue
            labs_dict[(i['div_id'])] = i['div_name']
    except json.JSONDecodeError:
        print("Error decoding JSON response PUBS")
else:
    print(f"Request failed with status code: {labs_request.status_code}")


def save_authors_info():
    print("started")
    authors = requests.get('http://193.232.208.28/api/v2.0/authors')
    authors_dict = {}

    if authors.status_code == 200:
        try:
            # Десериализуем JSON данные
            response_data = authors.json()
            # Создаем список словарей
            for entry in response_data:
                fio = entry["fio"]
                author_id = entry["prnd_id"]
                # authors_dict[author_id] = fio
                if author_id not in authors_dict:
                    pub_request = requests.get(
                        'http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(author_id))
                    if pub_request.status_code == 200:
                        try:
                            # Десериализуем JSON данные
                            response_data = pub_request.json()
                            for entry in response_data:
                                id = entry["prnd_id"]
                                if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                                    deltas_file = os.path.join(os.path.join(
                                        PATH_TO_PUB, str(id)), 'deltas.csv')
                                    deltas: np.ndarray = np.loadtxt(
                                        deltas_file, dtype=np.int32)
                                    has_non_zero = np.any(deltas != 0)
                                    if has_non_zero:
                                        authors_dict[author_id] = fio
                                        # print(fio)
                        except json.JSONDecodeError:
                            print("Error decoding JSON response PUBS")
                    else:
                        print(
                            f"Request failed with status code: {pub_request.status_code}")
        except json.JSONDecodeError:
            print("Error decoding JSON response AUTHOR")
    else:
        print(f"Request failed with status code: {authors.status_code}")
    with open("../cached_files/available_authors.json", 'w') as file:
        json.dump(authors_dict, file)
    print("saved")


@app.route('/deliver/')
def hello():
    return f'''
    <pre>
    /get_pub_deltas(?id&f) - путь для получения json с дельтами.\n
    \tЕсли указать id, то придёт ответ json с конкретным id. Если не передавать id парметром, то вернётся json с всеми дельтами.
    \tВ параметре f можно передать любое значение, он сигнализирует о том, что необходимо скачать файл из браузера.\n
    /get_pub_txt?id(&f) - путь для получения json с текстовым слоём.\n
    \tНаличие параметра id обязательно.\n
    \tВ параметре f можно передать любое значение, он сигнализирует о том, что необходимо скачать файл из браузера.\n
    /get_pub_meta_data?id(&f) - путь для получения json с мета информацией по статье.\n
    \tНаличие параметра id обязательно.\n
    \tВ параметре f можно передать любое значение, он сигнализирует о том, что необходимо скачать файл из браузера.\n
    /get_pub_pdf?id(&f) - путь для получения json с публикацией pdf.\n
    \tНаличие параметра id обязательно.\n
    \tВ параметре f можно передать любое значение, он сигнализирует о том, что необходимо скачать файл из браузера.\n
    </pre>
    '''


@app.route('/deliver/get_pub_deltas', methods=['GET'])
def get_pub_deltas():
    id = request.args.get('id')
    f = request.args.get('f')
    if f and id:
        return send_file(PATH_TO_PUB + id + '/deltas.csv', as_attachment=True)
    elif id:
        return send_file(PATH_TO_PUB + id + '/deltas.csv')
    elif f:
        return send_file(PATH_TO_TERMS, as_attachment=True)
    else:
        return send_file(PATH_TO_TERMS)


@app.route('/deliver/get_pub', methods=['GET'])
def get_pub():
    id = request.args.get('id')
    f = request.args.get('f')
    if f and id:
        return send_file(PATH_TO_PUB + id + '/main.pdf', as_attachment=True)
    elif id:
        return send_file(PATH_TO_PUB + id + '/main.pdf')
    else:
        return 'Некорретный запрос! Добавте параметр запрашиваемой статьи!'


@app.route('/deliver/get_pub_meta_data', methods=['GET'])
def get_pub_meta_data():
    id = request.args.get('id')
    f = request.args.get('f')
    if f and id:
        return send_file(PATH_TO_PUB + id + '/main.json', as_attachment=True)
    elif id:
        return send_file(PATH_TO_PUB + id + '/main.json')
    else:
        return 'Некорретный запрос! Добавте параметр запрашиваемой статьи!'


@app.route('/deliver/get_pub_txt', methods=['GET'])
def get_pub_txt():
    id = request.args.get('id')
    f = request.args.get('f')
    if f and id:
        return send_file(PATH_TO_PUB + id + '/main.txt', as_attachment=True)
    elif id:
        return send_file(PATH_TO_PUB + id + '/main.txt')
    else:
        return 'Некорретный запрос! Добавте параметр запрашиваемой статьи!'


@app.route('/deliver/authors', methods=['GET'])
def get_authors():
    # save_authors_info()
    # print(loaded_folder_info)
    with open("../cached_files/available_authors.json", 'r') as file:
        loaded_folder_info = json.load(file)
    return (jsonify(loaded_folder_info))
    #     flipped_dict = {value: key for key, value in loaded_folder_info.items()}
    #     sorted_json = dict(sorted(flipped_dict.items(), key=lambda item: item[1]))
    #     result_json = {value: key for key, value in sorted_json.items()}
    # with open("available_authors.json", 'w') as file:
    #     json.dump(result_json, file)
    # authors = requests.get('http://193.232.208.28/api/v2.0/authors')
    # authors_dict = {}
    # if authors.status_code == 200:
    #     try:
    # Десериализуем JSON данные
    # response_data = authors.json()
    # Создаем список словарей
    # for entry in response_data:
    # fio = entry["fio"]
    # author_id = entry["prnd_id"]
    # authors_dict[author_id] = fio
    # if author_id not in authors_dict:
    #     pub_request = requests.get('http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(author_id))
    #     if pub_request.status_code == 200:
    #         try:
    #             # Десериализуем JSON данные
    #             response_data = pub_request.json()
    #             for entry in response_data:
    #                 id = entry["prnd_id"]
    #                 if id in b:
    #                     authors_dict[author_id] = fio
    #                 # if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
    #                 #     if author_id not in authors_dict:
    #                 #         authors_dict[author_id] = fio
    #                         # print(fio)
    #         except json.JSONDecodeError:
    #             print("Error decoding JSON response PUBS")
    #     else:
    #         print(f"Request failed with status code: {pub_request.status_code}")
    #     except json.JSONDecodeError:
    #         print("Error decoding JSON response AUTHOR")
    # else:
    #     print(f"Request failed with status code: {authors.status_code}")
    # return(jsonify(authors_dict))

def get_journals_from_db():
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id, full_name FROM journals as j WHERE j.isand_id IS NOT NULL")
        journals = cursor.fetchall()

        # Формируем JSON в виде {id: full_name}
        return {str(journal_id): full_name for journal_id, full_name in journals}

    except psycopg2.Error as e:
        print(f"Ошибка при запросе к БД: {e}")
        return {}  # Возвращаем пустой словарь в случае ошибки

    finally:
        connection_pool.putconn(connection)

@app.route('/deliver/journals', methods=['GET'])
def get_journals():
    journals_data = get_journals_from_db()
    return jsonify(journals_data)

def get_conferences_from_db():
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id, full_name FROM conferences as c WHERE c.isand_id IS NOT NULL")
        conferences = cursor.fetchall()

        # Формируем JSON в виде {id: full_name}
        return {str(conference_id): full_name for conference_id, full_name in conferences}

    except psycopg2.Error as e:
        print(f"Ошибка при запросе к БД: {e}")
        return {}  # Возвращаем пустой словарь в случае ошибки

    finally:
        connection_pool.putconn(connection)

@app.route('/deliver/conferences', methods=['GET'])
def get_conferences():
    conferences_data = get_conferences_from_db()
    return jsonify(conferences_data)

def get_organizations_from_db():
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id, base_name FROM organizations as o WHERE o.isand_id IS NOT NULL")
        organizations = cursor.fetchall()

        # Формируем JSON в виде {id: full_name}
        return {str(organization_id): base_name for organization_id, base_name in organizations}

    except psycopg2.Error as e:
        print(f"Ошибка при запросе к БД: {e}")
        return {}  # Возвращаем пустой словарь в случае ошибки

    finally:
        connection_pool.putconn(connection)

@app.route('/deliver/organizations', methods=['GET'])
def get_organizations():
    organizations_data = get_organizations_from_db()
    return jsonify(organizations_data)

def get_cities_from_db():
    connection = connection_pool.getconn()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id, name FROM cities as o WHERE o.isand_id IS NOT NULL")
        cities = cursor.fetchall()

        # Формируем JSON в виде {id: full_name}
        return {str(city_id): name for city_id, name in cities}

    except psycopg2.Error as e:
        print(f"Ошибка при запросе к БД: {e}")
        return {}  # Возвращаем пустой словарь в случае ошибки

    finally:
        connection_pool.putconn(connection)

@app.route('/deliver/cities', methods=['GET'])
def get_cities():
    cities_data = get_cities_from_db()
    return jsonify(cities_data)

@app.route('/deliver/authors_posts', methods=['GET'])
def get_author_posts():
    selected_author = request.args.get('author_id')

    # Получаем isand_id (id в графовой бд) из базы данных по author_id
    isand_id = get_author_isand_id(selected_author)

    pub_request = requests.get(
        f'http://193.232.208.28/api/v2.5/authors/card/get_publications?id={isand_id}')
    result_dict = {}
    if pub_request.status_code == 200:
        try:
            # Десериализуем JSON данные
            response_data = pub_request.json()
            for entry in response_data:
                label = entry["publ_name"]
                request_id = entry["publ_isand_id"]
                id = get_prnd_id(request_id)

                if id and os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                    deltas_file = os.path.join(os.path.join(
                        PATH_TO_PUB, str(id)), 'deltas.csv')
                    deltas: np.ndarray = np.loadtxt(
                        deltas_file, dtype=np.int32)
                    has_non_zero = np.any(deltas != 0)
                    if has_non_zero:
                        result_dict[id] = label
        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    else:
        print(f"Request failed with status code: {pub_request.status_code}")
    return result_dict

@app.route('/deliver/conferences_posts', methods=['GET'])
def get_conference_posts():
    selected_conference = request.args.get('conference_id')

    # Получаем isand_id (id в графовой бд) из базы данных по author_id
    isand_id = get_conference_isand_id(selected_conference)

    pub_request = requests.get(
        f'http://193.232.208.28/api/v2.5/conferences/card/get_publications?id={isand_id}')
    result_dict = {}
    if pub_request.status_code == 200:
        try:
            # Десериализуем JSON данные
            response_data = pub_request.json()
            for entry in response_data:
                label = entry["publ_name"]
                request_id = entry["publ_isand_id"]
                id = get_prnd_id(request_id)

                if id and os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                    deltas_file = os.path.join(os.path.join(
                        PATH_TO_PUB, str(id)), 'deltas.csv')
                    deltas: np.ndarray = np.loadtxt(
                        deltas_file, dtype=np.int32)
                    has_non_zero = np.any(deltas != 0)
                    if has_non_zero:
                        result_dict[id] = label
        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    else:
        print(f"Request failed with status code: {pub_request.status_code}")
    return result_dict

@app.route('/deliver/journals_posts', methods=['GET'])
def get_journal_posts():
    selected_journal = request.args.get('journals_id')

    # Получаем isand_id (id в графовой бд) из базы данных по author_id
    isand_id = get_journal_isand_id(selected_journal)

    pub_request = requests.get(
        f'http://193.232.208.28/api/v2.5/journals/card/get_publications?id={isand_id}')
    result_dict = {}
    if pub_request.status_code == 200:
        try:
            # Десериализуем JSON данные
            response_data = pub_request.json()
            for entry in response_data:
                label = entry["publ_name"]
                request_id = entry["publ_isand_id"]
                id = get_prnd_id(request_id)

                if id and os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                    deltas_file = os.path.join(os.path.join(
                        PATH_TO_PUB, str(id)), 'deltas.csv')
                    deltas: np.ndarray = np.loadtxt(
                        deltas_file, dtype=np.int32)
                    has_non_zero = np.any(deltas != 0)
                    if has_non_zero:
                        result_dict[id] = label
        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    else:
        print(f"Request failed with status code: {pub_request.status_code}")
    return result_dict

@app.route('/deliver/organization_posts', methods=['GET'])
def get_organization_posts():
    selected_organization = request.args.get('organizations_id')

    # Получаем isand_id (id в графовой бд) из базы данных по author_id
    isand_id = get_organization_isand_id(selected_organization)

    pub_request = requests.get(
        f'http://193.232.208.28/api/v2.5/organizations/card/get_publications?id={isand_id}')
    result_dict = {}
    if pub_request.status_code == 200:
        try:
            # Десериализуем JSON данные
            response_data = pub_request.json()
            for entry in response_data:
                label = entry["publ_name"]
                request_id = entry["publ_isand_id"]
                id = get_prnd_id(request_id)

                if id and os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                    deltas_file = os.path.join(os.path.join(
                        PATH_TO_PUB, str(id)), 'deltas.csv')
                    deltas: np.ndarray = np.loadtxt(
                        deltas_file, dtype=np.int32)
                    has_non_zero = np.any(deltas != 0)
                    if has_non_zero:
                        result_dict[id] = label
        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    else:
        print(f"Request failed with status code: {pub_request.status_code}")
    return result_dict

@app.route('/deliver/city_posts', methods=['GET'])
def get_city_posts():
    selected_city = request.args.get('cities_id')

    # Получаем isand_id (id в графовой бд) из базы данных по author_id
    isand_id = get_city_isand_id(selected_city)

    pub_request = requests.get(
        f'http://193.232.208.28/api/v2.5/geo/card/get_publications?id={isand_id}')
    result_dict = {}
    if pub_request.status_code == 200:
        try:
            # Десериализуем JSON данные
            response_data = pub_request.json()
            for entry in response_data:
                label = entry["publ_name"]
                request_id = entry["publ_isand_id"]
                id = get_prnd_id(request_id)

                if id and os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                    deltas_file = os.path.join(os.path.join(
                        PATH_TO_PUB, str(id)), 'deltas.csv')
                    deltas: np.ndarray = np.loadtxt(
                        deltas_file, dtype=np.int32)
                    has_non_zero = np.any(deltas != 0)
                    if has_non_zero:
                        result_dict[id] = label
        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    else:
        print(f"Request failed with status code: {pub_request.status_code}")
    return result_dict

@app.route('/deliver/posts_for_graph', methods=['POST'])
def post_labs():
    json_data: dict = request.get_json()
    selected_type: str = json_data.get('selected_type')

    if selected_type in ['authors', 'labs']:
        essences = json_data.get('selected_authors')
    elif selected_type == 'conferences':
        essences = json_data.get('selected_conferences')
    elif selected_type == 'journals':
        essences = json_data.get('selected_journals')
    elif selected_type == 'organizations':
        essences = json_data.get('selected_organizations')
    elif selected_type == 'cities':
        essences = json_data.get('selected_cities')
    else:
        return jsonify({"error": "Invalid selected_type"}), 400

    if not essences:
        return jsonify({})

    selected_pubs = json_data.get('selected_works_id')
    selected_level = json_data.get('level')
    selected_scheme = json_data.get('selected_scheme_id')
    selected_cutoff = json_data.get('cutoff_value')
    selected_cutoff_terms = json_data.get('cutoff_terms_value')
    selected_include_common_terms = json_data.get('include_common_terms')
    selected_path = json_data.get('path')
    selected_years = json_data.get('years')

    if selected_type in ['authors', 'labs']:
        pubs_dict: Mapping[str, list[int]] = get_authors_pubs_dict(
            authors=essences, selected_pubs=selected_pubs, years=selected_years
        )[0] if selected_type == "authors" else get_labs_pubs_dict(
            labs=essences, years=selected_years
        )[0]
    elif selected_type == 'conferences':
        pubs_dict: Mapping[str, list[int]] = get_conferences_pubs_dict(
            conferences=essences, selected_pubs=selected_pubs, years=selected_years
        )[0]
    elif selected_type == 'journals':
        pubs_dict: Mapping[str, list[int]] = get_journals_pubs_dict(
            journals=essences, selected_pubs=selected_pubs, years=selected_years
        )[0]
    elif selected_type == 'organizations':
        pubs_dict: Mapping[str, list[int]] = get_organizations_pubs_dict(
            organizations=essences, selected_pubs=selected_pubs, years=selected_years
        )[0]
    elif selected_type == 'cities':
        pubs_dict: Mapping[str, list[int]] = get_cities_pubs_dict(
            cities=essences, selected_pubs=selected_pubs, years=selected_years
        )[0]

    result: Mapping[str, Mapping[str, list[float]]] = {}
    summ = 0

    for essence_id in pubs_dict:
        if pubs_dict[essence_id]:
            author_info = build_chart(
                None,
                pubs_dict[essence_id],
                selected_level,
                selected_scheme,
                selected_cutoff,
                selected_cutoff_terms,
                selected_include_common_terms,
                False,
                selected_path,
                []
            )
            labels = {}
            summ = 0
            for key in author_info.keys():
                label = str(key)
                labels[label] = float(author_info[key])
                summ += labels[label]
            print(f"summ of freqs {summ}", file=sys.stderr)
            result[essence_id] = labels

    print(result, file=sys.stderr)
    return jsonify(result)


@app.route('/deliver/articleRaiting', methods=['POST'])
def get_article_raiting():
    json_data = request.get_json()
    selected_type: str = json_data.get('selected_type')

    # Определяем, какие данные использовать в зависимости от agent_type
    if selected_type in ['authors', 'labs'] :
        essences = list(map(str, json_data.get('selected_authors')))
    elif selected_type == 'conferences':
        essences = list(map(str, json_data.get('selected_conferences')))
    elif selected_type == 'journals':
        essences = list(map(str, json_data.get('selected_journals')))
    elif selected_type == 'organizations':
        essences = list(map(str, json_data.get('selected_organizations')))
    elif selected_type == 'cities':
        essences = list(map(str, json_data.get('selected_cities')))
    else:
        return jsonify({"error": "Invalid selected_type"}), 400

    if not essences:
        return jsonify({})

    selected_pubs = json_data.get('selected_works_id')
    selected_level = json_data.get('level')
    selected_scheme = json_data.get('selected_scheme_id')
    selected_cutoff = json_data.get('cutoff_value')
    selected_cutoff_terms = json_data.get('cutoff_terms_value')
    selected_include_common_terms = json_data.get('include_common_terms')
    # include_management_theory = json_data.file['include_management_theory']
    selected_path = json_data.get('path')
    pubs_names_dict: Mapping[int, str] = {}
    result = {}

    # Выбираем нужный метод в зависимости от agent_type
    if selected_type in ['authors', 'labs']:
        dicts = get_authors_pubs_dict(
            authors=essences, selected_pubs=selected_pubs
        ) if selected_type == "authors" else get_labs_pubs_dict(
            labs=essences
        )
    elif selected_type == 'conferences':
        dicts = get_conferences_pubs_dict(
            conferences=essences, selected_pubs=selected_pubs
        )
    elif selected_type == 'journals':
        dicts = get_journals_pubs_dict(
            journals=essences, selected_pubs=selected_pubs
        )
    elif selected_type == 'organizations':
        dicts = get_organizations_pubs_dict(
            organizations=essences, selected_pubs=selected_pubs
        )
    elif selected_type == 'cities':
        dicts = get_cities_pubs_dict(
            cities=essences, selected_pubs=selected_pubs
        )

    pubs_dict = dicts[0]
    pubs_names_dict: Mapping[int, str] = dicts[1]
    for essence_id in pubs_dict:
        labels = []
        for pub_id in pubs_dict[essence_id]:
            pub_info: Mapping[str, float] = build_chart(
                None,
                [pub_id],
                selected_level,
                selected_scheme,
                selected_cutoff,
                selected_cutoff_terms,
                selected_include_common_terms,
                False,
                selected_path,
                []
            )
            count_of_occurrences = int(sum(pub_info.values()))
            if count_of_occurrences > 0:
                labels.append(
                    {pubs_names_dict[pub_id]: count_of_occurrences})
        result[essence_id] = sorted(
            labels, key=lambda x: list(x.values())[0], reverse=True)
    return jsonify(result)


def construct_id_translation_table():
    print("building translation table")
    authors = requests.get('http://193.232.208.28/api/v2.0/authors')
    translation_dict = {}

    for elm in authors.json():
        translation_dict.setdefault(int(elm['prnd_id']), int(elm['pers_id']))
    print(list(translation_dict.keys())[0:3])
    print(list(translation_dict.values())[0:3])
    with open("/home/isand_user/isand/web_application/back/cached_files/translation_table", 'w') as f:
        json.dump(translation_dict, f)
    print("done translation table")
    return translation_dict


@app.route('/deliver/translate_id', methods=['POST'])
def translate_id():
    json_data = request.get_json()
    auth_id = json_data['author_prnd_id']
    auth_id = str(auth_id)
    print(auth_id)

    with open("/home/isand_user/isand/web_application/back/cached_files/translation_table", 'r') as f:
        translation_table = json.load(f)

    return jsonify({'pers_id': translation_table[auth_id]})


def translate_back(auth_pers_id):

    with open("/home/isand_user/isand/web_application/back/cached_files/translation_table", 'r') as f:
        translation_table = json.load(f)

    for a in translation_table:
        if translation_table[a] == auth_pers_id:
            return a

    return None


@app.route('/deliver/produce_conference_authors_list', methods=['POST'])
def produce_conference_authors_list():
    json_data = request.get_json()
    conf_name = json_data['conf']
    try:
        years = json_data['years']
    except (KeyError):
        years = [2019, 2020, 2021, 2022, 2023]
    print(conf_name)

    authors_list = {}
    for year in years:
        pub_request = requests.get(
            f'http://193.232.208.28/api/v2.0/conference_org?year={year}&org_id=1&conf_name={conf_name}')

        if pub_request.status_code != 200:
            print(pub_request)
            continue
        for elm in pub_request.json():
            authors_list.setdefault(elm['a_id'], 0)

    return jsonify({'result': list(authors_list.keys())})


def produce_conference_delta(conf_name, years=[2019, 2020, 2021, 2022, 2023]):
    pubs_list = []
    pubs_deltas = []
    for year in years:
        pub_request = requests.get(
            f'http://193.232.208.28/api/v2.0/conference_org?year={year}&org_id=1&conf_name={conf_name}')

        if pub_request.status_code != 200:
            print(pub_request)
            continue
        for elm in pub_request.json():
            id = elm['p_id']
            if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):

                deltas_file = os.path.join(os.path.join(
                    PATH_TO_PUB, str(id)), 'deltas.csv')
                deltas: np.ndarray = np.loadtxt(
                    deltas_file, dtype=np.int32)
                has_non_zero = np.any(deltas != 0)
                if has_non_zero:
                    pubs_list.append(int(id))

    print(pubs_list)
    if pubs_list != []:
        # запрос дельт для конкретной публикации
        pub_deltas = build_chart(
            None,  # вроде не используется
            pubs_list,
            3,  # уровень
            1,  # тип вектора
            0,  # отсечение по частоте
            0,  # тоже отсечение по частоте
            True,  # включать общенаучные термины
            False,  # вроде не используется
            []
        )
        vector = pub_deltas
    else:
        vector = {}
    pubs_deltas = vector

    return pubs_deltas  # возвращаем дельты всех найденных пуликаций


@app.route('/deliver/construct_map', methods=['POST'])
def construct_map():
    print("deltas requested")
    # print(produce_conference_delta('MLSD'))
    # return "ok"
    json_data = request.get_json()
    authors_list = json_data.get('authors')
    confs_list = json_data.get('confs')
    key_confs_list = [k[0] for k in confs_list]
    confs_list = [k[1] for k in confs_list]
    print(key_confs_list)
    print(confs_list)

    if authors_list is None:
        return "not ok"

    print("---------")
    print(authors_list)
    print("---------")

    t_st = time.time()
    # пошучение данных для каждого автора из списка
    authors_delta = []
    for author in authors_list:
        pub_request = requests.get(
            'http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(author))
        pubs_list = []
        # запрос всех публикаций автора
        if pub_request.status_code == 200:
            try:
                response_data = pub_request.json()
                for entry in response_data:

                    id = entry["prnd_id"]
                    if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):

                        deltas_file = os.path.join(os.path.join(
                            PATH_TO_PUB, str(id)), 'deltas.csv')
                        deltas: np.ndarray = np.loadtxt(
                            deltas_file, dtype=np.int32)
                        has_non_zero = np.any(deltas != 0)
                        if has_non_zero:
                            pubs_list.append(int(id))

            except json.JSONDecodeError:
                print("Error decoding JSON response PUBS")

        print(pubs_list)
        if pubs_list != []:
            # запрос дельт для конкретной публикации
            pub_deltas = build_chart(
                None,  # вроде не используется
                pubs_list,
                3,  # уровень
                1,  # тип вектора
                0,  # отсечение по частоте
                0,  # тоже отсечение по частоте
                True,  # включать общенаучные термины
                False,  # вроде не используется
                []
            )
            vector = pub_deltas
        else:
            vector = {}
        authors_delta.append(vector)

    print(f"got vectors\t\t {time.time() - t_st}")
    # добавляем дельты конференций
    for conf in confs_list:
        delta = produce_conference_delta(conf)
        print(delta)
        authors_delta.append(delta)

    all_terms = {}
    for vec in authors_delta:
        # print(vec)
        for t in vec.keys():
            all_terms.setdefault(t[-1], 0)

    normalized_vector_representations = []
    for delta in authors_delta:
        representation = cp(all_terms)
        for t in delta.keys():
            representation[t[-1]] = delta[t]
        normalized_vector_representations.append(representation)

    pca = PCA(n_components=2)
    pca.fit([list(repr.values())
            for repr in normalized_vector_representations])
    dimentionless_repr = pca.transform(
        [list(repr.values()) for repr in normalized_vector_representations]).tolist()
    # print(dimentionless_repr)
    print((authors_list + confs_list))
    print(len((authors_list + confs_list)))
    print(len(dimentionless_repr))
    result = {'result': {str((authors_list + key_confs_list)
                             [i]): dimentionless_repr[i] for i in range(len(dimentionless_repr))}}
    print(result)
    with open("/home/isand_user/isand/web_application/back/deliver/2d_authors_deltas/cached_deltas", 'w') as f:
        json.dump(result, f)

    return jsonify(result)


@app.route('/deliver/send_map', methods=['POST'])
def send_map():
    # produce_conference_delta(conf_name = 'MLSD')
    with open("/home/isand_user/isand/web_application/back/deliver/2d_authors_deltas/cached_deltas", 'r') as f:
        result = json.load(f)
    return jsonify(result)


@app.route('/deliver/produce_connectivity_graph', methods=['POST'])
def produce_connectivity_graph():
    t_0 = time.time()

    json_data = request.get_json()
    author = json_data.get('author')
    act_range = json_data.get('range')
    common_terms_include = json_data.get('common')
    papers_cnt = json_data.get('papers_to_scan')
    do_edges = json_data.get('build_edges')

    try:
        time_range = json_data.get('time_range')
    except KeyError:
        time_range = None

    try:
        do_layout = json_data.get('layout')
    except KeyError:
        do_layout = False

    try:
        term_level = json_data.get('level')
    except KeyError:
        term_level = 3

    print("---------")
    print(author)
    print(act_range)
    print(common_terms_include)
    print(papers_cnt)
    print(time_range)
    print("---------")

    if (author == None or act_range == None):
        return jsonify({'nodes': [], 'edges': []})

    author_prnd_id = translate_back(author)
    print(author_prnd_id)

    if do_edges:
        pub_request = requests.get(
            'http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(author_prnd_id))

    else:
        pub_request = requests.get(
            'http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(author))

    print(pub_request)
    t_st = time.time()
    pubs_list = []
    no_deltas_pubs_list = []
    # запрос всех публикаций автора
    if pub_request.status_code == 200:
        try:
            response_data = pub_request.json()
            for entry in response_data:

                id = entry["prnd_id"]
                publ_year = int(entry["publ_year"])
                if time_range is None or (int(time_range[0]) <= publ_year and publ_year <= int(time_range[1])):
                    if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):

                        deltas_file = os.path.join(os.path.join(
                            PATH_TO_PUB, str(id)), 'deltas.csv')
                        deltas: np.ndarray = np.loadtxt(
                            deltas_file, dtype=np.int32)
                        has_non_zero = np.any(deltas != 0)
                        if has_non_zero:
                            pubs_list.append(int(id))
                    else:
                        no_deltas_pubs_list.append(int(id))

        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    print(f"got pubs\t\t {time.time() - t_st}")
    print(pubs_list[0:10])
    print(no_deltas_pubs_list[0:10])

    t_st = time.time()

    terms_in_pubs = dict()

    for pub in pubs_list:
        # запрос дельт для конкретной публикации
        pub_deltas = build_chart(
            None,  # вроде не используется
            [pub],
            term_level,  # уровень
            4,  # тип вектора
            act_range[0],  # отсечение по частоте
            act_range[0],  # тоже отсечение по частоте
            common_terms_include,
            False,  # вроде не используется
            []
        )
        terms = pub_deltas.keys()
        tta = {}
        for term in terms:
            tta[term[0]] = int(pub_deltas[term])

        terms_in_pubs[pub] = tta

    print(f"got deltas\t\t {time.time() - t_st}")
    # print(terms_in_pubs)
    # __________________________________________________________________________________________
    # формирование графа

    terms = terms_in_pubs
    pubs = terms.keys()

    papers_lim = papers_cnt
    # active_range = [5, 20]
    # термины, которые встречаются реже, не добавляются
    active_min = act_range[0]
    # термины, которые встречаются чаще, не добавляются
    active_max = act_range[1]

    papers = []
    all_terms = []
    cnt = 0

    t_st = time.time()

    # сбор терминов
    for pub in pubs:
        cnt += 1

        terms_in_paper = list(terms[pub].keys())
        all_terms += terms_in_paper

        if (cnt > papers_lim):
            break

    print(f"{cnt} papers processed")
    cnt = 0

    all_terms = np.unique(all_terms)

    # проход по статьям и сбор терминов из каждой статьи для анализа
    for pub in pubs:
        cnt += 1

        # terms_in_paper = list()
        filtered_paper_delta = []
        for term in terms[pub].keys():
            if (term in all_terms):  # проверка отсечений
                if (terms[pub][term] < active_max and terms[pub][term] > active_min):
                    filtered_paper_delta.append(term)

        papers.append(filtered_paper_delta)

        if (cnt > papers_lim):
            break

    print(f"selected terms\t\t {time.time() - t_st}")
    # если нужны только термины, но не сам граф, возвращаем только их
    if not do_edges:
        print("terms sent, graph not required")
        return jsonify({'nodes': all_terms.tolist(), 'edges': []})

    # построение ребер
    adj = np.zeros([len(all_terms), len(all_terms)])
    terms_to_id = dict((t[1], t[0]) for t in enumerate(all_terms))
    # тут надо оптимизировать (уже)
    t_st = time.time()
    used_terms = []
    edges = []
    for p in papers:
        for term in p:
            for t2 in p:
                if (term != t2):
                    if adj[terms_to_id[term], terms_to_id[t2]] == 0 and adj[terms_to_id[t2], terms_to_id[term]] == 0:
                        adj[terms_to_id[term], terms_to_id[t2]] = 1

                        edges.append((term, t2))

                        if term not in used_terms:
                            used_terms.append(term)
                        if t2 not in used_terms:
                            used_terms.append(t2)

    # print(f"{len(used_terms)} / {len(all_terms)}")
    # print(f"{len(edges)} / {int(len(used_terms) * (len(used_terms)-1) / 2)}")

    # if not include_dead_nodes:
    #     nodes = used_terms
    # else:
        # использовать все термины, или только те, у которых есть связи?
    nodes = all_terms.tolist()

    print(f"built edges\t\t {time.time() - t_st}")

    # теперь строим сам объект графа
    G = nx.Graph()

    # добавляем информацию в объект графа
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)

    # помечаем узлы, у которых нет связей и задаем лейблы
    i = 0
    t_st = time.time()
    for n in G.nodes():
        G.nodes[n]['show'] = True if n in used_terms else False

        G.nodes[n]['label'] = n
        i += 1
    print(f"mark dead\t\t {time.time() - t_st}")

    for adj in G.adjacency():
        G.nodes[adj[0]]['adjacency'] = len(adj[1].keys())

    if do_layout:
        start_time = time.time()
        pos = nx.kamada_kawai_layout(G)
        print(f"layout computed \t {time.time() - start_time}")
        start_time = time.time()
        pos = dict((k, p.tolist()) for k, p in pos.items())
        print(f"layout converted\t {time.time() - start_time}")
    else:
        pos = []

    print(
        f"-----------------------\ngraph sent\t\t {time.time() - t_0}\n-----------------------")
    return jsonify({'graph': nx.node_link_data(G), 'pos': pos})


@app.route('/deliver/get_author_min_max_year/<auth_prnd_id>', methods=['GET'])
def get_author_min_max_year(auth_prnd_id):
    print(auth_prnd_id, file=sys.stderr)
    auth_prnd_id = str(auth_prnd_id)
    # запрос всех публикаций автора
    pub_request = requests.get(
        'http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(auth_prnd_id))
    min_ = None
    max_ = None
    # определение минимальной и максимальной даты
    if pub_request.status_code == 200:
        try:
            response_data = pub_request.json()
            for entry in response_data:
                publ_year = int(entry["publ_year"])
                if min_ is None or publ_year < min_:
                    min_ = publ_year
                if max_ is None or publ_year > max_:
                    max_ = publ_year
        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    return jsonify({'min': min_, 'max': max_})


@app.route('/deliver/produce_author_publications_count', methods=['POST'])
def produce_author_publications_count():
    json_data = request.get_json()
    author = json_data.get('author')
    time_range = json_data.get('range')

    print("---------")
    print(author)
    print(time_range)
    print("---------")
    author_prnd_id = translate_back(author)

    pub_request = requests.get(
        'http://193.232.208.28/api/v2.0/publications?author_prnd_id=' + str(author_prnd_id))

    print(pub_request)
    pubs_cnt = 0
    no_deltas_pubs_cnt = 0
    print(author_prnd_id, file=sys.stderr)
    pubs_dates = {}
    # запрос всех публикаций автора
    if pub_request.status_code == 200:
        try:
            response_data = pub_request.json()
            for entry in response_data:

                id = entry["prnd_id"]
                publ_year = int(entry["publ_year"])
                if time_range is None or (int(time_range[0]) <= publ_year and publ_year <= int(time_range[1])):
                    # есть ли термины, рассчитаны ли дельты
                    pubs_dates.setdefault(publ_year, [0, 0, 0])
                    pubs_dates[publ_year][2] += 1
                    if os.path.exists(os.path.join(PATH_TO_PUB, str(id))):
                        pubs_dates[publ_year][1] += 1

                        deltas_file = os.path.join(os.path.join(
                            PATH_TO_PUB, str(id)), 'deltas.csv')
                        deltas: np.ndarray = np.loadtxt(
                            deltas_file, dtype=np.int32)
                        has_non_zero = np.any(deltas != 0)
                        if has_non_zero:
                            pubs_dates[publ_year][0] += 1
                            pubs_cnt += 1
                    else:
                        no_deltas_pubs_cnt += 1

        except json.JSONDecodeError:
            print("Error decoding JSON response PUBS")
    print(pubs_cnt, "/", no_deltas_pubs_cnt + pubs_cnt, file=sys.stderr)
    print(pubs_dates, file=sys.stderr)

    return jsonify({"count": pubs_cnt})


def give_thesaurus(use_terms=[]):
    # дает вершины и связи для графа полного тезауруса
    path = '/home/isand_user/isand/web_application/back/thesaurus_graph/'

    ut = use_terms

    # if (ut != [] and not ut is None):  # обновить хранимый граф, используя таблицы

    with open(path + "table.pkl", "rb") as f:
        table = pickle.load(f)
    adjacency_table = np.load(path + "adjacency_table.npy")

    nodes = []
    terms = [item["term"] for item in table]
    for term in terms:
        if term in ut or ut == []:
            nodes.append(term)

    edges = []
    for i in range(adjacency_table.shape[0]):
        for j in range(adjacency_table.shape[1]):
            if adjacency_table[i, j] == 1:
                if terms[i] in nodes and terms[j] in nodes:
                    edges.append([terms[i], terms[j]])

    # else:  # просто вернуть граф
    #     with open(path + "graph.pkl", "rb") as f:
    #         nodes, edges = pickle.load(f)

    return nodes, edges


@app.route('/deliver/produce_theaurus_graph', methods=['POST'])
def load_graph(use_t=None):
    # формирует подграф графа тезауруса, удовлетворящий дополнительным параметрам
    # и наполняет вершины дополнительными данными
    available_terms = request_terms().json['terms']
    everything = {}
    everything['основные термины'] = available_terms[0:17]
    everything['объекты управления'] = available_terms[17:41]
    everything['воздействия и сигналы'] = available_terms[41:88]
    everything['виды управления'] = available_terms[88:133]
    everything['управляющие объекты'] = available_terms[133:142]
    everything['системы управления'] = available_terms[142:149]
    everything['законы управления'] = available_terms[149:156]
    everything['элементы систем управления'] = available_terms[156:170]
    everything['структуры систем управления'] = available_terms[170:186]
    everything['состояния систем управления'] = available_terms[186:233]
    everything['свойства систем управления'] = available_terms[233:247]
    everything['общие термины'] = available_terms[247:]
    # завершили подгрузку данных тезауруса, выполняем построение

    if (use_t != None):
        use_terms = use_t
        use_root = []
    else:
        json_data = request.get_json()
        use_terms = json_data.get('use_terms')
        use_root = json_data.get('use_root')

    nodes, edges = give_thesaurus(use_terms)

    # создание объекта графа
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    print("constructed graph")
    # добавление информации к узлам графа
    labels = [str(n) for n in G.nodes()]

    i = 0
    for n in G.nodes:
        G.nodes[n]['label'] = labels[i]
        G.nodes[n]['derives'] = []
        G.nodes[n]['derived_from'] = []

        if n in everything['общие термины']:
            G.nodes[n]['subset'] = 0
        elif n in everything['основные термины']:
            G.nodes[n]['subset'] = 1
        elif n in everything['объекты управления']:
            G.nodes[n]['subset'] = 2
        elif n in everything['воздействия и сигналы']:
            G.nodes[n]['subset'] = 3
        elif n in everything['виды управления']:
            G.nodes[n]['subset'] = 4
        elif n in everything['управляющие объекты']:
            G.nodes[n]['subset'] = 5
        elif n in everything['системы управления']:
            G.nodes[n]['subset'] = 6
        elif n in everything['законы управления']:
            G.nodes[n]['subset'] = 7
        elif n in everything['элементы систем управления']:
            G.nodes[n]['subset'] = 8
        elif n in everything['структуры систем управления']:
            G.nodes[n]['subset'] = 9
        elif n in everything['состояния систем управления']:
            G.nodes[n]['subset'] = 10
        elif n in everything['свойства систем управления']:
            G.nodes[n]['subset'] = 11
        else:
            G.nodes[n]['subset'] = 12
        i += 1

    # рассчет "определяющей силы" терминов
    # через таблицу смежности считать нельзя, так как там смежность со всеми терминами, а граф может быть частичный
    for edge in G.edges:
        G.nodes[edge[0]]['derived_from'].append(edge[1])
        G.nodes[edge[1]]['derives'].append(edge[0])

    for node in G.nodes:
        if len(G.nodes[node]['derived_from']) == 0:
            if len(G.nodes[node]['derives']) == 0:
                G.nodes[node]['dp'] = 0
            else:
                G.nodes[node]['dp'] = -1
        else:
            G.nodes[node]['dp'] = len(
                G.nodes[node]['derives']) / len(G.nodes[node]['derived_from'])
    print("taking subgraph")
    # строем дерево с корнем use_roots
    if use_root != []:
        nodes_in_tree = []
        level = [use_root[0]]
        max_depth = use_root[1]

        cnt = 0
        while level != []:
            next_level = []
            for node in level:
                if not node in nodes_in_tree:
                    nodes_in_tree.append(node)

                    next_level += G.nodes[node]['derives']
            level = next_level
            cnt += 1
            if (cnt > max_depth):
                break

        return load_graph(use_t=nodes_in_tree)

    start_time = time.time()
    if use_root == [] and use_terms == []:
        pos = request_layout().json['pos']
        print(f"layout loaded \t {time.time() - start_time}")
    else:
        print("computing layout")
        pos = nx.kamada_kawai_layout(G)
        print(f"layout computed \t {time.time() - start_time}")
        start_time = time.time()
        pos = dict((k, p.tolist()) for k, p in pos.items())
        print(f"layout converted\t {time.time() - start_time}")

        # if use_root == [] and use_terms == []:
        #     save_layout(pos)
        #     print(f"layout saved \t {time.time() - start_time}")

    print("graph sent")

    return jsonify({'graph': nx.node_link_data(G), 'pos': pos})


@app.route('/deliver/request_terms', methods=['POST'])
def request_terms():
    path = '/home/isand_user/isand/web_application/back/thesaurus_graph/'

    with open(path + "table.pkl", "rb") as f:
        table = pickle.load(f)

        terms = [item["term"] for item in table]

        return jsonify({'terms': terms})


@app.route('/deliver/request_layout', methods=['POST'])
def request_layout():
    # возвращает камада кавай заранее посчитанную для полного тезауруса
    path = '/home/isand_user/isand/web_application/back/thesaurus_graph/'

    with open(path + 'standart_layout') as f:
        pos = json.load(f)

    return jsonify({"pos": pos})


def save_layout(pos):
    # возвращает камада кавай заранее посчитанную для полного тезауруса
    path = '/home/isand_user/isand/web_application/back/thesaurus_graph/'

    with open(path + 'standart_layout', 'w') as f:
        json.dump(pos, f)

    # return jsonify({"pos": pos})


@app.route('/deliver/pathes', methods=['GET'])
def get_pathes():
    selected_level = request.args.get('level')
    with open("../cached_files/available_pathes.json", 'r') as file:
        loaded_folder_info = json.load(file)
    output_array = loaded_folder_info[selected_level]
    output_array = [{'label': path['last_path'], 'value': path['full_path']}
                    for path in output_array]
    return jsonify(output_array)


@app.route('/deliver/raitings', methods=['GET'])
def get_raitings():
    selected_path = str(request.args.get('path'))
    selected_type = str(request.args.get('type'))
     # Получаем параметр show_all (необязательный, по умолчанию False)
    show_all = request.args.get('show_all', 'false').lower() == 'true'
    get_journals_from_db()

    if selected_type in ['authors', ]:
        cached_info = '../cached_files/scientist_ranged.csv'
        dict_info = authors_dict
    elif selected_type == 'labs':
        cached_info = '../cached_files/labs_ranged.csv'
        dict_info = labs_dict
    elif selected_type == 'conferences':
        cached_info = '../cached_files/conferences_ranged.csv'
        dict_info = get_conferences_from_db()
    elif selected_type == 'journals':
        cached_info = '../cached_files/journals_ranged.csv'
        dict_info = get_journals_from_db()
    elif selected_type == 'organizations':
        cached_info = '../cached_files/organizations_ranged.csv'
        dict_info = get_organizations_from_db()
    elif selected_type == 'cities':
        cached_info = '../cached_files/cities_ranged.csv'
        dict_info = get_cities_from_db()
    else:
        return jsonify({"error": "Invalid selected_type"}), 400

    df = pd.read_csv(cached_info)
    df.set_index('Path', inplace=True)
    filtered_row = df.loc[selected_path].dropna()
    sorted_df = filtered_row.sort_values(ascending=False)
    list_of_dicts = [{'id': id, 'value': value, 'name': dict_info[id]}
                     for id, value in sorted_df.to_dict().items() if id in dict_info]

    # Ограничиваем вывод до 30 элементов если show_all=False
    if not show_all:
        list_of_dicts = list_of_dicts[:30]

    return jsonify(list_of_dicts)

def handle_count_request(entities):
    response = requests.get(f'http://193.232.208.28/api/v2.5/{entities}/analysis/get_count')
    print(response)

    entities_count = 0
    if response.status_code == 200:
        try:
            response_data = response.json()
            entities_count = response_data[0]['count']
        except json.JSONDecodeError:
            print("Error decoding JSON response 'get_count'")
    else:
        print(f"Request failed with status code: {response.status_code}")
    return entities_count

@app.route('/deliver/get_total_count', methods=['GET'])
def get_total_count():
    authors_count = handle_count_request('authors')
    publications_count = handle_count_request('publications')

    return jsonify({"publications": publications_count, "authors": authors_count})

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------Metrics
# Константы
API_URL = 'https://kb-isand.ipu.ru/grapher'
PROFILES_DIR = 'Profiles'
DATA_FILES = {
    'authors_level2': os.path.join(PROFILES_DIR, 'all_authors_resultsfl2.json'),
    'authors_level3': os.path.join(PROFILES_DIR, 'all_authors_resultsfl3.json'),
    'all_authors': os.path.join(PROFILES_DIR, 'get_all_available_authors.json')
}
os.makedirs(PROFILES_DIR, exist_ok=True)

def prepare_data(author_id, factor_level=3):
    """Подготавливает данные для расчетов из папки Profiles"""
    data_file = DATA_FILES[f'authors_level{factor_level}']

    if not os.path.exists(data_file):
        raise ValueError(f"Файл данных для уровня {factor_level} не найден в папке {PROFILES_DIR}")

    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Данные целевого автора
    target_data = next((item for item in data if item['author_id'] == author_id), None)
    if not target_data:
        raise ValueError(f"Данные для автора {author_id} не найдены в файле {data_file}")

    target_terms = {item['term_name']: item['term_freq_stochastic'] for item in target_data['data']}
    target_series = pd.Series(target_terms)

    # Данные всех авторов
    records = []
    for entry in data:
        if entry['author_id'] == author_id:
            continue
        for term in entry['data']:
            records.append({
                'author_id': entry['author_id'],
                'term_name': term['term_name'],
                'value': term['term_freq_stochastic']
            })

    all_df = pd.DataFrame(records)
    pivot_df = all_df.pivot_table(index='term_name', columns='author_id', values='value', fill_value=0)

    return target_series, pivot_df

@app.route('/deliver/calculate_smm', methods=['GET'])
def calculate_smm():
    """
    API endpoint для расчета метрики SMM
    Использует файлы из папки Profiles
    Возвращает результаты в формате JSON
    """
    try:
        # Получаем параметры из запроса
        author_id = int(request.args.get('author_id'))
        factor_level = int(request.args.get('factor_level', default=3))
        k1_coefficient = float(request.args.get('k1_coefficient', default=1.0))

        # Валидация параметров
        if factor_level not in [2, 3]:
            return jsonify({"error": "factor_level должен быть 2 или 3"}), 400
        if not 0 <= k1_coefficient <= 1:
            return jsonify({"error": "k1_coefficient должен быть между 0 и 1"}), 400

        # Проверяем наличие файлов в папке Profiles
        if not all(os.path.exists(f) for f in DATA_FILES.values()):
            return jsonify({"error": f"Не найдены файлы данных в папке {PROFILES_DIR}"}), 404

        # Подготавливаем данные
        target_series, pivot_df = prepare_data(author_id, factor_level)

        # Рассчитываем метрики
        results = []
        for other_id in pivot_df.columns:
            other_data = pivot_df[other_id]
            common_terms = target_series.index.intersection(other_data.index)

            if len(common_terms) > 1:
                corr, _ = spearmanr(target_series[common_terms], other_data[common_terms])
                results.append({'author_id': int(other_id), 'SMM': float(round(corr, 4))})

        # Сортируем результаты
        results = sorted(results, key=lambda x: x['SMM'], reverse=True)

        # Применяем коэффициент отсечения
        if 0 < k1_coefficient < 1:
            results = results[:int(len(results) * k1_coefficient)]

        # Формируем JSON ответ
        response = {
            "author_id": author_id,
            "factor_level": factor_level,
            "k1_coefficient": k1_coefficient,
            "results": results,
            "count": len(results)
        }

        return jsonify(response)

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Ошибка при расчете SMM: {str(e)}"}), 500

def log_message(message):
    """Логирование сообщений с временной меткой"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", file=sys.stderr)

def check_data_files():
    """Проверяет наличие всех необходимых файлов данных"""
    try:
        return all(os.path.exists(fname) for fname in DATA_FILES.values())
    except Exception as e:
        log_message(f"Ошибка при проверке файлов данных: {str(e)}")
        return False

def fetch_authors_list():
    """Получаем список всех доступных авторов через API"""
    url = f'{API_URL}/get_all_available_authors'
    response = requests.get(url, timeout=30)

    if response.status_code != 200:
        raise Exception(f"HTTP ошибка {response.status_code} при получении списка авторов")

    authors = response.json()

    # Сохраняем список авторов
    with open(DATA_FILES['all_authors'], 'w', encoding='utf-8') as f:
        json.dump(authors, f, ensure_ascii=False, indent=4)

    return authors

def fetch_author_data(author_id, factor_level):
    """Получаем данные для конкретного автора и уровня через API"""
    params = {
        'auth_prnd_id': author_id,
        'profile_format': 'stochastic',
        'recalculate': True,
        'factor_level': factor_level
    }

    response = requests.get(
        f'{API_URL}/get_author_deltas',
        params=params,
        timeout=30
    )

    if response.status_code != 200:
        raise Exception(f"HTTP ошибка {response.status_code} для автора {author_id}")

    return response.json()

def download_author_level_data(authors, factor_level):
    """Загружаем данные авторов для указанного уровня"""
    results = []
    data_file = DATA_FILES[f'authors_level{factor_level}']

    log_message(f"Начало загрузки данных уровня {factor_level}")

    for author in tqdm(authors, desc=f"Загрузка данных (уровень {factor_level})"):
        author_id = author.get('prnd_author_id')
        if not author_id:
            log_message("Пропущен автор без ID")
            continue

        try:
            data = fetch_author_data(author_id, factor_level)
            if data:
                results.append({
                    'author_id': author_id,
                    'data': data
                })
        except Exception as e:
            log_message(f"Ошибка при получении данных для автора {author_id}: {str(e)}")
            continue

    # Сохраняем результаты
    try:
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        log_message(f"Данные уровня {factor_level} успешно сохранены в {data_file}")
    except Exception as e:
        log_message(f"Ошибка сохранения данных уровня {factor_level}: {str(e)}")
        raise

def download_data_process():
    """Фоновый процесс загрузки данных"""
    try:
        log_message("Начало загрузки данных авторов")

        # 1. Получаем список всех авторов
        authors = fetch_authors_list()

        if not authors:
            raise Exception("Не удалось получить список авторов")

        # 2. Загружаем данные для каждого уровня
        download_author_level_data(authors, 2)
        download_author_level_data(authors, 3)

        log_message("Все данные успешно загружены и сохранены")
    except Exception as e:
        log_message(f"Ошибка в процессе загрузки: {str(e)}")

@app.route('/deliver/download_author_data', methods=['GET'])
def download_author_data():
    """
    Запуск фоновой загрузки данных авторов
    Возвращает:
    - JSON с статусом начала загрузки
    """
    try:
        # Запускаем процесс загрузки в отдельном потоке
        Thread(target=download_data_process).start()

        return jsonify({
            "status": "success",
            "message": "Загрузка данных авторов начата в фоновом режиме",
            "data_files": {
                "authors_list": DATA_FILES['all_authors'],
                "level2_data": DATA_FILES['authors_level2'],
                "level3_data": DATA_FILES['authors_level3']
            },
            "check_status_url": "/check_download_status"
        }), 202

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": f"Ошибка при запуске процесса загрузки: {str(e)}"
        }), 500

@app.route('/deliver/check_download_status', methods=['GET'])
def check_download_status():
    """
    Проверка статуса загруженных файлов
    Возвращает:
    - JSON с информацией о файлах
    """
    try:
        files_info = {}
        for name, path in DATA_FILES.items():
            if os.path.exists(path):
                size = os.path.getsize(path)
                files_info[name] = {
                    "path": path,
                    "exists": True,
                    "size_bytes": size,
                    "size_mb": round(size / (1024 * 1024), 2),
                    "last_modified": datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                }
            else:
                files_info[name] = {
                    "path": path,
                    "exists": False
                }

        return jsonify({
            "status": "success",
            "files": files_info,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": f"Ошибка при проверке статуса: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(debug=False, host='193.232.208.58', port=5000)
