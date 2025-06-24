from typing import *
import requests

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import io
import spacy
from psycopg2 import connect as psycopg2Connect

from filecore import getDirByID, get_segmentated_json, get_text

#from .filecore import getDirByID, get_segmentated_json, get_text

_g_ru = spacy.load("ru_core_news_md")
_g_en = spacy.load("en_core_web_md")


class ProfileBuilder:

    @staticmethod
    def __get_term_lemmas(a_str: str, a_lang: int) -> Iterable[str]:
        if a_lang == 1:
            doc = _g_ru(a_str)
        else:
            doc = _g_en(a_str)
        return [token.lemma_ for token in doc]

    @staticmethod
    def __get_text_lemmas(a_input: TextIO, a_lang: int) -> Iterable[str]:
        if a_lang == 1:
            doc = _g_ru(a_input.read())
        else:
            doc = _g_en(a_input.read())
        return (token.lemma_ for token in doc)

    def __init__(self, a_cursor: psycopg2.extensions.cursor):
        a_cursor.execute('select factor_id, variant, language_id from factor_name_variants;')
        variants: Iterable[Tuple[int, str, int]] = a_cursor.fetchall()
        # variants: Iterable[Tuple[int, str, int]] = [(1, 'протокол консенсуса', 1)]

        self.__m_raw_fnv: List[Tuple[int, Iterable[str]]] = []
        for variant in variants:
            self.__m_raw_fnv.append(
                (variant[0], self.__get_term_lemmas(variant[1], variant[2]))
            )

        new_fnv: Dict[str, List[Tuple[int, Iterable[str]]]] = {}
        for variant in self.__m_raw_fnv:
            try:
                key = next(iter(variant[1]))
                if key not in new_fnv:
                    new_fnv[key] = []
                new_fnv[key].append(variant)
            except StopIteration:
                raise ValueError(f'Термин под номером {variant[0]} имеет в качестве леммы пустую строку')

        self.__m_fnv: Mapping[str, Iterable[Tuple[int, Iterable[str]]]] = new_fnv

    def make_profile_from_text(self, a_input: TextIO, a_lang: int) -> Mapping[int, float]:
        text_lemmas: Iterable[str] = self.__get_text_lemmas(a_input, a_lang)
        return self.make_profile_from_lemmas(text_lemmas)

    def make_profile_from_lemmas(self, a_input: Iterable[str]) -> Mapping[int, float]:

        # Результат:
        res: Dict[int, float] = {}

        # Хранилище текущих потенциальных терминов:
        current_terms: List[Tuple[int, Iterator[str]]] = []

        # Перебрать все токены:
        for lemma in a_input:

            # Проверить итераторы в `current_terms`:
            next_current_terms: List[Tuple[int, Iterator[str]]] = []
            for current_term in current_terms:
                current_term_id: int = current_term[0]
                current_term_it: Iterator[str] = current_term[1]

                try:
                    next_term_lemma = next(current_term_it)
                    if next_term_lemma == lemma:
                        next_current_terms.append(current_term)
                except StopIteration:
                    if current_term_id not in res:
                        res[current_term_id] = 0
                    res[current_term_id] += 1
            current_terms = next_current_terms

            # Найти такие варианты названий факторов, чьё первое слово равняется `lemma`:
            if lemma in self.__m_fnv:
                for term in self.__m_fnv[lemma]:
                    it = iter(term[1])
                    next(it)
                    current_terms.append((term[0], it))

        # Проработать граничный случай:
        for current_term in current_terms:
            current_term_id: int = current_term[0]
            current_term_it: Iterator[str] = current_term[1]

            try:
                next(current_term_it)
            except StopIteration:
                if current_term_id not in res:
                    res[current_term_id] = 0
                res[current_term_id] += 1

        return res

    def get_factor_name_variants(self) -> Iterable[Tuple[int, Iterable[str]]]:
        return self.__m_raw_fnv
    
    # Фильтрация словаря дельт терминов
    def get_filtered_profile(self, profile: Mapping[int, float], exceptions: Iterable[str]) -> Mapping[int, float]:
        new_profile = {}
        for factor_id, score in profile.items():
            for variant in self.get_factor_name_variants():                
                if variant[0] == factor_id:
                    if not any(term in exceptions for term in variant[1]):
                        new_profile[factor_id] = score
                        break 
        return new_profile
    
    # Расширение профила для надфакторов
    def add_factor_profile(self, profile: Mapping[int, float], cursor_account_db: psycopg2.extensions.cursor) -> Mapping[int, float]:
        # Отображение преемников для каждого фактора
        successors_map = {}
        cursor_account_db.execute("SELECT predecessor_id, successor_id FROM factor_graph_edges")
        for row in cursor_account_db:
            predecessor_id, successor_id = row
            if successor_id in successors_map: successors_map[successor_id].append(predecessor_id)
            else: successors_map[successor_id] = [predecessor_id]
        
        new_profile = {}
        
        for factor_id, score in profile.items():
            new_profile[factor_id] = score

        # Для каждого фактора в профиле, проверяем, есть ли у него предки
        # Если есть, то рекурсивно вычисляем дельту для каждого предка и добавляем ее в new_profile
        def dfs(factor_id, predecessor_score, predecessor_id = None):
            #print(f"In dfs factor_id = {factor_id}, predecessor_score = {predecessor_score}, predecessor_id = {predecessor_id}")

            if factor_id in new_profile and predecessor_id: new_profile[factor_id] += predecessor_score
            elif factor_id not in new_profile and predecessor_id: new_profile[factor_id] = predecessor_score
        
            if factor_id not in successors_map: 
                #print("factor_id not in successors_map")
                return None
            
            for successor_id in successors_map[factor_id]: dfs(successor_id, new_profile[factor_id], factor_id)

        for factor_id, score in profile.items(): dfs(factor_id, score)

        return new_profile  

    # Подсчёт стохастических векторов
    def get_stochastic(self, profile: Mapping[int, float], cursor_account_db: psycopg2.extensions.cursor) -> Mapping[int, float]:

        stochastic_vectors = {} 

        # Все уровни из таблицы factors
        cursor_account_db.execute("SELECT DISTINCT level FROM factors")
        levels = [row[0] for row in cursor_account_db]
        #print("levels", levels)

        # Для каждого уровня вычисляем сумму дельт факторов на этом уровне, которые встречаются в profile
        for level in levels:
            cursor_account_db.execute("SELECT id FROM factors WHERE level = %s", (level,))
            factor_ids = [row[0] for row in cursor_account_db]
            #print("factor_ids", factor_ids) 
            total_delta = sum(profile[factor_id] for factor_id in factor_ids if factor_id in profile)
            #print("total_delta", total_delta)

            # Для каждого фактора на этом уровне, который встречается в profile, вычисляем стохастический вектор
            for factor_id in factor_ids:
                if factor_id in profile:
                    stochastic_vector = profile[factor_id] / total_delta
                    stochastic_vectors[factor_id] = stochastic_vector

        return stochastic_vectors
    

def get_deltas_stochastic(filepath, pub_annotation = None):    
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    db_cursor = conn_account.cursor()  

    print('Init ProfileBuilder')
    pb: ProfileBuilder = ProfileBuilder(db_cursor)
 
    text = ""
    if filepath:
        #print("filepath", filepath)
        json_dict = get_segmentated_json(filepath)
        #print("json_dict", json_dict)
        content = get_text(filepath)
        #print("content", content)
        publ_p_text = None
        if json_dict:
            p_text = json_dict['publications'][0]['publication']['p_text']
            p_text_add = json_dict['publications'][0]['publication']['p_text_add'] 
            publ_p_text = p_text + p_text_add
            if len(publ_p_text) > 0: text = publ_p_text
            elif pub_annotation: text = pub_annotation
            elif content: text = content
            else: return None, None
        elif pub_annotation: text = pub_annotation
        elif content: text = content
        else: return None, None
    else: return None, None
    #print("text", text)

    if len(text) >= 1000000: 
        print("Very long publication")
        return None, None
    
    #doc = _g_ru(text)
    #print([token.lemma_ for token in doc])

    res = pb.make_profile_from_text(io.StringIO(text), 1)
    #print("len(res)", len(res))
    #print(res)

    exceptions = ["иб", "be", "jpeg4"]
    #test_exceptions = ['approach', 'подход', 'robust', 'робастный']
     
    new_res = pb.get_filtered_profile(res, exceptions)
    
    #print("len(new_res)", len(new_res))
    #print("new_res", new_res)

    new_res = pb.add_factor_profile(new_res, db_cursor)
    
    #print("len(new_res)", len(new_res)) 
    #print("new_res", new_res)

    stochastic_vectors = pb.get_stochastic(new_res, db_cursor)
    #print("stochastic_vectors", stochastic_vectors)

    return new_res, stochastic_vectors 

def get_deltas_stochastic_from_mathnet(jrnid: str, paperid: str) -> Tuple[Mapping[int, float], Mapping[int, float]]:
    url = f"https://www.mathnet.ru/api/texts/{jrnid}{paperid}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch text data for jrnid {jrnid} and paperid {paperid}. Status code: {response.status_code}")
        return None, None
    
    try:
        data = response.json()
        fulltext = data.get('fulltext', None)
    except requests.exceptions.JSONDecodeError:
        print(f"Failed to decode JSON text response for jrnid {jrnid} and paperid {paperid}")
        return None, None
    print("response", response) 
    #print("fulltext", fulltext)
    
    if not fulltext:
        print(f"No fulltext found for jrnid {jrnid} and paperid {paperid}")
        return None, None

    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    db_cursor = conn_account.cursor()  

    pb: ProfileBuilder = ProfileBuilder(db_cursor)

    res = pb.make_profile_from_text(io.StringIO(fulltext), 1)

    exceptions = ["иб", "be", "jpeg4"]
    new_res = pb.get_filtered_profile(res, exceptions)
    new_res = pb.add_factor_profile(new_res, db_cursor)
    stochastic_vectors = pb.get_stochastic(new_res, db_cursor)

    db_cursor.close()
    conn_account.close()

    return new_res, stochastic_vectors

def deltas_run():
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    cursor = conn_account.cursor()    
    cursor.execute("SELECT * FROM publications")   
    publs = cursor.fetchall()
    bad_publications = []

    for pub in publs:
        pub_id = pub[0]
        pub_annotation = pub[11]
        print("\n\npub_id", pub_id, "pub_annotation", pub_annotation) 
        filepath = getDirByID(pub_id) 
        print("filepath", filepath)
        if not (filepath or pub_annotation): 
            bad_publications.append(pub_id)
            continue
        deltas, stochastics = get_deltas_stochastic(filepath, pub_annotation)    

        print("deltas", deltas)
        print("stochastics", stochastics)

        if not deltas: 
            bad_publications.append(pub_id)
            continue

        for factor_id, value in deltas.items():
            stochastic = stochastics[factor_id]
            print("factor_id", factor_id, "value", value, "stochastic_vector", stochastic)
            cursor.execute("SELECT * FROM shadow_deltas WHERE publication_id=%s AND factor_id=%s", (pub_id, factor_id))
            result = cursor.fetchone()
            if result is None:
                cursor.execute("INSERT INTO shadow_deltas (publication_id, factor_id, value, stochastic) VALUES (%s, %s, %s, %s)", (pub_id, factor_id, value, stochastic))
            else: 
                cursor.execute("UPDATE shadow_deltas SET value=%s, stochastic=%s WHERE publication_id=%s AND factor_id=%s", (value, stochastic, pub_id, factor_id))

    conn_account.commit() 

    print("bad_publications", bad_publications)

    cursor.close()
    conn_account.close()

def fix_deltas():    
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )
    db_cursor = conn_account.cursor() 
    
    # Список ID факторов, которые нужно обновить
    factor_ids = [2767]

    # Сохранить исходные predecessor_id перед их обновлением
    db_cursor.execute("""
    SELECT successor_id, predecessor_id FROM factor_graph_edges
    WHERE successor_id IN %s;
    """, (tuple(factor_ids),))
    successor_predecessor_map = {row[0]: row[1] for row in db_cursor.fetchall()}

    # Обновление predecessor_id в таблице factor_graph_edges
    update_predecessor_sql = """
    UPDATE factor_graph_edges
    SET predecessor_id = 2
    WHERE successor_id IN %s;
    """
    db_cursor.execute(update_predecessor_sql, (tuple(factor_ids),))

    # Изменение дельт для всех факторов и их предков
    def update_parent_deltas(factor_id, delta, publication_id, old_predecessor_id):
        #print(f"update_parent_deltas for {old_predecessor_id}")
        if old_predecessor_id != 2:
            # Вычитать дельты у предка
            db_cursor.execute("""
            UPDATE deltas
            SET value = value - %s
            WHERE publication_id = %s AND factor_id = %s;
            """, (delta, publication_id, old_predecessor_id))
            # Получить предка предка
            old_predecessor_predecessor_id = successor_predecessor_map.get(old_predecessor_id)
            if old_predecessor_predecessor_id:
                # Рекурсивно обновить дельты для предка
                update_parent_deltas(old_predecessor_id, delta, publication_id, old_predecessor_predecessor_id) 

    for factor_id in factor_ids:
        db_cursor.execute("""
        SELECT publication_id, value FROM deltas WHERE factor_id = %s;
        """, (factor_id,))
        current_deltas = db_cursor.fetchall()
        
        for pub_id, delta in current_deltas:
            old_predecessor_id = successor_predecessor_map[factor_id]
            update_parent_deltas(factor_id, delta, pub_id, old_predecessor_id)
            # Прибавить дельты к новому общенаучному предку
            db_cursor.execute("""
        UPDATE deltas
        SET value = value + %s
        WHERE publication_id = %s AND factor_id = 2;
        """, (delta, pub_id))
            
    # Удалить записи с нулевыми дельтами
    db_cursor.execute("""
    DELETE FROM deltas WHERE value = 0;
    """)

    # Пересчёт stochastic после всех изменений
    db_cursor.execute("""
    UPDATE deltas
    SET stochastic = CASE WHEN total > 0 THEN value / total ELSE 0 END
    FROM (
        SELECT publication_id, level, SUM(value) as total
        FROM deltas
        JOIN factors ON factors.id = deltas.factor_id
        GROUP BY publication_id, level
    ) as calc_totals
    WHERE deltas.publication_id = calc_totals.publication_id
      AND deltas.factor_id IN (
          SELECT id FROM factors WHERE level = (
              SELECT level FROM factors WHERE id = deltas.factor_id
          )
      );
    """)

    # Применить изменения и закрыть соединения
    print("Try to commit")
    conn_account.commit()
    db_cursor.close()
    conn_account.close()

if __name__ == "__main__":
    fix_deltas()
