from flask import jsonify
import numpy as np
import os
from typing import *
import json

g_term_json_path = '/home/isand_user/isand/web_application/back/terms/terms.json'
g_struct_pub_path = '/home/isand_user/isand/web_application/back/struct_pub/'

g_term_json = json.load(open(g_term_json_path, 'r'))
g_term_to_words = {int(i): g_term_json['term_to_words'][i]
                   for i in g_term_json['term_to_words']}
g_term_to_lemmas = {int(i): g_term_json['term_to_lemma'][i]
                    for i in g_term_json['term_to_lemma']}
g_thesaurus = g_term_json['thesaurus']


def get_terms_from_area():
    for i in range(10):
        f'{i}'
    # Генерация текста для каждого столбца
    text_list = [f'Термин {i}' for i in range(100)]
    result = {'terms': text_list}
    return jsonify(result)


def __delta_dict(
    a_l: Iterable[int],
    a_view_area: Mapping[Any, Iterable[int]],
    a_transformer: Optional[Callable[[int, int, np.ndarray, int], int]] = None,
    a_base_dir=g_struct_pub_path
) -> Mapping[Any, Mapping[int, int]]:
    """
    Возвращает словарь с теми же ключами что и у a_view_area. В значениях словаря
    будет записан словарь номеров терминов и значений их дельт для работ из a_l.
    Если вставить вместо словаря сумму этих дельт, то получится результат __delta_sum().

    Есть возможность преобразовывать читаемые значения при помощи a_transformer.
    a_transformer будет применён каждый раз при чтении значения из ячейки таблицы.
    a_transformer принимает следующие аргументы:
    - значение дельты термина;
    - номер термина;
    - массив значений дельт терминов для данного текста;
    - номер текста.

    Parameters
    ----------
    a_l : Iterable[int]
        Множество номеров работ.
    a_view_area : Dict[Any, Iterable[int]]
        Словарь, значения в котором -- множества номеров терминов.
    a_transformer : Optional[Callable[[int, int, np.ndarray, int], int]]
        Лямбда-функция, преобразующая читаемое из файла дельту.
        Если None -- значение не меняется.
    """
    # термины из заданного множеста игнорируются (просто ну надо так, у нас оказывается дубликаты терминов существуют)
    ignore_terms = set([308])

    # Хранение результата:
    res: Dict[Any, Dict[int, int]] = {_: {} for _ in a_view_area}

    # Перебрать каждую работу из a_l:
    for l in a_l:

        # Сформировать путь до папки с работой под номером a_l:
        paper_dir = os.path.join(a_base_dir, str(l))
        # if not os.path.exists(paper_dir):
        #     raise ValueError(f'Папки для статьи под номером {l} не существует')

        # Прочитать массив дельт:
        deltas_file = os.path.join(paper_dir, 'deltas.csv')
        deltas: np.ndarray = np.loadtxt(deltas_file, dtype=np.int32)

        # Запись дельт в res для каждого термина из текущего значения в a_terms:
        for key in a_view_area:
            for term in a_view_area[key]:
                if term in ignore_terms:
                    continue
                if term not in res[key]:
                    res[key][term] = 0
                if a_transformer is None:
                    res[key][term] += deltas[term]
                else:
                    res[key][term] += a_transformer(deltas[term],
                                                    term, deltas, l)

    return res


def __delta_sum(
    a_l: Iterable[int],
    a_view_area: Mapping[Any, Iterable[int]],
    a_transformer: Optional[Callable[[int, int, np.ndarray, int], int]] = None,
    a_base_dir=g_struct_pub_path
) -> Mapping[Any, int]:
    """
    Возвращает словарь с теми же ключами что и у a_view_area. В значениях словаря
    будет записана сумма всех дельт по соответствующим терминам и всем работам a_l.

    Есть возможность преобразовывать читаемые значения при помощи a_transformer.
    a_transformer будет применён каждый раз при чтении значения из ячейки таблицы.
    a_transformer принимает следующие аргументы:
    - значение дельты термина;
    - номер термина;
    - массив значений дельт терминов для данного текста;
    - номер текста.

    Parameters
    ----------
    a_l : Iterable[int]
        Множество номеров работ.
    a_view_area : Dict[Any, Iterable[int]]
        Словарь, значения в котором -- множества номеров терминов.
    a_transformer : Optional[Callable[[int, int, np.ndarray, int], int]]
        Лямбда-функция, преобразующая читаемое из файла дельту.
        Если None -- значение не меняется.
    """

    res = __delta_dict(a_l, a_view_area, a_transformer, a_base_dir)
    return {_: sum(res[_].values()) for _ in res}


def __make_subtree_view(
    a_root_path: Iterable[str],
    a_view_level: int
) -> Mapping[Any, Iterable[int]]:
    """
    Возвращает некоторое множество вершин тезауруса и соответствующее им множество вершин,
    составленное по правилам описанным ниже.

    Берётся вершина R тезауруса по пути a_root_path. Берётся поддерево T, состоящее из R
    и всех вершин-потомков R.

    Возвращает словарь. Ключ -- путь до вершины X из T. Значение -- объединённое множество
    номеров терминов самой вершины X и всех её косвенных потомков.

    Если вершины A и B являются потомками C, а S(X) это множество терминов вершины X, то
    возвращённый словарь будет содержать в зависимости от значения a_n или строки
    - [C, A]: S(C) ∪ S(A)
    - [C, B]: S(C) ∪ S(B)
    или строки
    - [C]: S(C) ∪ S(A) ∪ S(B)

    Результат функции предполагается вставлять непосредственно во второй аргумент 
    функции ::__delta_sum(...).

    Parameters
    ----------
    a_root_path : Iterable[str]
        Путь до вершины-корня рассматриваемого поддерева. Пустой список означает
        корень исходного дерева.
    a_view_level : int
        Уровень подробности.
    """

    global g_thesaurus

    if a_view_level < 0:
        raise ValueError(f'a_view_level ({a_view_level}) меньше нуля')

    # Хранилище результата:
    res: Dict[Any, Iterable[int]] = {}

    # Рекурсивная функция:
    def rec(a_path: Optional[List[str]], a_subtree, a_layer: int, a_parent_terms: Set[int]):
        nonlocal res

        # Добавить в res новую запись если итератор дошёл до нуля:
        if a_layer == 0:
            res[tuple(a_path)] = a_parent_terms

        # Положить в a_parent_terms термины из текущей вершины:
        a_parent_terms.update(a_subtree['terms'])

        children = a_subtree['children']

        # Если нет потомков -- добавить в res:
        if not children and a_layer > 0:
            res[tuple(a_path)] = a_parent_terms
        else:
            # Перебрать потомков:
            for child in children:
                # Если a_layer > 0, то просто продолжить рекурсию.
                # В a_parent_terms передаётся копия текущего a_parent_terms:
                if a_layer > 0:
                    rec(a_path + [child], children[child],
                        a_layer - 1, set(a_parent_terms))
                # Иначе рекурсия продолжается, но уже без обновления пути и итератора.
                # В a_parent_terms передаётся ссылка на текущий a_parent_terms для его
                # последующего обновления:
                else:
                    rec(None, children[child], -1, a_parent_terms)

    # Получить вершину поддерева по пути a_root_path:
    subtree = g_thesaurus
    for key in a_root_path:
        subtree = subtree['children'][key]

    # Начало рекурсии:
    rec(list(a_root_path), subtree, a_view_level, set())

    return res


def build_chart(
    a_author_id: None,
    a_selected_works_id: Iterable[int],
    a_level: int,
    a_selected_scheme_id: int,
    a_cutoff_value: int,
    a_cutoff_terms_value: int,
    a_include_common_terms: bool,
    a_include_management_theory: bool,
    a_path: Iterable[str],
    a_base_dir=g_struct_pub_path
) -> Mapping[str, float]:
    """
    """

    # a_author_id не используется
    # a_include_management_theory не используется

    # Получить выделение:
    view = __make_subtree_view(a_path, a_level)
    if not a_include_common_terms:
        view = {_: view[_] for _ in view if _[0] != 'Общенаучные термины'}

    # =================================================================
    # Расчёт значений

    def common_transformer(d, _1, _2, _3):
        return 0 if d < a_cutoff_terms_value else d

    def bool_transformer(d, _1, _2, _3):
        return 0 if d < a_cutoff_terms_value else 1

    # Здесь происходит ветвление согласно парамеру strategy:
    deltas: Dict[Tuple[str], int] = {}

    # Абсолютный вектор:
    if a_selected_scheme_id == 0:
        if a_cutoff_terms_value > 0:
            deltas = __delta_sum(a_selected_works_id, view, common_transformer)
            deltas_dict = __delta_dict(
                a_selected_works_id, view, common_transformer)
            for key in deltas_dict:

                # Вариант с подсчётом суммы слов терминов
                count: int = sum(len(g_term_to_words[_])
                                 for _ in deltas_dict[key])

                # Вариант с подсчётом терминов
                # count: int = len(deltas_dict[key])

                if count < a_cutoff_terms_value:
                    del deltas[key]
        else:
            deltas = __delta_sum(a_selected_works_id, view, common_transformer)

    # Стохастический вектор:
    elif a_selected_scheme_id == 1:
        if a_cutoff_terms_value > 0:
            deltas = __delta_sum(a_selected_works_id, view, common_transformer)
        else:
            deltas = __delta_sum(a_selected_works_id, view, None)
        summ = float(sum(deltas.values()))
        deltas = {_: deltas[_] / summ for _ in deltas}
    # Булевый вектор:
    elif a_selected_scheme_id == 2:
        deltas = __delta_sum(a_selected_works_id, view, bool_transformer)
        max_x = float(max(deltas.values())) / 100.0
        deltas = {_: (1 if deltas[_] / max_x >=
                      a_cutoff_value else 0) for _ in deltas}
    # По количеству используемых терминов:
    elif a_selected_scheme_id == 3:
        terms_by_col: Mapping[Tuple[str], Mapping[int, int]
                              ] = __delta_dict(a_selected_works_id, view)
        deltas = __delta_sum(a_selected_works_id, view, common_transformer)
        deltas = {_: deltas[_] for _ in deltas if len(
            set(terms_by_col[_])) >= a_cutoff_terms_value}
    # Термины:
    elif a_selected_scheme_id == 4:
        terms = __delta_dict(a_selected_works_id, view, None)
        deltas = {}
        for key in terms:
            for term in terms[key]:
                #
                ww = g_term_to_words[term]
                tu = ''
                for w in ww:
                    tu += w + ' '
                if (tu,) not in deltas:
                    deltas[(tu,)] = 0
                deltas[(tu,)] += terms[key][term]

    # Оси координат для графика:
    max_x = float(max(deltas.values())) / \
        100.0 if max(deltas.values()) != 0 else 1

    # Оставить только последний элемент в кортежах ключей:
    deltas = {_: deltas[_]
              for _ in deltas if deltas[_] / max_x > a_cutoff_value}

    return deltas
