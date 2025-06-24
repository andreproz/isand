from typing import *

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import spacy
import pathlib
import time
import itertools

import tqdm

class Lemmatizer(Callable[[str | TextIO], Iterable[str]]):
    """
    Функционал, преобразующий текст или поток в список из лемматизированных слов.
    """

    def __init__(self):
        self.__m_ru = spacy.load("ru_core_news_md")
        self.__m_en = spacy.load("en_core_web_md")

    @staticmethod
    def detect_language(a_text: str) -> str:
        for c in a_text[:1000]:
            if c.lower() in {
                "а", "б", "в", "г", "д", "е", "ё", "ж", "з", "и", "й", "к",
                "л", "м", "н", "п", "р", "т", "у",
                "ф", "х", "ц", "ч", "ш", "щ", "ъ", "ы", "ь", "э", "ю", "я"
                #, 'о', 'c'
            }:
                return 'ru'
        return 'en'

    def __call__(self, a_input: str | TextIO) -> Iterable[str]:
        text: str
        if isinstance(a_input, str):
            text = a_input
        else:
            text = a_input.read()

        iterators = []
        n = 950000
        for part in (text[i:i+n] for i in range(0, len(text), n)):

            lang = Lemmatizer.detect_language(part)

            if lang == 'ru':
                doc = self.__m_ru(part)
            else:
                doc = self.__m_en(part)

            iterators.append((token.lemma_ for token in doc))

        return itertools.chain(*iterators)


g_override_lemmas: Final[bool] = False


class ProcessedPublications(Iterable[Tuple[int, Iterable[str]]]):
    """
    Класс, способный проходить по списку лемматизированных текстов публикаций.
    """

    def __init__(
            self,
            a_publications_folder: pathlib.Path,
            a_lemmatizer: Optional[Callable[[TextIO], Iterable[str]]]
    ):
        self.__m_publications_folder: pathlib.Path = a_publications_folder
        self.__m_lemmatizer: Optional[Callable[[TextIO], Iterable[str]]] = a_lemmatizer

    def __len__():
        return 51344

    def __iter__(self) -> Iterator[Tuple[int, Iterable[str]]]:

        def make_iter() -> Generator[Tuple[int, Iterable[str]], None, None]:
            for path_to_text in self.__m_publications_folder.rglob('*.text.txt'):
                path_to_folder = path_to_text.parents[0]
                publication_id: int = int(''.join(path_to_folder.parts[-8:]), 16)

                # with open(path_to_text, 'r') as text:
                #     teeeeex = text.read()
                # #     text_len = len(teeeeex)
                # #     test_lll: bool = text_len > 950000
                #     lggg = Lemmatizer.detect_language(teeeeex)
                # #     text_lll2 = lggg != 'ru'
                #     print(path_to_text.parents[0], '>>', lggg)
                #     if lggg == 'ru':
                #         print('!!!!!!!!!!!!!!!!!!!!')
                #         print('!!!!!!!!!!!!!!!!!!!!')

                path_to_lemmas: pathlib.Path = path_to_folder / 'processed_text.txt'
                if g_override_lemmas or not path_to_lemmas.exists():
                    try:
                        with open(path_to_text, 'r') as text, open(path_to_lemmas, 'w') as lemmas:
                            tokens = self.__m_lemmatizer(text)
                            for token in tokens:
                                lemmas.write(token + '\n')
                    except Exception as e:
                        # В случае любой ошибки в ходе обработки фалйа с леммами -- удалить его:
                        if path_to_lemmas.exists():
                            path_to_lemmas.unlink()
                        raise e

                with open(path_to_lemmas, 'r') as lemmas:
                    yield publication_id, (_.strip() for _ in lemmas)

        return make_iter()


class PostgresConnector:
    """
    Соединение с базой данных.
    """

    def __init__(self) -> None:
        self.dbname = 'account_db'
        self.user = 'isand'
        self.password = 'sf3dvxQFWq@!'
        self.host = '193.232.208.58'
        self.port = '5432'

    def __enter__(self) -> psycopg2.extensions.cursor:
        self.__m_connection = psycopg2.connect(
            dbname=self.dbname, user=self.user,
            password=self.password, host=self.host, port=self.port
        )
        self.__m_cursor = self.__m_connection.cursor()
        return self.__m_cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__m_connection.commit()
        self.__m_connection.close()
        self.__m_cursor.close()

    # Подключение к базе данных
    def get_cursor(self) -> psycopg2.extensions.cursor:
        return self.__m_cursor


class ProfileCalculationProcess:

    def __init__(self):
        raise NotImplemented

    class ProfileBuilder(Callable[[Iterable[str]], Mapping[int, Tuple[float, float]]]):
        """
        Законченный построитель профилей.

        Является функционалом, принимающий последовательность лемм, и возвращающий профиль в виде словаря.
        """

        def __init__(
                self,
                a_cursor: psycopg2.extensions.cursor,
                a_lemmatizer: Callable[[str, int], Iterable[str]]
        ):

            # Построение словаря уровней для каждого фактора:
            a_cursor.execute('select id, level from factors;')
            self.__m_factor_level: Mapping[int, int] = {_[0]: _[1] for _ in a_cursor.fetchall()}

            # Кеширование рёбер графа факторов:
            a_cursor.execute('select predecessor_id, successor_id from factor_graph_edges;')
            edges: Iterable[Tuple[int, int]] = list(a_cursor.fetchall())

            # Построение словаря предка каждого из фактора:
            self.__m_factor_predecessor: Mapping[int, int] = {_[1]: _[0] for _ in edges}

            # Построение словаря потомков каждого из фактора:
            def make_factor_successors() -> Mapping[int, Iterable[int]]:
                nonlocal edges

                res: Dict[int, List[int]] = {}
                for edge in edges:
                    if edge[0] not in res:
                        res[edge[0]] = []
                    res[edge[0]].append(edge[1])

                return res

            self.__m_factor_successors: Mapping[int, Iterable[int]] = make_factor_successors()
            del edges

            # Построение словаря лемматизированных представлений факторов на основе таблицы factor_name_variants:
            def make_raw_fnv() -> Mapping[int, Iterable[Iterable[str]]]:
                nonlocal a_cursor

                a_cursor.execute('select factor_id, variant from factor_name_variants;')
                res: Dict[int, List[Iterable[str]]] = {}
                for it in a_cursor.fetchall():
                    factor_id, variant = it
                    if factor_id not in res:
                        res[factor_id] = []

                    lemmas = list(a_lemmatizer(variant))
                    if len(lemmas) == 0:
                        raise ValueError(f'У фактора ({factor_id}) есть пустое строковое представление')

                    res[factor_id].append(lemmas)

                return res

            # Форма: номер фактора -> список лемматизированных представлений в виде списков токенов:
            self.__m_raw_fnv: Mapping[int, Iterable[Iterable[str]]] = make_raw_fnv()

            # Построение словаря поиска лемматизированных представлений факторов по первому слову:
            def make_first_token_search() -> Mapping[str, Iterable[Tuple[int, Iterable[str]]]]:
                nonlocal a_cursor

                res: Dict[str, List[Tuple[int, Iterable[str]]]] = {}
                for factor_id, variants in self.__m_raw_fnv.items():
                    for variant in variants:
                        first_token: str = next(iter(variant))  # Не должно выкидывать StopIteration
                        if first_token not in res:
                            res[first_token] = []
                        res[first_token].append((factor_id, variant))

                return res

            # Форма: лемма -> список из пар (номер фактора, лемматизированное представление в виде списка токенов):
            self.__m_first_token_search: Mapping[str, Iterable[Tuple[int, Iterable[str]]]] = make_first_token_search()

        def __call__(self, a_input: Iterable[str]) -> Mapping[int, Tuple[float, float]]:

            # Итоговый словарь, из которого формируется ответ.
            # Формат: номер фактора -> дельта:
            pure_profile: Dict[int, int] = {}

            # Метод добавления новой дельты:
            def add_delta(a_factor_id: int, a_delta: int) -> None:
                nonlocal pure_profile

                if a_factor_id not in pure_profile:
                    pure_profile[a_factor_id] = 0
                pure_profile[a_factor_id] += a_delta

                # Допустимый уровень рекурсии:
                _: Optional[int] = self.__m_factor_predecessor.get(a_factor_id)
                if _ is not None:
                    add_delta(_, a_delta)

            # Хранилище текущих потенциальных терминов:
            current_terms: List[Tuple[int, Iterator[str]]] = []

            # Перебрать все токены:
            for lemma in a_input:
                if len(lemma) == 0:
                    continue

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
                        add_delta(current_term_id, 1)
                current_terms = next_current_terms

                # Найти такие варианты названий факторов, чьё первое слово равняется `lemma`:
                variants: Optional = self.__m_first_token_search.get(lemma)
                if variants is not None:
                    for term in variants:
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
                    add_delta(current_term_id, 1)

            # Подсчёт stochastics:
            level_sum: Dict[int, int] = {}
            for factor_id, delta in pure_profile.items():
                level: int = self.__m_factor_level[factor_id]

                if level not in level_sum:
                    level_sum[level] = 0
                level_sum[level] += delta

            # Непосредственно результирующий словарь:
            res: Dict[int, Tuple[float, float]] = {
                factor_id: (delta, delta / level_sum[self.__m_factor_level[factor_id]])
                for factor_id, delta in pure_profile.items()
            }

            return res

    @staticmethod
    def add_profile_for_publication(
            a_cursor: psycopg2.extensions.cursor,
            a_publication_id: int,
            a_profile: Mapping[int, Tuple[float, float]]
    ) -> None:
        """
        Занесение профиля в базу данных.
        """

        entry: Final[str] = ','.join(
            f'({a_publication_id}, {k}, {v[0]}, {v[1]})'
            for k, v
            in a_profile.items()
        )

        if len(entry) > 0:
            a_cursor.execute(
                f'insert into sim0n_deltas '
                f'(publication_id, factor_id, value, stochastic) '
                f'values {entry};'
            )

    @staticmethod
    def run(
            a_cursor: psycopg2.extensions.cursor,
            a_profile_builder: Callable[[Iterable[str]], Mapping[int, Tuple[float, float]]],
            a_processed_publications: Iterable[Tuple[int, Iterable[str]]]
    ) -> None:
        a_cursor.execute('truncate sim0n_deltas;')

        # count_of_publications: Final[int] = 51344
        # counter: int = 0
        # start_time: float = time.time()

        for publication_id, lemmas in tqdm.tqdm(a_processed_publications, total=51344):
            profile = a_profile_builder(lemmas)
            ProfileCalculationProcess.add_profile_for_publication(a_cursor, publication_id, profile)

            # N = 10
            # M = 10
            # # Список из времени обработки пачек из N статей:
            # last_delta_time_list: List[float] = [0]
            # counter += 1
            #
            # if counter % N == 0:
            #     current_time: float = time.time()
            #     passed_time: float = current_time - start_time
            #
            #     last_delta_time_list.append(current_time - last_delta_time_list[-1])
            #     if len(last_delta_time_list) == M + 1:
            #         last_delta_time_list = last_delta_time_list[1:]
            #
            #     average_time_per_pub = sum(last_delta_time_list) / (len(last_delta_time_list)*N)
            #     expected = (count_of_publications - counter) * average_time_per_pub
            #     print(
            #         f'processing: {counter} / {count_of_publications}, '
            #         f'{counter / count_of_publications:%}, '
            #         f'passed time: {passed_time:.0f} s., '
            #         f'per pub: {average_time_per_pub:.1f} s., '
            #         f'expected: {expected/(60*60):.1f} h.'
            #     )


if __name__ == '__main__':

    print('initialization')
    text_db_path = pathlib.Path('/var/storages/data/publications/papers')

    with PostgresConnector() as cursor:
        lemmatizer = Lemmatizer()
        profile_builder = ProfileCalculationProcess.ProfileBuilder(cursor, lemmatizer)
        processed_publications = ProcessedPublications(text_db_path, lemmatizer)

        print('start')
        ProfileCalculationProcess.run(cursor, profile_builder, processed_publications)
