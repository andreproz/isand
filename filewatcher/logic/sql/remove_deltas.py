from typing import *

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import connect as psycopg2Connect


class DeltaRemover:

    def __init__(self, a_cursor: psycopg2.extensions.cursor):
        a_cursor.execute('select id, level from factors;')

        # Маппинг из уровня в факторы:
        raw_level_factors: Dict[int, List[int]] = {}
        for _ in a_cursor.fetchall():
            i: int = _[0]
            level: int = _[1]
            if level not in raw_level_factors:
                raw_level_factors[level] = []
            raw_level_factors[level].append(i)

        self.__m_raw_level_factors: Mapping[int, Iterable[int]] = raw_level_factors

        # Маппинг из фактора в уровни:
        raw_factor_levels: Dict[int, int] = {}
        for level in raw_level_factors:
            for factor in raw_level_factors[level]:
                raw_factor_levels[factor] = level

        self.__m_raw_factor_levels: Mapping[int, int] = raw_factor_levels

        # Строковая версия raw_level_factors:
        self.__m_raw_level_factors_str: Mapping[int, str] = {
            _: ','.join(str(__) for __ in self.__m_raw_level_factors[_]) for _ in self.__m_raw_level_factors
        }

        self.__m_cursor = a_cursor

    def remove_delta(self, a_l: Container[int], a_f: Container[int]):
        print(f"In remove_delta:  {a_f}")

        # Отображение "номер статьи, номер уровня" -> "сумма стохастиков которые были удалены с уровня"
        # (см. число x в комментах ниже):
        removed_stochastics: Dict[Tuple[int, int], float] = {}  # номер публикации, сумма удалённых стохастиков

        # Список строк таблицы deltas которые нужно удалить:
        removed_deltas: List[int] = []

        # Перебрать таблицу deltas:
        self.__m_cursor.execute('select id, publication_id, factor_id, stochastic from sim0n_deltas;')
        for i, publication, factor, stoch in self.__m_cursor.fetchall():
            if publication in a_l and factor in a_f:
                factor_level = self.__m_raw_factor_levels[factor]

                key = (publication, factor_level)
                if publication not in removed_stochastics:
                    removed_stochastics[key] = 0
                removed_stochastics[key] += stoch

                # Добавленная проверка на None
                if i is not None:
                    removed_deltas.append(i)

        # Удалить removed_deltas из deltas:
        # Добавленная проверка на пустой список
        print("removed_deltas", removed_deltas)
        self.__m_cursor.execute(f'delete from sim0n_deltas where factor_id in (' + ','.join(str(x) for x in a_f) + ');')

        # Обновить стохастики:
        # Мы удалили лишние дельты, теперь хотим умножить на константу оставшиеся стохастики
        # чтобы их сумма стала равна единице:
        # (y_1 + y_2 + ... + y_n) = S + x = 1
        # C * S = 1
        # Отсюда получаем
        # C = 1 / (1 - x)
        print("removed_stochastics", removed_stochastics)
        for key in removed_stochastics:
            if abs(removed_stochastics[key] - 1.0) < 0.0000000001:
                continue
            C = 1.0 / (1.0 - removed_stochastics[key])
            self.__m_cursor.execute(
                f'update sim0n_deltas set stochastic = '
                f'case '
                f'    when stochastic * {str(C)} > 1 then 0.9999 '
                f'    when stochastic * {str(C)} = 0 then 0.0000000001 '
                f'    else stochastic * {str(C)} '
                f'end '
                f'where publication_id = {key[0]} '
                f'and factor_id in (' + self.__m_raw_level_factors_str[key[1]] + ');'
            )


if __name__ == '__main__':   
    conn_account = psycopg2Connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    db_cursor = conn_account.cursor()  
    print('Init DeltaRemover')
    del_rm: DeltaRemover = DeltaRemover(db_cursor)

    db_cursor.execute("SELECT DISTINCT(publication_id) FROM sim0n_deltas")
    pubs = [pub[0] for pub in db_cursor.fetchall()]
    #print("pubs", pubs)
    '''
    1229 = ИБ, 673 = JPEG4, 2767 = ток
    669 = 'Алгоритмы сжатия изображений, видеокодеки, оценивание качества изображения'
    1226 = 'Организационно-правовые вопросы защиты информации'
    1246 = 'Криптография'
    1264 = 'Техническая защита информации'
    1278 = 'Программно-аппаратные средства защиты информации'
    1296 = 'Информационная безопасность телекоммуникационных и вычислительных сетей'
    1306 = 'Управление информационными рисками / информационной безопасностью, Аналитические вопросы ИБ'
    2765 = 'Электричество и магнетизм',
    606 = 'Обработка фото- и видеоданных'
    1225 = 'Информационная безопасность и кибербезопасность'
    '''
    excluded_factors = [1229,
                        1226, 1246, 1229, 1264, 1278, 1296, 1306,
                        1225
                       ]
    
    del_rm.remove_delta(pubs, excluded_factors)
    conn_account.commit()
    

    db_cursor.close()
    conn_account.close()