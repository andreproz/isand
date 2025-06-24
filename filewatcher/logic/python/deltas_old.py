import json
import numpy as np
import re, os
from typing import *
import natasha

g_natasha_segmenter = natasha.Segmenter()
g_natasha_morph_tagger = natasha.NewsMorphTagger(natasha.NewsEmbedding())
g_natasha_morph_vocab = natasha.MorphVocab()

def calc_deltas_for_paper(
    paper_text: str,
    ru_lemmas,
    en_lemmas,
    ru_words
) -> None:
    """
    Для заданного номера работы вычисляет массив дельт и записывает его в соответствующую
    работе папку. Дельта это число встреч данного термина в тексте.

    Аргумент lemmas содержит словарь вида (идентификатор термина: список лемм термина),
    где список представляет собой список лемм термина. Например, для термина "компактное
    множество" будет ['компактный', 'множество'].

    Результирующий файл представляет собой csv с целыми числами. i-е число -- дельта для
    термина i-го номера. Размер массива равен максимальному значению идентификатора термина.
    Например, для lemmas = {0: [...], 1: [...], 3: [...]} результирующий файл будет
    содержить массив из 4-х чисел, третье из которых будет равно нулю.

    Поиск терминов в данном варианте алгоритма происходит путём лемматизации слов текста
    и терминов, и последующего поиска терминов как подстрок. Более продвинутый, но пока
    не реализованный способ использует семантический анализ.
    """
    
    # Получить объект natasha.Doc из текста a_text.
    doc: natasha.Doc = natasha.Doc(paper_text)
    doc.segment(g_natasha_segmenter)
    doc.tag_morph(g_natasha_morph_tagger)

    tokens: List[natasha.doc.DocToken] = doc.tokens
    # Подсчёт вхождения термина из списка lemmas в тексте
    res = {_: 0 for _ in ru_lemmas}
    headers = {}

    for token in tokens:
        token.lemmatize(g_natasha_morph_vocab)
        lemma: str = token.lemma

        for i in ru_lemmas:
            
            try:
                f: bool = False
                if i not in headers:
                    if lemma == ru_lemmas[i][0]:
                        headers[i] = 0
                        f = True
                else:
                    f = True

                if f:
                    if lemma == ru_lemmas[i][headers[i]]:
                        headers[i] += 1
                        if headers[i] >= len(ru_lemmas[i]):
                            del headers[i]
                            res[i] += 1
                    else:
                        del headers[i]
            except:
                pass
        for i in en_lemmas:
            
            try:
                f: bool = False
                if i not in headers:
                    if lemma == en_lemmas[i][0]:
                        headers[i] = 0
                        f = True
                else:
                    f = True

                if f:
                    if lemma == en_lemmas[i][headers[i]]:
                        headers[i] += 1
                        if headers[i] >= len(en_lemmas[i]):
                            del headers[i]
                            res[i] += 1
                    else:
                        del headers[i]
                        
            except:
                pass
        
    res = {key: value for key, value in res.items() if value != 0}
    res = {' '.join(ru_words[key]): value for key, value in res.items()}
    return res
    
    
def get_deltas(path_to_file, ru_lemmas, en_lemmas, ru_words):
# def get_deltas(path_to_file, path_to_term = '/home/isand_user/isand/servers/filewatcher/static/terms.json'):
    with open(path_to_file, 'r', encoding='utf-8') as text:
        d_text = json.load(text)
        d_text = d_text['publications'][0]['publication']['p_text'] + d_text['publications'][0]['publication']['p_text_add']
    print("path_to_file", path_to_file)
    print("ru_lemmas", ru_lemmas)

    with open('/home/unreal_dodic/filewatcher/static/ru_terms.json', 'r', encoding='utf-8') as lemmas:
        ru_lemmas = json.load(lemmas)
        ru_lemmas_words = {int(key): value for key, value in ru_lemmas['term_to_words'].items()}
        ru_lemmas = {int(key): value for key, value in ru_lemmas['term_to_lemma'].items()}
    
    with open('/home/unreal_dodic/filewatcher/static/en_terms.json', 'r', encoding='utf-8') as lemmas:
        en_lemmas = json.load(lemmas)
        en_lemmas = {int(key): value for key, value in en_lemmas['term_to_lemma'].items()}

    
    res = calc_deltas_for_paper(d_text, ru_lemmas, en_lemmas, ru_lemmas_words)
    
    with open(f"{path_to_file[:-16]}deltas.json", "w", encoding="utf-8") as json_file:
        json.dump(res, json_file, ensure_ascii=False, indent=2)
    return res

if __name__ == '__main__':
    with open('/home/isand_user/isand/servers/filewatcher/static/ru_terms.json', 'r', encoding='utf-8') as lemmas:
        ru_lemmas = json.load(lemmas)
        ru_lemmas_words = {int(key): value for key, value in ru_lemmas['term_to_words'].items()}
        ru_lemmas = {int(key): value for key, value in ru_lemmas['term_to_lemma'].items()}
    
    with open('/home/isand_user/isand/servers/filewatcher/static/en_terms.json', 'r', encoding='utf-8') as lemmas:
        en_lemmas = json.load(lemmas)
        en_lemmas = {int(key): value for key, value in en_lemmas['term_to_lemma'].items()}
        
    get_deltas('/var/storages/00/00/00/00/00/00/00/0a/1499-1095.segmentated.json', ru_lemmas, en_lemmas, ru_lemmas_words)
    pass