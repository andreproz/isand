import json
import numpy as np
import re, os
import requests
from typing import *
import natasha

g_natasha_segmenter = natasha.Segmenter()
g_natasha_morph_tagger = natasha.NewsMorphTagger(natasha.NewsEmbedding())
g_natasha_morph_vocab = natasha.MorphVocab()

def getData(url = 'http://193.232.208.28/api/v2.0/factor/get_terms'):
    if not url: return ''
    
    data = None
    response = requests.get(url)
    data = response.json()
    return data

def lemitizeWords(term_words):
    lemma_words = [] 
    for word in term_words:
        doc = natasha.Doc(word)
        doc.segment(g_natasha_segmenter)
        doc.tag_morph(g_natasha_morph_tagger)
        for token in doc.tokens: token.lemmatize(g_natasha_morph_vocab)
        if len(doc.tokens[-1].lemma) > 0: lemma_words.append(doc.tokens[-1].lemma)
    return lemma_words

def upgradeRuTerms(data, 
                   terms_path = '/home/isand_user/isand/servers/filewatcher/static/ru_terms.json'):
    if not data: return ''
    new_word_data = {item["term_id"]: item["term_names"].split(';')[-1].strip().split() for item in data} 
    new_lemma_data = {k: lemitizeWords(v) for k, v in new_word_data.items()} 
    new_data = {"term_to_words": new_word_data, "term_to_lemma": new_lemma_data}
    with open(terms_path, 'w', encoding='utf-8') as file:
        json.dump(new_data, file, ensure_ascii=False, indent=2)

def merge_lists(terms):
    words_bad = [term.strip().split() for term in terms]
    merged_words_bag = list(set([word for sublist in words_bad for word in sublist]))
    return merged_words_bag

def upgradeEnTerms(data, 
                   terms_path = '/home/isand_user/isand/servers/filewatcher/static/en_terms.json'):
    if not data: return ''
    new_word_data = {item["term_id"]: merge_lists(item["term_names"].split(';')[:-1]) for item in data} 
    new_lemma_data = {k: lemitizeWords(v) for k, v in new_word_data.items()} 
    new_data = {"term_to_words": new_word_data, "term_to_lemma": new_lemma_data}
    with open(terms_path, 'w', encoding='utf-8') as file:
        json.dump(new_data, file, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    server_json = getData()
    #upgradeRuTerms(server_json)
    upgradeEnTerms(server_json)
