import json
import os
import requests
import zipfile
from python.config import *

def getPRNDJson(url: str = URL_PRND, url_param: list[str] = URL_PARAM, code_word: str = CODE_WORD) -> str:
    # print(url + 'card' + '/0/' + code_word)
    prndJson = requests.get(url + 'card' + '/0/' + code_word)
    prndJson = prndJson.json()
    total = list(prndJson.values())[1]
    pages = int(total) // 1000 + 1 if int(total) % 1000 != 0 else int(total) // 1000
    print(pages)

def getAllJson(url, code_word, page=0):
    sortedBuffer = []
    newJsonPubl = requests.get(url + f'{page}/' + code_word)
    newJsonPubl = newJsonPubl.json()
    # определение кол-ва страниц
    pages = int(newJsonPubl['total_publications']) // 1000 + 1 if int(newJsonPubl['total_publications']) % 1000 != 0 else int(newJsonPubl['total_publications']) // 1000      

    for page in range(pages):
        newJsonPubl = requests.get(url + f'{page}/' + code_word)
        newJsonPubl = newJsonPubl.json()
        # проходим по публикациям, которые были присланы в данной итерации
        for pub in newJsonPubl['publications']:
            if pub['field_pub_file'] != None:
                sortedBuffer += [{'nid': pub['nid'], 'field_pub_file': pub['field_pub_file']}]
    del newJsonPubl
    sortedBuffer.sort(key=lambda x: int(x['nid']), reverse=True)
    with open(SITE_INFO_PATH + 'newinfo.txt', 'w') as file:
        for pub in sortedBuffer:
            file.write(pub['nid'] + ' ' + URL_PDF_PUBLICATION + pub['field_pub_file'].replace('\\', '') + '\n')
            
if __name__ == '__main__':
    getPRNDJson()