from requests import get
from os import makedirs, walk, remove, listdir, rename
from os.path import join, exists, dirname, basename, relpath,splitext, isdir
from subprocess import run
from shutil import copy, rmtree
from fitz import open as fitzOpen
from datetime import datetime
from tarfile import open as tarOpen
from psycopg2.extras import Json
import asyncio
import json
# from sql.connection import *
# from python.grobid_parser import *
# from python.deltas_old import get_deltas
# from python.config import *
# from python.crcod import crcod
# from python.duplicate import duplicate_test
# from sql.config import *


from .sql.basawork import upload2DB
from .sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from .sql.postgres import SQLQuery
from .python.grobid_parser import create_json_structure, TEIFile
from .python.config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP
from .python.deltas_old import get_deltas
from .python.crcod import crcod
from .python.duplicate import duplicate_test
from .python.filecore import createDir, clear_temp, move_files 
'''
from sql.basawork import upload2DB
from sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from sql.postgres import SQLQuery
from python.grobid_parser import create_json_structure, TEIFile
from python.config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP
from python.deltas import get_deltas
from python.crcod import crcod
from python.duplicate import duplicate_test
from python.filecore import createDir, clear_temp, move_files  
'''
DEEP_SIGN_CONST = 16

def move_files(src_dir, dst_dir):
    for filename in listdir(src_dir):
        src_file = join(src_dir, filename)
        dst_file = join(dst_dir, filename)
        copy(src_file, dst_file)
    return None
       
def downloadPdf(url, path_to_temp = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP): 
    response = get(url)
    if response.status_code >= 200 and response.status_code < 300:
        path_to_temp = path_to_temp + '/' + url.split('/')[-1][:-4]
        path_with_publ = path_to_temp + '/' + url.split('/')[-1]
        try:
            makedirs(path_to_temp)
            with open(path_with_publ, 'wb') as file:
                file.write(response.content)
        except:
            pass
        return path_with_publ
    return None

def downloadAllPdf(dif, path_to_temp=FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP):
    print("In downloadAllPdf")
    id_prnd_path = []
    for publ in dif:
        res = downloadPdf(publ[1], path_to_temp)
        if res:
            id_prnd_path += [[publ[0], res]]
    return id_prnd_path

def tarGzipArchivator(path_to_source = FILE_SYSTEM_PATH + FILE_SYSTEM_SOURCE_DATA_PRND, path_to_temp = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP):
    print("tarGzipArchivator run")
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    time_str = now.strftime('%Y-%m-%d-%H-%M-%S')
    dest_dir = join(path_to_source, date_str)
    makedirs(dest_dir, exist_ok=True)
    filename = f"{time_str}.tar.gz"
    archive_path = join(dest_dir, filename)
    with tarOpen(archive_path, "w:gz") as tar:
        for root, dirs, files in walk(path_to_temp, topdown=True):
            dirs[:] = [d for d in dirs if d != 'archive']
            for file in files:
                file_path = join(root, file)
                arcname = relpath(file_path, start=path_to_temp)
                tar.add(file_path, arcname=arcname)
    
def grobidAnalysis(path, path_to_grobid='/home/isand_user/isand/servers/grobid/grobid_client_python/grobid_client/grobid_client.py'):
    command = f'python3 {path_to_grobid} --n 3 --input {path} --include_raw_citations --include_raw_affiliations processFulltextDocument'
    run(command, shell=True)
    
def grobidParse(path, ext_source = ""):
    tei_file = TEIFile(path)
    tei_file.parse()
    tei_file.data = create_json_structure(tei_file.data, ext_source)
    with open(f'{path[:-15]}.segmentated.json', 'w', encoding='utf-8') as json_file:
        json.dump(tei_file.data, json_file, ensure_ascii=False, indent=2)
    creation_date = tei_file.data['creation_date']
    pub = tei_file.data['publications'][0]['publication'] 
    p_title = pub['p_title']
    p_title_add = pub['p_title_add']
    p_text = pub['p_text']
    p_text_add = pub['p_text_add']
    authors = []
    if 'authors' in pub:
        if len(pub['authors']) == 0: pass
        elif 'author' in pub['authors'][0]:
            authors = [i['author']['a_fio'] for i in pub['authors']]
        else:
            authors = [i['a_fio'] for i in pub['authors']]
    return creation_date, p_title, p_title_add, p_text, p_text_add, authors

def getAllPubl(url, code_word, page=0):
    print("Into getAllPubl")
    sortedBuffer = []
    newJsonPubl = get(url + f'{page}/' + code_word)
    print("Get newJsonPubl by url", url + f'{page}/' + code_word)
    
    if newJsonPubl.status_code < 200 and newJsonPubl.status_code >= 300:
        return None
    newJsonPubl = newJsonPubl.json()
    
    # определение кол-ва страниц
    pages = int(newJsonPubl['total_publications']) // 1000 + 1 if int(newJsonPubl['total_publications']) % 1000 != 0 else int(newJsonPubl['total_publications']) // 1000      

    for page in range(pages):
        print("page", page, "|", pages)
        newJsonPubl = get(url + f'{page}/' + code_word)
        print("Get newJsonPubl by url", url + f'{page}/' + code_word)
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

def getNewPubl(url, code_word, page=0):
    sortedBuffer = []
    newJsonPubl = get(url + f'{page}/' + code_word)
    if newJsonPubl.status_code < 200 and newJsonPubl.status_code >= 300:
        return None
    newJsonPubl = newJsonPubl.json()

    for pub in newJsonPubl['publications']:
        if pub['field_pub_file'] != None:
            sortedBuffer += [{'nid': pub['nid'], 'field_pub_file': pub['field_pub_file']}]
    del newJsonPubl

    sortedBuffer.sort(key=lambda x: int(x['nid']), reverse=True)

    with open(SITE_INFO_PATH + 'newinfo.txt', 'w') as file:
        for pub in sortedBuffer:
            file.write(pub['nid'] + ' ' + URL_PDF_PUBLICATION + pub['field_pub_file'].replace('\\', '') + '\n')

def getDif(path, newpath):
    dif = []
    try:
        with open(newpath, 'r') as newfile, open(path, 'r') as file:
            nid = file.readline().split()[0]
            for line in newfile:
                if line.split()[0] == nid:
                    break
                dif += [line.split()]
        return dif
    except FileNotFoundError:
        with open(newpath, 'r') as newfile:
            for line in newfile:
                dif += [line.split()]
        return dif

def getTextPdf(path):
    filename_without_extension, _ = splitext(path.replace("'", ""))
    cleaned_path = path.replace("'", "")
    
    with fitzOpen.open(cleaned_path) as doc, open(f'{filename_without_extension}.text.txt', 'w') as file:
        text = chr(12).join([page.get_text() for page in doc])
        file.write(text)

def tempAnalysis(id_prnd_path) -> None:    
    new_publ = []
    for publ in id_prnd_path:
        if exists(publ[1]):
            grobid_flag = False
            pdf_path = publ[1]
            foldername = dirname(pdf_path)
            print(foldername)
            crcod_dict = crcod(pdf_path)
            grobidAnalysis(foldername)
            p_title, p_title_add, p_text, p_text_add, authors, res_dupl, deltas, creation_date = '', '', '', '', '', '', '', ''
            parametres = dict()
            if exists(f'{pdf_path[:-3]}grobid.tei.xml'):
                grobid_flag = True
                creation_date, p_title, p_title_add, p_text, p_text_add, authors = grobidParse(f'{pdf_path[:-3]}grobid.tei.xml', "prnd")
                deltas = get_deltas(pdf_path)
                getTextPdf(pdf_path) 
            parametres['grobid_authors'] = authors  
            parametres['creation_date'] = creation_date 
            parametres['doi'] = "" 
            parametres["grobid_title"] = p_title
            parametres["prnd_key"] = publ[0]
            parametres["deltas"] = deltas
            pub_id = upload2DB(parametres)
            if not pub_id:  
                print("None pub_id")
                continue
            print("add pub_id", pub_id)
            new_path = createDir(pub_id)
            print("foldername", foldername, "new_path", new_path)
            move_files(foldername, new_path) 
            new_publ.append(pub_id)
    return new_publ

def check_consistenty():
    first_line_info, last_line_info, first_line_newinfo = None, None, None
    with open(SITE_INFO_PATH + 'info.txt', 'r') as file:
        first_line_info = file.readline()
    with open(SITE_INFO_PATH + 'info.txt', 'r') as file: 
        for last_line_info in file: pass
    with open(SITE_INFO_PATH + 'newinfo.txt', 'r') as file:
        first_line_newinfo = file.readline()
    if len(first_line_info) * len(first_line_newinfo) == 0: return -1 
    first_id_info, last_id_info  = int(first_line_info.split()[0]), int(last_line_info.split()[0])
    if len(first_line_newinfo.split()) > 2: return -2
    fist_id_newinfo =int(first_line_newinfo.split()[0])
    if first_id_info > fist_id_newinfo and last_id_info > fist_id_newinfo:
        with open(SITE_INFO_PATH + 'newinfo.txt', 'r') as file:
            newinfo_content = file.read()
        with open(SITE_INFO_PATH + 'info.txt', 'a') as file:
            file.write("\n")
        with open(SITE_INFO_PATH + 'info.txt', 'a') as file:
            file.write(newinfo_content)
 
async def update(ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None):
    getAllPubl(URL_PUBLICATIONS, CODE_WORD) 
    dif = getDif(SITE_INFO_PATH + 'info.txt', SITE_INFO_PATH + 'newinfo.txt')  #[[id, url], [id, url]]
    #print("dif", dif)
    id_prnd_path = downloadAllPdf(dif)
    #print("id_prnd_path", id_prnd_path)
    #return None
    tarGzipArchivator()
    new_publications = None
    new_publications = tempAnalysis(id_prnd_path)
    #update_info() #Запись в info.txt с первой строки
    clear_temp() #Очистка /var/storages/data/workgroup/temp
    return new_publications

if __name__ == '__main__':
    print("In da update")
    #check_consistenty() записывает старые id из newinfo в info
    new_publications = asyncio.run(update())
    print("new_publications", new_publications)
    # print(grobidParse('/var/storages/data/publications/prnd/00/00/00/00/00/00/00/0a/1499-1095.grobid.tei.xml'))
    pass