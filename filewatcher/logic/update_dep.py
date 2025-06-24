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
'''
from sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from sql.postgres import SQLQuery
from python.grobid_parser import create_json_structure, TEIFile
from python.config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP
from python.deltas_old import get_deltas
from python.crcod import crcod
from python.duplicate import duplicate_test
'''
DEEP_SIGN_CONST = 16

def move_files(src_dir, dst_dir):
    for filename in listdir(src_dir):
        src_file = join(src_dir, filename)
        dst_file = join(dst_dir, filename)
        copy(src_file, dst_file)
    return None

def insertMove(
        id_user,
        pdf_path, 
        id_prnd, 
        p_title, 
        p_title_add, 
        p_text, 
        p_text_add, 
        authors, 
        res_dupl, 
        postgres, 
        grobid_flag,
        crcod_dict,
        deltas,
        creation_date) -> None:
    
    filename = basename(pdf_path)[:-4]
    foldername = dirname(pdf_path)
    id_publ = postgres.select(table='PUBLICATION',
                              columns=['id_publ'],
                              where_keys=['id_prime'],
                              where_values=[id_prnd])[0][0]
    postgres.update(table='PUBLICATION', 
                    columns=['id_user', 'id_prime', 'filename_pdf', 'filename_crcod', 'fileformat', 'encrypted', 'unknown_chars'], 
                    values=[id_user, id_prnd, filename + '.pdf', filename + '.crcod.json', 'pdf', crcod_dict['encrypted'], crcod_dict['unknown_chars']],
                    where=f' WHERE id_publ = \'{id_publ}\'')
    if grobid_flag:
        creation_date = datetime.strptime(creation_date, '%d.%m.%Y %H:%M:%S')
        postgres.update(table='PUBLICATION', 
                        columns=['filename_grobid', 'filename_segmentated', 'filename_deltas', 'filename_text'], 
                        values=[f'{filename}.grobid.tei.xml', f'{filename}.segmentated.json', f'{filename}.deltas.json', f'{filename}.text.txt'], 
                        where=f' WHERE id_publ = \'{id_publ}\'')
        postgres.insert(table='PUBLICATION2', 
                        columns=['id_publ', 'p_title', 'p_title_add', 'creation_date'], 
                        values=[id_publ, p_title, p_title_add, creation_date])
        postgres.insert(table='PUBL_TEXT', columns=['id_publ', 'p_text', 'p_text_add'], values=[id_publ, p_text, p_text_add])
    if res_dupl:
        postgres.update(table='PUBLICATION', 
                        columns=['duplicate', 'id_dupl'], 
                        values=[True if res_dupl[0] == 'duplicate' else False, res_dupl[1]], 
                        where=f' WHERE id_publ = \'{id_publ}\'')
    if authors:
        for a_fio in authors:
            postgres.insert(table='AUTHOR', 
                            columns=['id_publ', 'a_fio'], 
                            values=[id_publ, a_fio])
    if deltas:
        postgres.insert(table='DELTAS', columns=['id_publ', 'deltas'], values=[id_publ, Json(deltas)])
    source = postgres.select(table='user_isand', 
                    columns=['org_name'], 
                    where_keys=['id_user'],
                    where_values=[id_user])[0][0]
    new_path = createDir(id_publ, source)
    if new_path:
        postgres.update(table='PUBLICATION', 
                        columns=['path'], 
                        values=[new_path], 
                        where=f' WHERE id_publ = \'{id_publ}\'')
    move_files(foldername, new_path)
    return None

def insertMove2(
        id_user,
        pdf_path, 
        id_prnd, 
        p_title, 
        p_title_add,  
        p_text, 
        p_text_add, 
        authors, 
        res_dupl, 
        postgres, 
        grobid_flag,
        crcod_dict,
        deltas,
        creation_date) -> None:
    
    filename = basename(pdf_path)[:-4]
    filename = filename.replace("'", "")
    filename = filename.rstrip(filename[-1])
    foldername = dirname(pdf_path)
    insert_result = postgres.insert(table='PUBLICATION', 
                    columns=['id_user', 'filename_pdf', 'filename_crcod', 'fileformat', 'encrypted', 'unknown_chars'], 
                    values=[id_user, filename + '.pdf', filename + '.crcod.json', 'pdf', crcod_dict['encrypted'], crcod_dict['unknown_chars']])
    id_publ = insert_result[0]
    if grobid_flag:
        creation_date = datetime.strptime(creation_date, '%d.%m.%Y %H:%M:%S')
        postgres.update(table='PUBLICATION', 
                        columns=['filename_grobid', 'filename_segmentated', 'filename_deltas', 'filename_text'], 
                        values=[f'{filename}.grobid.tei.xml', f'{filename}.segmentated.json', f'{filename}.deltas.json', f'{filename}.text.txt'], 
                        where=f' WHERE id_publ = \'{id_publ}\'')
        postgres.insert(table='PUBLICATION2', 
                        columns=['id_publ', 'p_title', 'p_title_add', 'creation_date'], 
                        values=[id_publ, p_title, p_title_add, creation_date])
        postgres.insert(table='PUBL_TEXT', columns=['id_publ', 'p_text', 'p_text_add'], values=[id_publ, p_text, p_text_add])
    if res_dupl:
        postgres.update(table='PUBLICATION', 
                        columns=['duplicate', 'id_dupl'], 
                        values=[True if res_dupl[0] == 'duplicate' else False, res_dupl[1]], 
                        where=f' WHERE id_publ = \'{id_publ}\'')
    if authors:
        for a_fio in authors:
            postgres.insert(table='AUTHOR', 
                            columns=['id_publ', 'a_fio'], 
                            values=[id_publ, a_fio])
    if deltas:
        postgres.insert(table='DELTAS', columns=['id_publ', 'deltas'], values=[id_publ, Json(deltas)])
    source = postgres.select(table='user_isand', 
                    columns=['org_name'], 
                    where_keys=['id_user'],
                    where_values=[id_user])[0][0]
    new_path = createDir(id_publ, source)
    print("new_path", new_path)
    if new_path:
        postgres.update(table='PUBLICATION', 
                        columns=['path'], 
                        values=[new_path], 
                        where=f' WHERE id_publ = \'{id_publ}\'')
    move_files(foldername, new_path)
    return None
    

def createDir(id_publ, source):
    id_publ = hex(id_publ)[2:]
    path = ''
    for i in range(len(id_publ), DEEP_SIGN_CONST):
        id_publ = '0' + id_publ
    for i in range(DEEP_SIGN_CONST // 2):
        path += id_publ[:2] + '/'
        id_publ = id_publ[2:]
    path = path[:-1]
    makedirs(FILE_SYSTEM_PATH + f'publications/{source}/' + path, exist_ok=True)
    return FILE_SYSTEM_PATH + f'publications/{source}/' + path
       
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
    
def grobidParse(path):
    tei_file = TEIFile(path)
    tei_file.parse()
    tei_file.data = create_json_structure(tei_file.data)
    with open(f'{path[:-15]}.segmentated.json', 'w', encoding='utf-8') as json_file:
        json.dump(tei_file.data, json_file, ensure_ascii=False, indent=2)
    return tei_file.data['creation_date'], tei_file.data['publications'][0]['publication']['p_title'], tei_file.data['publications'][0]['publication']['p_title_add'], tei_file.data['publications'][0]['publication']['p_text'], tei_file.data['publications'][0]['publication']['p_text_add'], [i['a_fio'] for i in tei_file.data['publications'][0]['publication']['authors']]

def getAllPubl(url, code_word, page=0):
    sortedBuffer = []
    newJsonPubl = get(url + f'{page}/' + code_word)
    
    if newJsonPubl.status_code < 200 and newJsonPubl.status_code >= 300:
        return None
    newJsonPubl = newJsonPubl.json()
    
    # определение кол-ва страниц
    pages = int(newJsonPubl['total_publications']) // 1000 + 1 if int(newJsonPubl['total_publications']) % 1000 != 0 else int(newJsonPubl['total_publications']) // 1000      

    for page in range(pages):
        newJsonPubl = get(url + f'{page}/' + code_word)
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
    with fitzOpen(f'{path.replace("'", "")}') as doc, open(f'{filename_without_extension}.text.txt', 'w') as file:  # open document
        text = chr(12).join([page.get_text() for page in doc])
        file.write(text)

def tempAnalysis(id_prnd_path, ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None) -> None:
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    
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
                creation_date, p_title, p_title_add, p_text, p_text_add, authors = grobidParse(f'{pdf_path[:-3]}grobid.tei.xml')
                deltas = get_deltas(f'{pdf_path[:-3]}segmentated.json', ru_lemmas, en_lemmas, ru_lemmas_words)
                res_dupl = duplicate_test(p_title, p_title_add, p_text, p_text_add, authors, deltas, postgres)
                getTextPdf(pdf_path)
            #insertMove(1, pdf_path, publ[0], p_title, p_title_add, p_text, p_text_add, authors, res_dupl, postgres, grobid_flag, crcod_dict, deltas, creation_date)
            parametres["title"]  = p_title
            upload2DB(parametres)
    return None
    
def tempJournal(journal, ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None) -> None:
    journal_path = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP + "/" + journal + "/"
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    print("journal_path", journal_path)

    id_user = None
    id_res = postgres.select(table='user_isand', 
                            columns=['id_user'], 
                            where_keys=['org_name'],
                            where_values=[journal.lower()]) 
    if id_res: id_user = id_res[0][0]
    else: 
        id_res = postgres.insert(table='user_isand', 
                    columns=['org_name'], 
                    values=[journal.lower()])
        id_user = id_res[0]
    print("id_user", id_user)
    
    for directory in listdir(journal_path):
        pdf_files = [file for file in listdir(journal_path + directory) if file.endswith('.pdf')]
        if len(pdf_files) == 0: continue
        publ = join(journal_path + f"'{directory}'", f"'{pdf_files[0]}'")
        print("publ", publ) 
        if publ:
            grobid_flag = False
            pdf_path = str(publ)
            foldername = dirname(pdf_path)
            print("foldername",foldername)
            crcod_dict = crcod(pdf_path) 
            grobidAnalysis(foldername)
            p_title, p_title_add, p_text, p_text_add, authors, res_dupl, deltas, creation_date = '', '', '', '', '', '', '', ''
            filename_without_extension, _ = splitext(pdf_path.replace("'", ""))
            if exists(f'{filename_without_extension}.grobid.tei.xml'):
                print("pass here")
                grobid_flag = True
                creation_date, p_title, p_title_add, p_text, p_text_add, authors = grobidParse(f'{filename_without_extension}.grobid.tei.xml')
                deltas = get_deltas(f'{filename_without_extension}.segmentated.json', ru_lemmas, en_lemmas, ru_lemmas_words)
                res_dupl = duplicate_test(p_title, p_title_add, p_text, p_text_add, authors, deltas, postgres)
                getTextPdf(pdf_path)
            insertMove2(id_user, pdf_path, publ[0], p_title, p_title_add, p_text, p_text_add, authors, res_dupl, postgres, grobid_flag, crcod_dict, deltas, creation_date)
    return id_user


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
            
def update_info(): 
    with open('info.txt', 'r') as file:
        first_line_info = file.readline()
    first_id_info = int(first_line_info.split()[0]) 
    with open('new_info.txt', 'r') as file:
        new_info_content = file.readlines()
    new_articles = []
    for line in new_info_content:
        a_id, a_url = line.split()
        if int(a_id) > first_id_info: new_articles.append([a_id, a_url])
    with open('tmp_info.txt', 'w') as file:
    	for n_a in new_articles: file.write(n_a[0] + " " + n_a[1] + "\n")
    	with open('info.txt', 'r') as n_file:
    		for line in n_file:
    			old_id, old_url = line.split()
    			file.write(old_id + " " + old_url + "\n")
    rename('tmp_info.txt', 'info.txt')

def clear_temp():
    PATH = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP 
    NOT_DELETE = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP
    for folder_name in listdir(folder_path):
        folder = join(folder_path, folder_name)
        if isdir(folder) and folder_name != NOT_DELETE:
            rmtree(folder)

def upload_journal(journal, ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None):
    path_to_source = FILE_SYSTEM_PATH + "source_data/" + journal.lower()
    path_to_temp = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP + "/" + journal + "/"
    tarGzipArchivator(path_to_source, path_to_temp)
    id_user = tempJournal(journal)
    return id_user

def upload_article(upload_dir, filepath, id_user, ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None):
    print("upload_article")
    print("filepath", filepath, "id_user", id_user)
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    journal = None
    journal_res = postgres.select(table='user_isand', 
                            columns=['org_name'], 
                            where_keys=['id_user'],
                            where_values=[id_user]) 
    if journal_res: journal = journal_res[0][0]
    else: return None
    path_to_source = FILE_SYSTEM_PATH + "source_data/" + journal.lower()
    path_to_temp = upload_dir
    tarGzipArchivator(path_to_source, path_to_temp)

    for directory in listdir(upload_dir):
        pdf_files = [file for file in listdir(join(upload_dir)) if file.endswith('.pdf')]
        if len(pdf_files) == 0: continue
        publ = f"'{join(upload_dir, pdf_files[0])}'"
        print("publ", publ) 
        if publ:
            grobid_flag = False
            pdf_path = str(publ)
            foldername = upload_dir
            print("foldername",foldername)
            crcod_dict = crcod(pdf_path) 
            grobidAnalysis(foldername)
            p_title, p_title_add, p_text, p_text_add, authors, res_dupl, deltas, creation_date = '', '', '', '', '', '', '', ''
            filename_without_extension, _ = splitext(pdf_path.replace("'", ""))
            if exists(f'{filename_without_extension}.grobid.tei.xml'):
                print("pass here")
                grobid_flag = True
                creation_date, p_title, p_title_add, p_text, p_text_add, authors = grobidParse(f'{filename_without_extension}.grobid.tei.xml')
                deltas = get_deltas(f'{filename_without_extension}.segmentated.json', ru_lemmas, en_lemmas, ru_lemmas_words)
                res_dupl = duplicate_test(p_title, p_title_add, p_text, p_text_add, authors, deltas, postgres)
                getTextPdf(pdf_path)
            insertMove2(id_user, pdf_path, publ[0], p_title, p_title_add, p_text, p_text_add, authors, res_dupl, postgres, grobid_flag, crcod_dict, deltas, creation_date)
    return "cool"

def upload_articles(upload_dir, filepath, id_user, ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None):
    print("upload_article")
    print("filepath", filepath, "id_user", id_user)
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    journal = None
    journal_res = postgres.select(table='user_isand', 
                            columns=['org_name'], 
                            where_keys=['id_user'],
                            where_values=[id_user]) 
    if journal_res: journal = journal_res[0][0]
    else: return None
    path_to_source = FILE_SYSTEM_PATH + "source_data/" + journal.lower()
    path_to_temp = upload_dir
    tarGzipArchivator(path_to_source, path_to_temp)

    for directory in listdir(upload_dir):
        pdf_files = [file for file in listdir(join(upload_dir)) if file.endswith('.pdf')]
        if len(pdf_files) == 0: continue
        publ = f"'{join(upload_dir, pdf_files[0])}'"
        print("publ", publ) 
        if publ:
            grobid_flag = False
            pdf_path = str(publ)
            foldername = upload_dir
            print("foldername",foldername)
            crcod_dict = crcod(pdf_path) 
            grobidAnalysis(foldername)
            p_title, p_title_add, p_text, p_text_add, authors, res_dupl, deltas, creation_date = '', '', '', '', '', '', '', ''
            filename_without_extension, _ = splitext(pdf_path.replace("'", ""))
            if exists(f'{filename_without_extension}.grobid.tei.xml'):
                print("pass here")
                grobid_flag = True
                creation_date, p_title, p_title_add, p_text, p_text_add, authors = grobidParse(f'{filename_without_extension}.grobid.tei.xml')
                deltas = get_deltas(f'{filename_without_extension}.segmentated.json', ru_lemmas, en_lemmas, ru_lemmas_words)
                res_dupl = duplicate_test(p_title, p_title_add, p_text, p_text_add, authors, deltas, postgres)
                getTextPdf(pdf_path)
            insertMove2(id_user, pdf_path, publ[0], p_title, p_title_add, p_text, p_text_add, authors, res_dupl, postgres, grobid_flag, crcod_dict, deltas, creation_date)
    return "cool"

async def update(ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None):
    getNewPubl(URL_PUBLICATIONS, CODE_WORD) 
    dif = getDif(SITE_INFO_PATH + 'info.txt', SITE_INFO_PATH + 'newinfo.txt')  #[[id, url], [id, url]]
    id_prnd_path = downloadAllPdf(dif)
    tarGzipArchivator()
    tempAnalysis(id_prnd_path, ru_lemmas, ru_lemmas_words, en_lemmas)
    update_info() #Запись в info.txt с первой строки
    #clear_temp() #Очистка /var/storages/data/workgroup/temp
    return None

if __name__ == '__main__':
    print("In da update")
    #check_consistenty() записывает старые id из newinfo в info
    asyncio.run(update())
    # print(grobidParse('/var/storages/data/publications/prnd/00/00/00/00/00/00/00/0a/1499-1095.grobid.tei.xml'))
    pass