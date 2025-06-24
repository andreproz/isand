import json

from datetime import datetime
from os import makedirs, walk, remove, listdir, rename
from os.path import join, exists, dirname, basename, relpath,splitext, isdir
from psycopg2.extras import Json
from subprocess import run

#global launch
#'''
from .sql.basawork import upload2DB
from .sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from .sql.postgres import SQLQuery
from .python.grobid_parser import create_json_structure, TEIFile
from .python.config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, FILE_SYSTEM_PUBLICATION
from .python.deltas_old import get_deltas
from .python.crcod import crcod
from .python.duplicate import duplicate_test
from .python.filecore import getSource, getTextPdf 
#'''

'''
from sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from sql.postgres import SQLQuery
from python.grobid_parser import create_json_structure, TEIFile
from python.config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, FILE_SYSTEM_PUBLICATION
from python.deltas_old import get_deltas
from python.crcod import crcod
from python.duplicate import duplicate_test
from python.filecore import getSource, getTextPdf 
'''
    
def grobidAnalysis(path, path_to_grobid='/home/isand_user/isand/servers/grobid/grobid_client_python/grobid_client/grobid_client.py'):
    print("grobidAnalysis::path", path) 
    command = f'python3 {path_to_grobid} --n 3 --force --input {path} --include_raw_citations --include_raw_affiliations processFulltextDocument'
    run(command, shell=True)

def grobidParse(path, ext_source):
    tei_file = TEIFile(path)
    tei_file.parse()
    tei_file.data = create_json_structure(tei_file.data, ext_source)
    with open(f'{path[:-15]}.segmentated.json', 'w', encoding='utf-8') as json_file:
        json.dump(tei_file.data, json_file, ensure_ascii=False, indent=2)
    creation_date = tei_file.data['creation_date']
    pub = tei_file.data['publications'][0]['publication']
    '''
    print("pub['authors']")
    for i in pub['authors']:
        print("i['author']['a_fio']", i['author']['a_fio'])
    '''
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

def updateGrobid(filename, id_publ, creation_date, p_title, p_title_add, p_text, p_text_add, 
                 authors, deltas):
    #print("In updateGrobid")
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    
    creation_date = datetime.strptime(creation_date, '%d.%m.%Y %H:%M:%S')
    #print("creation_date", creation_date)
    res1 = postgres.update(table='PUBLICATION', 
                    columns=['filename_grobid', 'filename_segmentated', 'filename_deltas', 
                             'filename_text'], 
                    values=[f'{filename}.grobid.tei.xml', 
                            f'{filename}.segmentated.json', 
                            f'{filename}.deltas.json', f'{filename}.text.txt'], 
                    where=f' WHERE id_publ = \'{id_publ}\'')
    #print("res1", res1)
    res2 = postgres.update(table='PUBLICATION2', 
                    columns=['p_title', 'p_title_add', 'creation_date'], 
                    values=[p_title, p_title_add, creation_date], 
                    where=f' WHERE id_publ = \'{id_publ}\'')
    #print("res2", res2)
    res3 = postgres.update(table='PUBL_TEXT', 
                    columns=['p_text', 'p_text_add'], 
                    values=[p_text, p_text_add], 
                    where=f' WHERE id_publ = \'{id_publ}\'')
    #print("res3", res3)

    if authors:
        for a_fio in authors:
            res4 = postgres.update(table='AUTHOR', 
                            columns=['a_fio'], 
                            values=[a_fio],
                            where=f' WHERE id_publ = \'{id_publ}\'')
            #print("res4", res4)

    if deltas:
        res5 = postgres.update(table='DELTAS', 
                        columns=['deltas'], 
                        values=[Json(deltas)],
                        where=f' WHERE id_publ = \'{id_publ}\'')
        #print("res5", res5)
    return 0

def grobid2folder(foldername, ext_source = None, id_publ=None, ru_lemmas=None, ru_lemmas_words=None, en_lemmas=None):
    grobid_flag = False 
    print("\nGrobig in dir", f'{foldername}')
    pdf_files = [f for f in listdir(foldername) if f.endswith('.pdf')]
    print("pdf_files", pdf_files)
    if len(pdf_files) == 0:
        return "No pdf_files"
    pdf_name = str(pdf_files[0])
    print("pdf_name", pdf_name)    
    pdf_path = f'"{join(foldername, pdf_name)}"'
    dir_path = f'"{foldername}"'
    print("pdf_path", pdf_path)
    filename, _ = splitext(pdf_name)
    print("filename", filename) 
    crcod_dict = crcod(pdf_path)
    print("grobid filefolder", pdf_path) 
    grobidAnalysis(dir_path)
    p_title, p_title_add, p_text, p_text_add, authors, res_dupl, deltas, creation_date = '', '', '', '', '', '', '', ''
    article_path, _ = splitext(pdf_path.replace('"', ""))
    print("article_path", article_path)
    print(f'{article_path}.grobid.tei.xml', exists(f'{article_path}.grobid.tei.xml'))
    parametres = dict()
    if exists(f'{article_path}.grobid.tei.xml'):
        #print("grobid.tei.xml exists") 
        grobid_flag = True
        print(f"grobid2folder::dir_path = {dir_path}")
        ext_source = getSource(dir_path)
        creation_date, p_title, p_title_add, p_text, p_text_add, authors = grobidParse(f'{article_path}.grobid.tei.xml', ext_source)
        '''
        print("creation_date", creation_date)
        print("p_title", p_title)
        print("p_title_add", p_title_add)
        print("p_text", p_text)
        print("p_text_add", p_text_add)
        print("authors", authors)
        '''
        #deltas = get_deltas(f'{article_path}.segmentated.json', ru_lemmas, en_lemmas, ru_lemmas_words)
        getTextPdf(pdf_path)
    if grobid_flag:
        if id_publ:
            if id_publ == None:
                print("id_publ = None")
                return 0
            #updateGrobid(filename, id_publ, creation_date, p_title, p_title_add, p_text, p_text_add, authors, deltas)
        else:            
            parametres["authors"] = authors
            parametres["creation_date"] = creation_date
            parametres["crcod_dict"] = crcod_dict
            parametres["deltas"] = deltas
            parametres["ext_source"] = ext_source
            parametres["foldername"] = foldername
            parametres["filename"] = filename
            parametres["grobid_flag"] = grobid_flag
            parametres["id_publ"] = id_publ
            parametres["p_text"] = p_text
            parametres["p_text_add"] = p_text_add
            parametres["p_title"] = p_title
            parametres["p_title_add"] = p_title_add  
            
            #insert_update_publication(parametres)

    else: return None
    return parametres

def path_in_bd(path):
    postgres = SQLQuery(
        dbname=DBNAME,
        user=USER,
        host=HOST,
        port=PORT,
        password=PASSWORD
    )
    id_publ = postgres.select(table='PUBLICATION', 
                    columns=['id_publ'], 
                    where_keys=['path'],
                    where_values=[path])
    if len(id_publ) == 0: return None
    id_publ = id_publ[0][0]
    #print("id_publ", id_publ) 
    return id_publ

def recursive_walk(pubpath = FILE_SYSTEM_PATH + FILE_SYSTEM_PUBLICATION):
    #print("In recursive_walk")
    #worked: 'assa', 'cpes', 'cs', 'dccn', 'druker', 'econvest', 'icct', 'ifac_tecis', 'prnd', 'mlsd', 'pu', 'ubs'
    #now: 'vspu'
    excluded_dirs = ['assa', 'cpes', 'cs', 'dccn', 'druker', 'econvest', 'icct', 'ifac_tecis', 'prnd', 'mlsd', 'pu', 'ubs']
    iter = 0
    for root, dirs, files in walk(pubpath):
        print(f'Current directory: {root}')
        print(f'Current dirs: {dirs}')
        print(f'Current files: {files}')
        print('\n')

        if iter == 0:
            for d in excluded_dirs:
                dirs.remove(d)
        
        if len(files) > 0 and "journal_name.json" not in files: 
            id_publ = path_in_bd(root)
            if id_publ: grobid2folder(root, id_publ)
            else:
                print("This path not in BD.") 
                id_publ = None
                grobid2folder(root, id_publ)

        iter += 1
    return 0

def run_grobid_update():
    recursive_walk()

if __name__ == '__main__':
    print("In grobid update")
    #run_grobid_update()
    foldername = "/home/unreal_dodic/test_grobid/"
    grobid2folder(foldername)
    pass