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
from psycopg2 import connect as psycopg2Connect

'''
from sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from sql.basawork import upload2DB
from python.filecore import createDir, move_files
from python.get_grobid import operate_grobid

'''
#'''
from .sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from .sql.basawork import upload2DB
from .python.get_grobid import operate_grobid
#'''

def extract_features(filepath):
    print("extract_features", filepath)
    parametres = dict()
    
    with open(filepath, 'r') as file:
        data = json.load(file)

    publications = data.get('publications', [])

    for publication in publications:
        publication_data = publication.get('publication', {})
        parametres['title'] = publication_data.get('p_title', '')
        parametres['creation_date'] = data.get('creation_date', '')
        parametres['doi'] = publication_data.get('doi', '')

        authors = []
        references = publication_data.get('references_by', [])
        for reference in references:
            reference_data = reference.get('reference', {})
            authors_data = reference_data.get('r_authors', [])
            for author_data in authors_data:
                author_fio = author_data.get('r_author', {}).get('r_a_fio', '')
                author_last = author_data.get('r_author', {}).get('r_a_last_name', '')
                author_first = author_data.get('r_author', {}).get('r_a_first_name', '')
                author_sec = author_data.get('r_author', {}).get('r_a_sec_name', '')
                author = (author_fio, author_last, author_first, author_sec)
                authors.append(author)

        parametres['authors'] = authors

    return parametres


async def upload(dirpath, scenario, conference_name = "", journal_name = "", year_publication = None):
    print("In upload", dirpath)

    filepath = None
    ext_source = "" 
    new_pub_id = []

    if len(conference_name) > len(journal_name): ext_source = conference_name
    else: ext_source = journal_name 

    if scenario == 1: #upload journal
        for directory in listdir(dirpath):
            print("\n\ndirectory", directory) 
            pdf_files = [file for file in listdir(join(dirpath, directory)) if file.endswith('.pdf')]
            #print("pdf_files", pdf_files)
            if len(pdf_files) == 0: 
                print("len(pdf_files)", len(pdf_files))
                continue
            publ = f"{join(dirpath, directory, pdf_files[0])}"
            grobid_parametres = dict()
            pdf_path = None
            publ_dir = join(dirpath, directory)   
            if publ:
                pdf_path = str(publ)
                grobid_parametres = operate_grobid(pdf_path)
                if not grobid_parametres: continue
                print("\nparametres", grobid_parametres)  
            else: continue
            #is_article_good = False    
            article_parametres = dict() 

            article_parametres["grobid_authors"] = grobid_parametres.get('authors', None)
            article_parametres["grobid_title"] = grobid_parametres.get('p_title', None)
            
            article_parametres["creation_date"] = grobid_parametres.get('creation_date', None)

            if year_publication: article_parametres["publication_date"] = year_publication
            if conference_name: article_parametres["conference_name"] = conference_name
            if journal_name: article_parametres["journal_name"] = journal_name
            
            pub_id = upload2DB(article_parametres)
            if pub_id: 
                print("pub_id", pub_id)
                publ_newdir = createDir(pub_id)
                move_files(publ_dir, publ_newdir)
                new_pub_id.append(pub_id)
                
    elif scenario == 3: #user upload
        for file in listdir(dirpath):
            if file.endswith('.user.json'):
                filepath = join(dirpath, file)
                parametres = extract_features(filepath) 
                pub_id = upload2DB(parametres, is_article_good = True)
                return pub_id

    print("new_pub_id", new_pub_id)
    return new_pub_id

if __name__ == '__main__':
    print("In da upload")
    '''
    ready:
    "ubs" : ['conference', [
                ["UBS_2011", 2011], ["UBS_2013", 2013], ["UBS_2017", 2017], ["UBS_2019", 2019], ["UBS_2021", 2021], ["UBS_2022", 2022]
            ]],
    "socphys" : ['conference', [["SocPhys2015", 2015], ["SocPhys2018", 2018]]],
    "stab" : ['conference', [
            ["STAB_2016", 2016], ["STAB_2018", 2018], ["STAB_2020", 2020], ["STAB_2022", 2022]
            ]],
    '''

    '''
    process:
    "ait" : ['journal', [
                ["AiT1966", 1966], ["AiT1967", 1967], ["AiT1968", 1968], ["AiT1969", 1969], ["AiT1970", 1970], ["AiT1971", 1971], ["AiT1972", 1972], ["AiT1973", 1973],
                ["AiT1974", 1974], ["AiT1975", 1975], ["AiT1976", 1976], ["AiT1977", 1977], ["AiT1978", 1978], ["AiT1979", 1979], ["AiT1980", 1980], ["AiT1981", 1981],
                ["AiT1982", 1982], ["AiT1983", 1983], ["AiT1984", 1984], ["AiT1985", 1985], ["AiT1986", 1986], ["AiT1987", 1987], ["AiT1988", 1988], ["AiT1989", 1989],
                ["AiT1990", 1990], ["AiT1991", 1991], ["AiT1992", 1992], ["AiT1993", 1993], ["AiT1994", 1994], ["AiT1995", 1995], ["AiT1996", 1996], ["AiT1997", 1997],
                ["AiT1998", 1998], ["AiT1999", 1999], ["AiT2000", 2000], ["AiT2001", 2001], ["AiT2002", 2002], ["AiT2003", 2003], ["AiT2004", 2004], ["AiT2005", 2005],
                ["AiT2006", 2006], ["AiT2007", 2007], ["AiT2008", 2008], ["AiT2009", 2009], ["AiT2010", 2010], ["AiT2011", 2011], ["AiT2012", 2012], ["AiT2013", 2013],
                ["AiT2014", 2014], ["AiT2015", 2015], ["AiT2016", 2016], ["AiT2017", 2017], ["AiT2018", 2018], ["AiT2019", 2019], ["AiT2020", 2020]
            ]] 

    '''
    
    '''
    "avtprom" : ['journal', [
                    ["Avtprom2018", 2018], ["Avtprom2019", 2019], ["Avtprom2020_1", 2020], ["Avtprom2020_2", 2020], ["Avtprom2021", 2021], ["Avtprom2022", 2022],
                 ["Avtprom2023", 2023]
                ]],
    "mlsd" : ['conference', [
                ["MLSD_2007", 2007], ["MLSD_2009", 2009], ["MLSD_2011", 2011], ["MLSD_2012", 2012], ["MLSD_2013", 2009], ["MLSD_2014", 2014], ["MLSD_2015", 2015], 
                ["MLSD_2016", 2016], ["MLSD_2017", 2017], ["MLSD_2018", 2018], ["MLSD_2019", 2019], ["MLSD_2020", 2020], ["MLSD_2021", 2021], ["MLSD_2022", 2022]
             ]],
    
    '''
    
    dir_upload = {  
        "avtprom" : ['journal', [
                    ["Avtprom2018", 2018], ["Avtprom2019", 2019], ["Avtprom2020_1", 2020], ["Avtprom2020_2", 2020], ["Avtprom2021", 2021], ["Avtprom2022", 2022],
                 ["Avtprom2023", 2023]
                ]],
        "mlsd" : ['conference', [
                    ["MLSD_2007", 2007], ["MLSD_2009", 2009], ["MLSD_2011", 2011], ["MLSD_2012", 2012], ["MLSD_2013", 2009], ["MLSD_2014", 2014], ["MLSD_2015", 2015], 
                    ["MLSD_2016", 2016], ["MLSD_2017", 2017], ["MLSD_2018", 2018], ["MLSD_2019", 2019], ["MLSD_2020", 2020], ["MLSD_2021", 2021], ["MLSD_2022", 2022]
                ]],
        "pubss" : ['conference', [
                    ["PUBS2018", 2018], ["PUBS2019", 2019], ["PUBS2020", 2020], ["PUBS2021", 2021], ["PUBS2022", 2022], ["PUBS2023", 2023],
                    ["PUBSS_TESIS", 2023]
                ]]    
    }          

    basedirpath = "/var/storages/data/workgroup/temp/"
    dirpath = ""
    full_pub_id = []

    for key in dir_upload:
        flag = dir_upload[key][0]
        publications = dir_upload[key][1]
        for d_u in publications:
            foldername = d_u[0]
            year = d_u[1]
            dirpath = join(basedirpath, foldername)
            print("key", key, "foldername", dirpath, "year", year)
            #conference_name = "", journal_name = ""
            if flag == 'conference':
                new_pub_id = asyncio.run(upload(dirpath, 1, conference_name = key, year_publication = year))  
            elif flag == 'journal':
                new_pub_id = asyncio.run(upload(dirpath, 1, journal_name = key, year_publication = year))  
            print("new_pub_id", new_pub_id)
            full_pub_id.append(new_pub_id)

    file_path = "full_pub_id.json"
    json_data = json.dumps(full_pub_id)
    with open(file_path, "w") as file:
        file.write(json_data)
    print("full_pub_id", full_pub_id) 
    