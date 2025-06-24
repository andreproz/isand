from os import path as osPath, walk, remove, makedirs
from shutil import copy2, move
from json import loads
#from sql.connection import *

def deltas_compare(json1, json2):
    if all(key in json2 for key in json1) or all(key in json1 for key in json2):
        sum1 = sum(json1.values())
        sum2 = sum(json2.values())
        if sum1 >= 20 and sum2 >= 20:
            diff_percent = abs(sum1 - sum2) / max(sum1, sum2) * 100
            return diff_percent <= 5
        else:
            return sum1 == sum2
    else:
        return False


#Функция перебора файлов в каталоге
'''
def temp_pdf_files(directory, exclude_dir='archive', postgres):
    for root, dirs, files in walk(directory):
        dirs[:] = [d for d in dirs if d != exclude_dir]
        for file in files:
            deltas, p_title, p_text, authors = None, None, None, None
            if file.lower().endswith('.pdf'):
                pdf_file_path = osPath.join(root, file)
                last_pdf_subdirectory = osPath.basename(root)
                crcod(pdf_file_path)
                grobidAnalysis(pdf_file_path)
                if osPath.exists(f'{pdf_file_path[:-4]}.grobid.tei.xml'):
                    p_title, p_text, authors = grobidParse(f'{pdf_file_path[:-4]}.grobid.tei.xml')
                if osPath.exists(f'{pdf_file_path[:-4]}.segmentated.json'):    
                    deltas = Deltas(f'{pdf_file_path[:-4]}.segmentated.json')
                if deltas or p_title:
                    dupl_res = duplicate_test(p_title, p_text, authors, deltas)
                    if not dupl_res:
                        # postgres.insert(table='USER_ISAND', columns=['org_name'], values=['mlsd'])
                        id_publ = postgres.insert(table='PUBLICATION', columns=['file_name_pdf', 'id_user'], values=[file, 4], returning=['id_publ'])
                        path = createDir(int(id_publ), FILE_SYSTEM_PATH + FILE_SYSTEM_PUBLICATION)
                        move_files([pdf_file_path, f'{pdf_file_path[:-4]}.grobid.tei.xml', f'{pdf_file_path[:-4]}.segmentated.json', f'{pdf_file_path[:-4]}.deltas.json', f'{pdf_file_path[:-4]}.crcod.json'], path)
                        postgres.update(table='PUBLICATION', columns=['file_name_grobid'], values=[f'{file[:-4]}.grobid.tei.xml'], where=f' WHERE id_publ = \'{id_publ}\'')
                        postgres.update(table='PUBLICATION', columns=['file_name_segmentated'], values=[f'{file[:-4]}.segmentated.json'], where=f' WHERE id_publ = \'{id_publ}\'')
                        postgres.update(table='PUBLICATION', columns=['file_name_deltas'], values=[f'{file[:-4]}.deltas.json'], where=f' WHERE id_publ = \'{id_publ}\'')
                        postgres.update(table='PUBLICATION', columns=['file_name_crcod'], values=[f'{file[:-4]}.crcod.json'], where=f' WHERE id_publ = \'{id_publ}\'')
                        postgres.update(table='PUBLICATION', columns=['path'], values=[path], where=f' WHERE id_publ = \'{id_publ}\'')
                        if p_title:
                            postgres.update(table='PUBLICATION', columns=['p_title'], values=[p_title], where=f' WHERE path = \'{path}\'', returning=['id_publ'])
                        if p_text:
                            postgres.insert(table='PUBL_TEXT', columns=['id_publ', 'p_text'], values=[id_publ, p_text])
                        for author in authors:
                            a_fio = author['a_fio']
                            a_last_name = author['a_last_name']
                            a_first_name = author['a_first_name']
                            a_sec_name = author['a_sec_name']
                            if a_fio:
                                id_author = postgres.insert(table='AUTHOR', columns=['a_fio', 'a_last_name', 'a_first_name', 'a_sec_name'], values=[a_fio, a_last_name, a_first_name, a_sec_name], returning=['id_author'])
                                postgres.insert(table='PUBL_AUTHOR', columns=['id_publ', 'id_author'], values=[id_publ, id_author])
                        postgres.insert(table='DELTAS', columns=['id_publ', 'deltas'], values=[id_publ, Json(deltas)])
                    elif dupl_res[0] == 'duplicate':
                        id_publ = dupl_res[1]  
                        id_dupl = postgres.insert(table='DUPLICATE', columns=['file_name_pdf', 'id_publ'], values=[file, id_publ], returning=['id_dupl'])
                        path = createDir(int(id_dupl), FILE_SYSTEM_PATH + FILE_SYSTEM_DUPLICATE)
                        move_files([pdf_file_path, f'{pdf_file_path[:-4]}.grobid.tei.xml', f'{pdf_file_path[:-4]}.segmentated.json', f'{pdf_file_path[:-4]}.deltas.json', f'{pdf_file_path[:-4]}.crcod.json'], path)
                        postgres.update(table='DUPLICATE', columns=['file_name_grobid'], values=[f'{file[:-4]}.grobid.tei.xml'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['file_name_segmentated'], values=[f'{file[:-4]}.segmentated.json'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['file_name_deltas'], values=[f'{file[:-4]}.deltas.json'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['path'], values=[path], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['reason'], values=['duplicate'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                    elif dupl_res[0] == 'suspect':
                        id_publ = dupl_res[1]  
                        id_dupl = postgres.insert(table='DUPLICATE', columns=['file_name_pdf', 'id_publ'], values=[file, id_publ], returning=['id_dupl'])
                        path = createDir(int(id_dupl), FILE_SYSTEM_PATH + FILE_SYSTEM_DUPLICATE)
                        move_files([pdf_file_path, f'{pdf_file_path[:-4]}.grobid.tei.xml', f'{pdf_file_path[:-4]}.segmentated.json', f'{pdf_file_path[:-4]}.deltas.json', f'{pdf_file_path[:-4]}.crcod.json'], path)
                        postgres.update(table='DUPLICATE', columns=['file_name_grobid'], values=[f'{file[:-4]}.grobid.tei.xml'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['file_name_segmentated'], values=[f'{file[:-4]}.segmentated.json'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['file_name_deltas'], values=[f'{file[:-4]}.deltas.json'], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['path'], values=[path], where=f' WHERE id_dupl = \'{id_dupl}\'')
                        postgres.update(table='DUPLICATE', columns=['reason'], values=['suspect'], where=f' WHERE id_dupl = \'{id_dupl}\'')
'''
def get_ids(p_title, p_title_add, deltas, postgres):
    id_publ_titles = []
    id_publ_deltas = []
    if p_title:
        #получаем список id публикаций
        id_publ_titles = postgres.select(table='PUBLICATION2', columns=['id_publ'], 
                                        where_keys=['p_title'],
                                        where_values=[p_title])
        id_publ_titles += postgres.select(table='PUBLICATION2', columns=['id_publ'], 
                                        where_keys=['p_title_add'],
                                        where_values=[p_title])
    if p_title_add:
        id_publ_titles += postgres.select(table='PUBLICATION2', columns=['id_publ'], 
                                        where_keys=['p_title'],
                                        where_values=[p_title_add])
        id_publ_titles += postgres.select(table='PUBLICATION2', columns=['id_publ'], 
                                        where_keys=['p_title_add'],
                                        where_values=[p_title_add])
    if deltas:
        with postgres.conn.cursor() as cursor:
            cursor.execute(f"SELECT id_publ, deltas FROM DELTAS;")
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                id_publ, deltas_publ = row[0], loads(row[1])
                if deltas_compare(deltas_publ, deltas):
                    id_publ_deltas += [(id_publ, )]
    id_publ = list(set(id_publ_titles + id_publ_deltas))
    return id_publ

def get_fio(pub_ids, postgres):
    authors_fio = postgres.select(table='AUTHOR', 
                                 columns=['id_publ', 'a_fio'], 
                                 where_keys=['id_publ' for _ in pub_ids], 
                                 where_values=pub_ids)
    result_dict = {}
    for key, value in authors_fio:
        if key in result_dict:
            result_dict[key].append(value)
        else:
            result_dict[key] = [value]
    return result_dict


def duplicate_test(p_title, p_title_add, p_text, p_text_add, authors, deltas, postgres):
    block_pass = [False, False, False]
    suspect_ids = get_ids(p_title, p_title_add, deltas, postgres)
    for i in range(len(suspect_ids)): suspect_ids[i] = suspect_ids[i][0] 

    #Проверка на первый блок
    if len(suspect_ids) == 0: return ''          
    block_pass[0] = True

    #Проверка на второй блок
    fio_dict = get_fio(suspect_ids, postgres)
    fio_suspect_ids = []
    if authors:
        #print(fio_dict)
        for ids, fio in fio_dict.items():
            if set(fio) == set(authors): fio_suspect_ids += [ids]
        if len(fio_suspect_ids) > 0: block_pass[1] = True 
        # TODO: проверить на УДК

    nfio_suspect_ids = [x for x in suspect_ids if x not in fio_suspect_ids]
    
    #Проверка на третий блок
    output_result = []
    for sus_ids in fio_suspect_ids:
        r_publ_text = postgres.select(table='PUBL_TEXT', 
                                    columns=['p_text'], 
                                    where_keys = ['id_publ'], 
                                    where_values = [sus_ids])
        r_publ_text_add = postgres.select(table='PUBL_TEXT', 
                                    columns=['p_text_add'], 
                                    where_keys = ['id_publ'], 
                                    where_values = [sus_ids])
        publ_text, publ_text_add = r_publ_text[0][0], r_publ_text_add[0][0]
        if publ_text == p_text or publ_text_add == p_text or publ_text == p_text_add or publ_text_add == p_text_add: 
            output_result.append(['duplicate', sus_ids])
        block_pass[2] = True
    for sus_ids in nfio_suspect_ids:
        r_publ_text = postgres.select(table='PUBL_TEXT', 
                                    columns=['p_text'], 
                                    where_keys = ['id_publ'], 
                                    where_values = [sus_ids])
        r_publ_text_add = postgres.select(table='PUBL_TEXT', 
                                    columns=['p_text'], 
                                    where_keys = ['id_publ'], 
                                    where_values = [sus_ids])
        publ_text, publ_text_add = r_publ_text[0][0], r_publ_text_add[0][0]
        if publ_text == p_text or publ_text_add == p_text or publ_text == p_text_add or publ_text_add == p_text_add: 
            output_result.append(['duplicate', sus_ids])
        block_pass[2] = True
    if block_pass[2] == False and block_pass[0] == True or block_pass[1] == True:
        output_result.append(['suspect', suspect_ids[0]])
    if len(output_result) == 0: 
        print('')
        return ''
    else: 
        print(output_result[0])
        return output_result[0]

    '''
    if p_title:
        id_publ_title = postgres.select(table='PUBLICATION', 
                                        columns=['id_publ'], 
                                        where='p_title = %s', 
                                        params=(p_title,))                  
    if deltas:
        with postgres.conn.cursor() as cursor:
            cursor.execute(f"SELECT id_publ, deltas FROM DELTAS;")
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                id_publ, deltas_publ = row[0], loads(row[1])
                if deltas_compare(deltas_publ, deltas):
                    id_publ_deltas += [id_publ]    
    if id_publ_title or id_publ_deltas:
        if authors:
            for author in authors:
                if id_publ_title:
                    id_author = postgres.select(table='PUBL_AUTHOR', columns=['id_author'], where='id_publ = %s', params=(id_publ_title,))
                    author_fio = postgres.select(table='AUTHOR', columns=['a_fio'], where='id_author = %s', params=(id_author,))
                    if author['a_fio'] == author_fio:
                        publ_text = postgres.select(table='PUBL_TEXT', columns=['p_text'], where='id_publ = %s', params=(id_publ_title,))
                        if publ_text == p_text:
                            return ['duplicate', id_publ_title]
                        else:
                            return ['suspect', id_publ_title]
                    else:
                        continue
                elif id_publ_deltas:
                    for id_publ in id_publ_deltas:
                        id_author = postgres.select(table='PUBL_AUTHOR', columns=['id_author'], where='id_publ = %s', params=(id_publ,))
                        author_fio = postgres.select(table='AUTHOR', columns=['a_fio'], where='id_author = %s', params=(id_author,))
                        if author['a_fio'] == author_fio:
                            publ_text = postgres.select(table='PUBL_TEXT', columns=['publ_text'], where='id_publ = %s', params=(id_publ,))
                            if publ_text == p_text:
                                return ['duplicate', id_publ_deltas[0]]
                            else:
                                return ['suspect', id_publ_deltas[0]]
                        else:
                            continue
            else:
                return ['duplicate', id_publ_title] if id_publ_title else  ['duplicate', id_publ_deltas[0]]
        else:
            if p_text:
                publ_text = postgres.select(table='PUBL_TEXT', columns=['p_text'], where='id_publ = %s', params=(id_publ_title,))
                if publ_text == p_text:
                    return ['duplicate', id_publ_title] if id_publ_title else  ['duplicate', id_publ_deltas[0]]
                else:
                    return ['duplicate', id_publ_title] if id_publ_title else  ['duplicate', id_publ_deltas[0]]
            else:
                return ['duplicate', id_publ_title] if id_publ_title else  ['duplicate', id_publ_deltas[0]]
    else: 
        return ''
    '''
    
if __name__ == '__main__':
    teixml_path = '/var/storages/00/00/00/00/00/00/00/01/548-853.grobid.tei.xml'
    segmented_path = '/var/storages/00/00/00/00/00/00/00/01/548-853.segmentated.json'
    p_title, p_text, authors = grobidParse(teixml_path)
    deltas = Deltas(segmented_path)
    print("p_title", p_title)
    # temp_pdf_files('/var/storages/data/workgroup/temp/VSPU')