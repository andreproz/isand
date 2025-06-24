import glob
import json
import re
import sys
from os import makedirs, walk, remove, listdir, rename
from os.path import abspath, basename, dirname, exists, join, isdir, isfile, normpath, relpath, sep, splitext
from fitz import open as fitzOpen
from shutil import copy, move, rmtree, unpack_archive

sys.path.append(dirname(dirname(abspath(__file__))))

from config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP

#from .config import CODE_WORD, URL_PUBLICATIONS, URL_PDF_PUBLICATION, FILE_SYSTEM_PATH, FILE_SYSTEM_SOURCE_DATA_PRND, SITE_INFO_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP



DEEP_SIGN_CONST = 16

def getTextPdf(path):
    filename_without_quotes = path.replace("'", "")
    filename_without_quotes = path.replace('"', "")
    filename_without_extension, _ = splitext(filename_without_quotes)
    with fitzOpen(f'{filename_without_quotes}') as doc, open(f'{filename_without_extension}.text.txt', 'w') as file:  # open document
        text = chr(12).join([page.get_text() for page in doc])
        file.write(text)
    
def getUniversalFormat(filepath, grobid_response):
    article_path_without_format, _ = splitext(filepath.replace('"', ""))
    with open(f'{article_path_without_format}.segmentated.json', "w", encoding="utf-8") as json_file:
        json.dump(grobid_response, json_file, ensure_ascii=False, indent=2)

def getSource(filepath):
    print(f"filecore::getSource = {filepath}")
    dirs = normpath(filepath).split(sep) 
    required_dir = None
    print("dirs", dirs, "|")
    if len(dirs) > 5: required_dir = dirs[5]
    else: required_dir = dirs[-2]
    return required_dir

def organize_pdfs(base_dir):
    for root, dirs, files in walk(base_dir):
        # Пропускаем каталог 'archive' и все его подкаталоги
        if 'archive' in root:
            continue

        for file in files:
            if file.endswith('.pdf'):
                # Получаем полный путь файла
                file_path = join(root, file)
                # Создаем название нового каталога без расширения файла
                new_dir_name = splitext(file)[0]
                new_dir_path = join(root, new_dir_name)

                # Создаем новый каталог, если он еще не существует
                if not exists(new_dir_path):
                    makedirs(new_dir_path)

                # Перемещаем файл PDF в новый каталог
                new_file_path = join(new_dir_path, file)
                move(file_path, new_file_path)

def copy_journals_with_direct_structure(source_dir, target_dir):
    """
    Копирует журналы из исходного каталога в целевой, изменяя структуру каталогов для PDF файлов,
    так чтобы каждый PDF файл находился в своей подпапке непосредственно в каталоге журнала.
    
    :param source_dir: Путь к исходному каталогу с журналами.
    :param target_dir: Путь к целевому каталогу для копирования.
    """
    for root, dirs, files in walk(source_dir):
        # Определяем название журнала как первую часть относительного пути к файлу
        journal_name = relpath(root, source_dir).split(sep)[0]
        target_journal_dir = join(target_dir, journal_name)
        
        for file in files:
            if file.endswith('.pdf'):
                # Создаем подпапку для каждого PDF файла непосредственно в каталоге журнала
                file_folder_name = file[:-4] # Убираем расширение .pdf для названия папки
                target_file_dir = join(target_journal_dir, file_folder_name)
                if not exists(target_file_dir):
                    makedirs(target_file_dir)
                
                source_file_path = join(root, file)
                target_file_path = join(target_file_dir, file)
                
                # Копирование файла
                copy(source_file_path, target_file_path)

def createDir(id_publ):
    id_publ = hex(id_publ)[2:]
    path = ''
    for i in range(len(id_publ), DEEP_SIGN_CONST):
        id_publ = '0' + id_publ
    for i in range(DEEP_SIGN_CONST // 2):
        path += id_publ[:2] + '/'
        id_publ = id_publ[2:]
    path = path[:-1]
    makedirs(FILE_SYSTEM_PATH + f'publications/papers/' + path, exist_ok=True)
    print("new dir: ", (FILE_SYSTEM_PATH + f'publications/papers/' + path), "created")
    return FILE_SYSTEM_PATH + f'publications/papers/' + path

def getDirByID(id_publ):
    id_publ = hex(id_publ)[2:]
    path = ''
    for i in range(len(id_publ), DEEP_SIGN_CONST):
        id_publ = '0' + id_publ
    for i in range(DEEP_SIGN_CONST // 2):
        path += id_publ[:2] + '/'
        id_publ = id_publ[2:]
    path = path[:-1]
    filepath = FILE_SYSTEM_PATH + f'publications/papers/' + path
    print("filepath", filepath)
    if isdir(filepath):
        return filepath
    else:
        return None

def move_files(src_dir, dst_dir):
    for filename in listdir(src_dir):
        src_file = join(src_dir, filename)
        dst_file = join(dst_dir, filename)
        copy(src_file, dst_file)
    return None

def clear_temp():
    PATH = FILE_SYSTEM_PATH + FILE_SYSTEM_WORKGROUP + WORKGROUP_TEMP
    NOT_DELETE = []
    for folder_name in listdir(PATH):
        folder = join(PATH, folder_name)
        if isdir(folder) and folder_name not in NOT_DELETE:
            rmtree(folder)
    
def get_segmentated_json(filepath):
    pattern = '*.segmentated.json'
    # Находим файл по шаблону
    files = glob.glob(f'{filepath}/{pattern}')

    # Если файл найден
    if files:
        # Открываем файл и читаем его содержимое
        with open(files[0], 'r') as f:
            content = f.read()

        # Распарсиваем содержимое в словарь
        data = json.loads(content)

        # Возвращаем словарь
        return data
    else:
        # Если файл не найден, возвращаем None
        return None

def get_text(filepath):
    # Шаблон для поиска файла
    pattern = '*.text.txt'

    # Находим файл по шаблону
    files = glob.glob(f'{filepath}/{pattern}')

    # Если файл найден
    if files:
        # Открываем файл и читаем его содержимое
        with open(files[0], 'r') as f:
            content = f.read()

        # Возвращаем содержимое файла
        return content
    elif exists(f'{filepath}/{pattern}'):
        # Если файл существует, но не найден функцией glob
        with open(f'{filepath}/{pattern}', 'r') as f:
            content = f.read()
        return content
    else:
        # Если файл не найден, возвращаем None
        return None
    
if __name__ == '__main__':
    '''
    new_pub_id = [33355, 33356, 33357, 33358, 33359, 33360, 33361, 33362, 33363, 33364, 33365, 
    33366, 33367, 33368, 33369, 33370, 33371, 33372, 33373, 33374, 33375, 33376, 33377, 33378, 
    33379, 33380, 33381, 33382, 33383, 33384, 33385, 33386, 33387, 33388, 33389, 33390, 33391, 
    33392, 33393, 33394, 33395, 33396, 33397, 33398, 33399, 33400, 33401, 33402, 33403, 33404, 
    33405, 33406, 33407, 33408, 33409, 33410, 33411, 33412, 33413, 33414, 33415, 33416, 33417, 
    33418, 33419, 33420, 33421, 33422, 33423, 33424, 33425, 33426, 33427, 33428, 33429, 33430, 
    33431, 33432, 33433, 33434, 33435, 33436, 33437, 33438, 33439, 33440, 33441, 33442, 33443, 
    33444, 33445, 33446, 33447, 33448, 33449, 33450, 33451, 33452, 33453, 33454, 33455, 33456, 
    33457, 33458, 33459, 33460, 33461, 33462, 33463, 33464, 33465, 33466, 33467, 33468, 33469, 
    33470, 33471, 33472, 33473, 33474, 33475, 33476, 33477, 33478, 33479, 33480, 33481, 33482, 
    33483, 33484, 33485, 33486, 33487, 33488, 33489, 33490, 33491, 33492, 33493, 33494, 33495, 
    33496, 33497, 33498, 33499, 33500, 33501, 33502, 33503, 33504, 33505, 33506, 33507, 33508, 
    33509, 33510, 33511, 33512, 33513, 33514, 33515, 33516, 33517, 33518, 33519, 33520]

    dirpath = "/var/storages/data/workgroup/temp/ICCT_2017"
    dirs = []
    for directory in listdir(dirpath):
        print("\n\ndirectory", directory) 
        #dirs.append(f'"{join(dirpath, directory)}"')
        dirs.append(join(dirpath, directory))

    for i in range(len(new_pub_id)):
        pub_id = new_pub_id[i]
        foldername = dirs[i] 
        print("pub_id", pub_id, "foldername", foldername)
        new_path = createDir(pub_id)
        print("new_path", new_path)
        move_files(foldername, new_path)
    '''
    #clear_temp()
    filepath = getDirByID(1)
    print("getDirByID(1)", filepath)
    if filepath: json_dict = get_segmentated_json(filepath)
    #print("json_dict", json_dict)
    #if filepath: print("json_dict[publications][publication]", json_dict['publications'][0]['publication']['p_text'])
    #content = get_text(filepath)
    #print("content", content)