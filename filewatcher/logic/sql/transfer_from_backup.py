'''
import os
import shutil
import psycopg2

# Путь к бекапу
backup_path = '/home/unreal_dodic/publications/'

# Путь к актуальным папкам
current_path = '/var/storages/data/publications/papers/'

# Список разрешенных папок
allowed_folders = ['assa', 'cpes', 'cs', 'dccn', 'druker', 'econvest', 'icct', 'ifac_tecis', 'mlsd', 'pu', 'ubs', 'vspu']

# Подключение к базе данных
conn_account = psycopg2.connect(
    dbname='account_db',
    user='isand',
    host='193.232.208.58',
    port='5432',
    password='sf3dvxQFWq@!'
)
cur = conn_account.cursor()

# Рекурсивный обход папок
def process_folder(folder_path, source_folder):
    #print("source_folder", source_folder)
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isdir(item_path):
            process_folder(item_path, source_folder)
        else:
            # Получаем 16-ричное имя папки из пути к текущему файлу
            path_parts = os.path.normpath(folder_path).split(os.sep)
            hex_folder = path_parts[-8:]
            hex_folder = '/'.join(hex_folder)
            #print("hex_folder", hex_folder, "hex_folder.replace('/', '').isalnum()", hex_folder.replace('/', '').isalnum(), "len(hex_folder.replace('/', ''))", len(hex_folder.replace('/', '')))
            if not hex_folder.replace('/', '').isalnum() or len(hex_folder.replace('/', '')) != 16:
                continue
            publication_id = int(hex_folder.replace('/', ''), 16)
            #print("publication_id", publication_id)
            # Проверяем, существует ли запись с этим id в таблице publications
            cur.execute("SELECT id FROM publications WHERE id = %s", (publication_id,))
            if not cur.fetchone():
                continue
            # Проверяем, существует ли запись с этим publication_source_id в таблице publication_sources
            cur.execute("SELECT id FROM publication_sources WHERE id = (SELECT publication_source_id FROM publications WHERE id = %s)", (publication_id,))
            source_id = cur.fetchone()
            #print("source_id", source_id)
            if source_id:
                source_id = source_id[0]
                # Проверяем, существует ли непустая папка с таким же 16-ричным именем в актуальных папках
                dest_path = os.path.join(current_path, hex_folder)
                if os.path.isdir(dest_path) and os.listdir(dest_path):
                    continue
            else:
                # Находим id журнала или конференции по full_name из папки бекапа
                cur.execute("SELECT id FROM journals WHERE name = %s", (source_folder,))
                journal_id = cur.fetchone()
                cur.execute("SELECT id FROM conferences WHERE name = %s", (source_folder,))
                conference_id = cur.fetchone()
                print("source_folder", source_folder, "journal_id", journal_id, "conference_id", conference_id)

                if journal_id and conference_id:
                    # Если есть в обоих, то заполняем journal_id и conference_id
                    cur.execute("INSERT INTO publication_sources (journal_id, conference_id) VALUES (%s, %s) RETURNING id", (journal_id[0], conference_id[0]))
                elif journal_id:
                    # Если есть в таблице journals, то заполняем journal_id
                    cur.execute("INSERT INTO publication_sources (journal_id) VALUES (%s) RETURNING id", (journal_id[0],))
                elif conference_id:
                    # Если есть в таблице conferences, то заполняем conference_id
                    cur.execute("INSERT INTO publication_sources (conference_id) VALUES (%s) RETURNING id", (conference_id[0],))
                else:
                    continue

                source_id = cur.fetchone()[0]
                print("New source_id", source_id)

                # Обновляем publication_source_id в таблице publications
                cur.execute("UPDATE publications SET publication_source_id = %s WHERE id = %s", (source_id, publication_id))

                # Фиксируем изменения в базе данных
                conn_account.commit()

            # Проверяем, существует ли непустая папка с таким же 16-ричным именем в актуальных папках
            dest_path = os.path.join(current_path, hex_folder)
            if os.path.isdir(dest_path) and os.listdir(dest_path):
                continue

            # Переносим файл из бекапа в актуальные папки
            print("item_path", item_path, "dest_path", dest_path)
            shutil.move(item_path, dest_path)

# Проходим по всем папкам бекапа
for source_folder in os.listdir(backup_path):
    # Проверяем, что текущая папка содержится в списке разрешенных папок
    if source_folder not in allowed_folders:
        continue
    source_path = os.path.join(backup_path, source_folder)
    if not os.path.isdir(source_path):
        continue
    process_folder(source_path, source_folder)

# Закрываем соединение с базой данных
cur.close()
conn_account.close()
'''

import os
import shutil
import psycopg2

# Путь к бекапу
backup_path = '/home/unreal_dodic/publications/'

# Путь к актуальным папкам
current_path = '/var/storages/data/publications/papers/'

# Список разрешенных папок
allowed_folders = ['assa', 'cpes', 'cs', 'dccn', 'druker', 'econvest', 'icct', 'ifac_tecis', 'mlsd', 'pu', 'ubs', 'vspu']

# Подключение к базе данных
conn_account = psycopg2.connect(
    dbname='account_db',
    user='isand',
    host='193.232.208.58',
    port='5432',
    password='sf3dvxQFWq@!'
)
cur = conn_account.cursor()


# Рекурсивный обход папок
def process_folder(folder_path, source_folder):
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isdir(item_path):
            process_folder(item_path, source_folder)
        else:
            # Получаем 16-ричное имя папки из пути к текущему файлу
            path_parts = os.path.normpath(folder_path).split(os.sep)
            hex_folder = path_parts[-8:]
            hex_folder = '/'.join(hex_folder)
            if not hex_folder.replace('/', '').isalnum() or len(hex_folder.replace('/', '')) != 16:
                continue
            publication_id = int(hex_folder.replace('/', ''), 16)
            # Проверяем, существует ли запись с этим id в таблице publications
            cur.execute("SELECT id FROM publications WHERE id = %s", (publication_id,))
            if not cur.fetchone():
                continue
            # Проверяем, существует ли непустая папка с таким же 16-ричным именем в актуальных папках
            dest_path = os.path.join(current_path, hex_folder)
            print("dest_path", dest_path)
            if not os.path.isdir(dest_path):
                continue
            # Переносим файл из бекапа в актуальные папки
            print("item_path", item_path, "dest_path", dest_path)
            shutil.copy2(item_path, dest_path)

# Проходим по всем папкам бекапа
for source_folder in os.listdir(backup_path):
    # Проверяем, что текущая папка содержится в списке разрешенных папок
    if source_folder not in allowed_folders:
        continue
    source_path = os.path.join(backup_path, source_folder)
    if not os.path.isdir(source_path):
        continue
    process_folder(source_path, source_folder)

# Закрываем соединение с базой данных
cur.close()
conn_account.close()
