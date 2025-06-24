import aioschedule as schedule
import asyncio
import json
import psycopg2
import sys
from asyncio import create_task, sleep
from datetime import datetime, timedelta
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Query, Request, WebSocket, Body
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from hashlib import md5
from json import load
from os import makedirs, listdir, remove, walk
from os.path import abspath, dirname, exists, isfile, join
from shutil import unpack_archive
from typing import Optional, Dict, Any

sys.path.append(dirname(abspath(__file__)))


from logic.python.config import FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP
from logic.update import update    
from logic.upload import upload    
from logic.grobid_update import grobid2folder
#from logic.filecore import organize_pdfs, copy_journals_with_direct_structure
from logic.python.filecore import copy_journals_with_direct_structure, getDirByID, organize_pdfs
from logic.python.get_classificator import get_classificator
from logic.sql.config import DBNAME, USER, HOST, PORT, PASSWORD
from logic.sql.postgres import SQLQuery

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

state = False # Флаг для автообовления 

async def start_scheduler():
    global state
    while state:
        now = datetime.now()
        next_run = now + timedelta(days=(7 - now.weekday()) % 7)  # Следующий понедельник
        next_run = next_run.replace(hour=4, minute=0, second=0, microsecond=0)  # В 4 утра
        #next_run = now + timedelta(minutes=2)
        if next_run < now:  # Если следующий запуск уже прошёл, перейти к следующему понедельнику
            next_run += timedelta(days=7)
            #next_run += timedelta(minutes=2)
        wait_seconds = (next_run - now).total_seconds()
        print(f"Next update scheduled at {next_run}, sleeping for {wait_seconds} seconds")
        await sleep(wait_seconds)
        if state: await update()

@app.websocket_route("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = await websocket.receive_text()
        if data == "start":
            await filewatcher_start() # Включение автообновления
        elif data == "stop":
            await filewatcher_stop() # Выключение автообновления
        # Отправка обновленного состояния всем подключенным клиентам
        await websocket.send_text(f"State updated: {state}")    

@app.get('/filewatcher/')
async def home(request: Request):
    return templates.TemplateResponse('index.html', {'request': request})
    
@app.get('/filewatcher/start') # Сюда добавить вызыв update()
async def filewatcher_start():
    global state
    state = True 
    date = (datetime.now() + timedelta(days=(0 - datetime.now().weekday()) % 7)).strftime("%d-%m-%Y")

    print("Ayou ye")

    create_task(update())   

    create_task(start_scheduler())

    return {'state': state, 'date': date}

@app.get('/filewatcher/stop')
async def filewatcher_stop():
    global state
    state = False
    return {'state': state, 'message': 'Auto-update stopped successfully'}

@app.get('/filewatcher/update')
async def filewathcer_update():
    create_task(update())

@app.get('/filewatcher/get_state')
async def filewatcher_get_state():
    date = (datetime.now() + timedelta(days=(0 - datetime.now().weekday()) % 7)).strftime("%d-%m-%Y")
    return {'state': state, 'date': date}

@app.get('/filewatcher/get_statistics')
async def filewatcher_get_statistics():
    pass  # Your existing logic here

@app.get('/filewatcher/get_terms')
async def get_terms():
    pass
    #return await get_terms_from_area()  # Assuming this is an async function

@app.get('/filewatcher/get_id_upload')
async def get_id_upload(id_upload: int = Query(..., description="ID загрузки")):
    print("In get_id_upload")    
    upload_dict = {}
    with open('upload_journal.txt', 'r') as file:
        lines = file.readlines()
        
        id_upload, id_part = lines.strip().split()
        if id_upload in upload_dict: upload_dict[id_upload].append(id_part)
        else: upload_dict[id_upload] = [id_part]

    return {
        "id_upload": id_upload,
        "parts": upload_dict[id_upload]
    } 

@app.post("/filewatcher/upload")
async def upload_part(
    id_upload: int = Query(..., description="ID загрузки"),
    pnum: int = Query(..., description="Номер части"),
    hash: str = Query(..., description="Алгоритм хеширования"),
    file: UploadFile = File(...),
    file_hash: str = Form(..., description="Хеш файла")
):
    print("In upload_part")
    # Проверка, что указанный алгоритм хеширования - md5
    if hash.lower() != 'md5':
        raise HTTPException(status_code=400, detail="Unsupported hash algorithm")
    
    upload_dir = join(FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP, "TEST_API_UPLOAD", str(id_upload))
    if not exists(upload_dir):
        makedirs(upload_dir)

    file_location = f"{upload_dir}/{pnum}"
    content = await file.read()

    # Считаем MD5 хеш загружаемого файла
    md5_hash = md5(content).hexdigest()
    if md5_hash != file_hash:
        raise HTTPException(status_code=400, detail="MD5 hash mismatch")

    print("file_location")
    with open(file_location, "wb") as buffer:
        buffer.write(content)

    upload_journal = "static/upload_journal.txt"
    with open(upload_journal, "a") as file:
        file.write(f'{id_upload}\t{pnum}\n')

    return {
        "id_upload": id_upload,
        "pnum": pnum,
        "hash_algorithm": hash,
        "status": "Part uploaded successfully",
        "file_hash": file_hash
    }

@app.post("/filewatcher/complete_upload")
async def complete_upload(
    id_upload: int = Query(..., description="ID загрузки"),
    format: str = Query(..., description="Формат файла"),
    id_user: int = Query(..., description="ID пользователя")
):
    upload_dir = join(FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP, "TEST_API_UPLOAD", str(id_upload))
    print("upload_dir in complete_upload", upload_dir)
    if not exists(upload_dir) or len(listdir(upload_dir)) == 0:
        raise HTTPException(status_code=404, detail="Upload not found or no parts uploaded.")

    print("listdir(upload_dir)", listdir(upload_dir))
    print("format", format)
    parts = sorted(listdir(upload_dir), key=lambda x: int(x))
    final_location = f"{upload_dir}/complete_file.{format}"
    print("final_location", final_location)
    with open(final_location, "wb") as final_file:
        for part in parts:
            part_path = join(upload_dir, part)
            with open(part_path, "rb") as part_file:
                final_file.write(part_file.read())

    archive_format = ['zip', 'tar.gz']
    file_format = ['pdf']

    if format in archive_format:
        unpack_archive(join(upload_dir, final_location), upload_dir, "gztar")
        organize_pdfs(upload_dir)        
        target_dir = "/var/storages/data/workgroup/temp/TEST_API_UPLOAD/copium"
        copy_journals_with_direct_structure(upload_dir, target_dir)
        #upload_articles(upload_dir, final_location, id_user)

    elif format in file_format:
        try:
            for root, dirs, files in walk(upload_dir):
                for file in files:
                    if not file.endswith("." + format):
                        remove(join(root, file))
        except OSError: print("Error occurred while deleting files.")

        print("Ready to upload", upload_dir, final_location, id_user)
        #upload_article(upload_dir, final_location, id_user)

    return {
        "id_upload": id_upload,
        "id_user": id_user,
        "format": format,
        "status": "Upload completed successfully"
    }

@app.get("/filewatcher/download_lk")
async def download(
    id_upload: int = Query(None, description="ID загрузки"),
    id_publ: int = Query(None, description="ID публикации"),
    id_user: str = Query(None, description="ID пользователя"),
    hash: str = Query("md5", regex="^md5$", description="Алгоритм хеширования"),
    whash: int = Query(None, description="Хэш объекта")
):
    if hash.lower() != 'md5':
        raise HTTPException(status_code=400, detail="Неподдерживаемый алгоритм хеширования")

    uni_dir = join(FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP, "TEST_API_UPLOAD", str(id_upload))
    print("directory with universal format in uni_dir", uni_dir)
    if not exists(uni_dir) or len(listdir(uni_dir)) == 0:
        raise HTTPException(status_code=404, detail="Upload not found or no parts uploaded.")

    segmented_file = None
    for file in listdir(uni_dir):
        print("file", file) 
        if file.endswith('.segmentated.json'):
            segmented_file = join(uni_dir, file)
            break

    if segmented_file is None:
        raise HTTPException(status_code=404, detail="File with .segmentated.json suffix not found.")

    return FileResponse(segmented_file)

@app.get("/filewatcher/download_filearchive")
async def download(
    id_publ: int = Query(None, description="ID публикации"),
    hash: str = Query("md5", regex="^md5$", description="Алгоритм хеширования"),
    whash: int = Query(0, description="Хэш объекта")
):
    if hash.lower() != 'md5':
        raise HTTPException(status_code=400, detail="Unsupported hashing algorithm.")
    
    dest_dir = getDirByID(id_publ) # Получить путь к папке на файловом архиве по id

    if not dest_dir or not exists(dest_dir):
        raise HTTPException(status_code=404, detail="There is no directory by this id on filearchive.")

    w_file = None
    file_extension = ""

    if whash == 0: # *.pdf
        file_extension = ".pdf"
    elif whash == 1: # *.segmentated.json
        file_extension = ".segmentated.json"
    elif whash == 2: # *.text.txt
        file_extension = ".text.txt"
    else:
        raise HTTPException(status_code=400, detail="Invalid 'whash' value")

    # Поиск файла с нужным расширением в директории
    for filename in listdir(dest_dir):
        if filename.endswith(file_extension):
            w_file = join(dest_dir, filename)
            break

    if not w_file or not exists(w_file):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(w_file)

'''
Получает файл-концепт (универсальный формат) и сохраняет его в папке
'''
@app.post("/filewatcher/get_concpect_file")
async def get_concpect_file(
    id_upload: int = Query(..., description="ID загрузки"),
    suffix: str = Query(..., description="Откуда универсальный формат загружен"),
    json_data: Optional[Dict[str, Any]] = Body(None),
):
    print("get_concpect_file", id_upload, "suffix", suffix, "json_data", json_data)
    if not json_data:
        raise HTTPException(status_code=400, detail="File or JSON data required.")

    uni_dir = join(FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP, "TEST_API_UPLOAD", str(id_upload))

    if not exists(uni_dir):
        raise HTTPException(status_code=404, detail="Directory not found.")

    if json_data:
        with open(join(uni_dir, f"data.{suffix}.json"), 'w') as f:
            json.dump(json_data, f)

    return {"status": "success"}

@app.get("/filewatcher/get_deltas")
async def get_deltas(id_publ: int = Query(..., description="ID публикации")):
    connection = psycopg2.connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    cursor = connection.cursor() 

    query = """
    SELECT fnv.variant, d.value
    FROM deltas d
    JOIN factors f ON d.factor_id = f.id
    JOIN factor_name_variants fnv ON f.id = fnv.factor_id
    WHERE d.publication_id = %s
    """
    cursor.execute(query, (id_publ,))
    results = cursor.fetchall() 
    cursor.close()
    connection.close()

    if not results:
        raise HTTPException(status_code=404, detail="Data not found for the given publication ID")

    deltas_dict = {row[0]: row[1] for row in results}

    return deltas_dict

@app.get("/filewatcher/get_link_source")
async def get_link_source(source_name: str = Query(None, description="Имя источника")):
    connection = psycopg2.connect(
        dbname='account_db',
        user='isand',
        host='193.232.208.58',
        port='5432',
        password='sf3dvxQFWq@!'
    )

    cursor = connection.cursor() 

    if source_name is None:
        query = """
        SELECT 'prnd' AS source_name, id, prnd_id FROM publication_mapping_prnd
        UNION ALL
        SELECT 'dk' AS source_name, publication_id, dk_id FROM publication_mapping_dk
        UNION ALL
        SELECT 'mathnet' AS source_name, pub_id, paperid FROM publication_mapping_mathnet
        """
    elif source_name == "prnd":
        query = """
        SELECT 'prnd' AS source_name, id, prnd_id 
        FROM publication_mapping_prnd
        """
    elif source_name == "dk":
        query = """
        SELECT 'dk' AS source_name, publication_id, dk_id 
        FROM publication_mapping_dk
        """
    elif source_name == "mathnet":
        query = """
        SELECT 'mathnet' AS source_name, pub_id, id 
        FROM publication_mapping_mathnet
        """
    else:
        raise HTTPException(status_code=400, detail="Неверное имя источника")

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    if not results:
        raise HTTPException(status_code=404, detail="Данные не найдены")

    return results


@app.get("/filewatcher/regrobid")
async def regrobid(
    id_upload: int = Query(..., description="ID загрузки")
):
    uni_dir = join(FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP, "TEST_API_UPLOAD", str(id_upload))
    if not exists(uni_dir):
        raise HTTPException(status_code=404, detail="Directory not found.")

    grobid_result = grobid2folder(uni_dir)

    if grobid_result == "0k":
        return {"status": "success"}
    else:
        return {"status": "error", "message": "Something has gone wrong"}

@app.get("/filewatcher/pub2basa")
async def pub2basa(
    id_upload: int = Query(..., description="ID загрузки")
):
    uni_dir = join(FILE_SYSTEM_PATH, FILE_SYSTEM_WORKGROUP, WORKGROUP_TEMP, "TEST_API_UPLOAD", str(id_upload))
    print("uni_dir", uni_dir)
    #pub_id = asyncio.run(upload(uni_dir))
    pub_id = await (upload(uni_dir, 3))
    return {
        "status": "success.",
        "pub_id": pub_id
    }

'''
Получает актуальный классификатор
'''
@app.get("/filewatcher/get_classificator")
async def get_concept_file():
    try:
        json_data = await asyncio.to_thread(get_classificator)

        if json_data:
            return JSONResponse(status_code=200, content={"status": "success", "classificator": json.loads(json_data)})
        else:
            return JSONResponse(status_code=404, content={"status": "fail", "message": "No data found"})

    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})   