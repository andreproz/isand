import requests
import hashlib

# Настройки
API_URL = "http://0.0.0.0:5006/filewatcher/upload"
COMPLETE_URL = "http://0.0.0.0:5006/filewatcher/complete_upload"
FILE_PATH = "/home/unreal_dodic/pdf_archive.tar.gz"
#FILE_PATH = "/home/unreal_dodic/9078.pdf"
ID_UPLOAD = 1853  # Пример идентификатора загрузки
ID_USER = 5  # Пример идентификатора пользователя
FORMAT = "tar.gz"  # Формат файла
PART_SIZE = 1024 * 1024 * 10  # Размер части файла (например, 10MB)

def calculate_md5(file_path):
    """Вычисляет MD5 хеш файла."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def split_file(file_path, part_size):
    """Генератор, разбивающий файл на части указанного размера."""
    with open(file_path, 'rb') as file:
        part_number = 0
        while True:
            data = file.read(part_size)
            if not data:
                break  # Завершаем цикл, если больше нет данных для чтения
            part_number += 1
            yield part_number, data

def upload_file_by_parts(file_path, id_upload, id_user, format):
    """Загружает файл по частям и завершает загрузку."""
    print("In upload_file_by_parts")
    file_hash = calculate_md5(file_path)
    for part_number, data in split_file(file_path, PART_SIZE):
        print("part_number", part_number)
        files = {
            'file': ('part_{}'.format(part_number), data),
            'file_hash': (None, file_hash)  # Передаем file_hash как часть мультипарт-формы
        }
        # Параметры id_upload, pnum и hash передаем через URL как query parameters
        print("API_URL", API_URL)
        response = requests.post(
            f"{API_URL}?id_upload={id_upload}&pnum={part_number}&hash=md5",
            files=files
        )
        if response.status_code != 200:
            print(f"Ошибка при загрузке части {part_number}: {response.text}")
            return False
        print(f"Часть {part_number} успешно загружена")
    
    # Завершаем загрузку, передавая параметры через URL
    complete_response = requests.post(
        f"{COMPLETE_URL}?id_upload={id_upload}&format={format}&id_user={id_user}&hash=md5"
    )
    if complete_response.status_code == 200:
        print("Загрузка успешно завершена")
    else:
        print(f"Ошибка при завершении загрузки: {complete_response.text}")

if __name__ == "__main__":
    upload_file_by_parts(FILE_PATH, ID_UPLOAD, ID_USER, FORMAT)