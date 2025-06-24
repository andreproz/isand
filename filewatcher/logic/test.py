import os
import shutil

def organize_pdfs(base_dir):
    for root, dirs, files in os.walk(base_dir):
        # Пропускаем каталог 'archive' и все его подкаталоги
        if 'archive' in root:
            continue

        for file in files:
            if file.endswith('.pdf'):
                # Получаем полный путь файла
                file_path = os.path.join(root, file)
                # Создаем название нового каталога без расширения файла
                new_dir_name = os.path.splitext(file)[0]
                new_dir_path = os.path.join(root, new_dir_name)

                # Создаем новый каталог, если он еще не существует
                if not os.path.exists(new_dir_path):
                    os.makedirs(new_dir_path)

                # Перемещаем файл PDF в новый каталог
                new_file_path = os.path.join(new_dir_path, file)
                shutil.move(file_path, new_file_path)
                
organize_pdfs(f'/var/storages/data/workgroup/temp/PUBSS')

def compare_jsons_v3(json1, json2):
    # Проверяем, являются ли поля каждого JSON подмножеством другого
    if all(key in json2 for key in json1) or all(key in json1 for key in json2):
        # Суммируем значения полей каждого JSON
        sum1 = sum(json1.values())
        sum2 = sum(json2.values())

        # Если суммы больше 10, рассчитываем разницу в процентах
        if sum1 > 10 and sum2 > 10:
            diff_percent = abs(sum1 - sum2) / max(sum1, sum2) * 100
            # Проверяем, отличается ли количество цифр не более чем на 10%
            return diff_percent <= 10
        else:
            # Проверяем на полное равенство сумм
            return sum1 == sum2
    else:
        return False

def copy_journals_with_direct_structure(source_dir, target_dir):
    """
    Копирует журналы из исходного каталога в целевой, изменяя структуру каталогов для PDF файлов,
    так чтобы каждый PDF файл находился в своей подпапке непосредственно в каталоге журнала.
    
    :param source_dir: Путь к исходному каталогу с журналами.
    :param target_dir: Путь к целевому каталогу для копирования.
    """
    for root, dirs, files in os.walk(source_dir):
        # Определяем название журнала как первую часть относительного пути к файлу
        journal_name = os.path.relpath(root, source_dir).split(os.sep)[0]
        target_journal_dir = os.path.join(target_dir, journal_name)
        
        for file in files:
            if file.endswith('.pdf'):
                # Создаем подпапку для каждого PDF файла непосредственно в каталоге журнала
                file_folder_name = file[:-4] # Убираем расширение .pdf для названия папки
                target_file_dir = os.path.join(target_journal_dir, file_folder_name)
                if not os.path.exists(target_file_dir):
                    os.makedirs(target_file_dir)
                
                source_file_path = os.path.join(root, file)
                target_file_path = os.path.join(target_file_dir, file)
                
                # Копирование файла
                shutil.copy(source_file_path, target_file_path)


# Пример использования функции
source_dir = '/var/storages/temp'
target_dir = '/var/storages/temp2'
copy_journals_with_direct_structure(source_dir, target_dir)

# # Тестирование обновленной функции
# json1 = {'поле1': 3, 'поле2': 6}
# json2 = {'поле1': 3, 'поле2': 6, 'поле3': 2}

# # Пример использования функции
# result_v3 = compare_jsons_v3(json1, json2)
# result_v3