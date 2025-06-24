import warnings
import re
import fitz
from PyPDF2 import PdfReader
from os.path import splitext
import json

def isEnglishLanguage(sentence):
    pattern1 = r'[^\s0-9a-zA-Z.,-:/№•+=@]'
    pattern2 = r'[^\s0-9А-Яа-яёЁ.,-:/№•+=@]'
    matches1 = re.findall(pattern1, sentence)
    matches2 = re.findall(pattern2, sentence)
    if len(matches1) < len(matches2):
        return True
    else:
        return False

def containsOtherEncodings(sentence):
    pattern = r'[^\s0-9a-zA-ZА-Яа-яёЁ.,-:/№•+=@]'
    matches = re.findall(pattern, sentence)
    return len(matches) > len(sentence)/4

def isEncrypted(data):
    try:
        if PdfReader(data).is_encrypted:
            return True
        else:
            return False
    except Exception:
        return False
    except RuntimeWarning:
        return False
    
def isExtractable(data):
    try:
        doc = fitz.open(data)  # open document
        pages = 0  # number of pages
        k = 0  # number of blank pages
        for page in doc:  # iterate the document pages
            pages += 1
            text = page.get_text()  # get plain text (is in UTF-8)
            if text == '':
                k += 1
        if k/pages > 0.8:  # если больше 80% страниц пусты, то весь файл без текстового слоя
            return 'no'
        else:
            return 'yes'
    except Exception:
        return 'error'
    except RuntimeWarning:
        return 'warning'    

def crcod(pdf_path: str) -> dict:
    warnings.filterwarnings('ignore')
    crcod_dict: dict = {
        'encrypted': isEncrypted(pdf_path),
        'extractable': isExtractable(pdf_path),
        'unknown_chars': containsOtherEncodings(pdf_path),
        'english_language': isEnglishLanguage(pdf_path),
    }
    #print("In crcod", pdf_path)
    filename_without_extension, _ = splitext(pdf_path.replace('"', ""))
    filename = filename_without_extension + ".crcod.json"
    #print("In crcod", filename)
    with open(f'{filename}', 'w') as file:
        json.dump(crcod_dict, file)
    return crcod_dict