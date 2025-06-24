import requests
from datetime import datetime
#'''
from .filecore import getTextPdf, getUniversalFormat
#'''

'''
from filecore import getTextPdf, getUniversalFormat
'''

API_URL = "http://192.168.1.126:8000/process/"

def grobid2pdf(filepath):
    with open(filepath, 'rb') as f:
        files = {'files': f}
        response = requests.post(API_URL, files=files)

        # здесь вы можете обработать ответ сервера
        if response.ok:
            print("Файл успешно отправлен и обработан")
        else:
            print("Ошибка при отправке файла или его обработке")

        return response.json()

def operate_grobid(filepath):
    result_dict = grobid2pdf(filepath)
    #print("result_dict", result_dict)
    first_value = next(iter(result_dict.values()))
    #print("first_value", first_value)
    if not first_value: return None
    getTextPdf(filepath) # create txt file
    getUniversalFormat(filepath, first_value)
    publication_dict = list(result_dict.values())[0]['publications'][0]['publication']
    #print("result_dict", publication_dict)
    grobid_dict = dict()

    grobid_dict["authors"] = []
    grobid_dict["creation_date"] = datetime.now().strftime('%Y.%m.%d')
    grobid_dict["p_annotation"] = publication_dict.get("p_annotation")
    grobid_dict["p_annotation_add"] = publication_dict.get("p_annotation_add")
    grobid_dict["p_text"] = publication_dict.get("p_text")
    grobid_dict["p_title"] = publication_dict.get("p_title")    
    #grobid_dict["response"] = result_dict

    list_of_authors = publication_dict.get("authors")
    for l_o_a in list_of_authors:
        a_fio = l_o_a['author']['a_fio']
        grobid_dict["authors"].append(a_fio)

    #print("grobid_dict", grobid_dict)

    return grobid_dict

if __name__ == "__main__":
    filepath = "/var/storages/data/workgroup/temp/AiT1967/10914/10914.pdf"
    response = operate_grobid(filepath)
    #print("response", response)