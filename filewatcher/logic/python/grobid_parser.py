import json
import re
from bs4 import BeautifulSoup
from datetime import datetime

from lingua import LanguageDetectorBuilder, Language


def read_tei(tei_file):
    with open(tei_file, 'r') as tei:
        soup = BeautifulSoup(tei, 'xml')
        return soup
    raise RuntimeError('Cannot generate a soup from the input')

def elem_to_text(elem, default=''):
    if elem:
        return elem.getText()
    else:
        return default

class TEIFile(object):
    def __init__(self, filename):
        self.filename = filename
        self.soup = read_tei(filename)
        self.data = {
            "main_lang": "",
            "add_lang": "",
            "doi": "",
            "title": "",
            "title_add": "",
            "authors": [],
            "authors_add": [],
            "keywords": [],
            "keywords_add": [],
            "annotation": "",
            "annotation_add": "",
            "text": "",
            "references": []
        }

    def parse(self):
        self.data["text"] = self.text
        self.data["doi"] = self.doi
        self.data["title"], self.data["title_add"] = self.title
        self.data["authors"], self.data["authors_add"] = self.authors
        self.data["keywords"], self.data["keywords_add"] = self.keywords
        self.data["annotation"], self.data["annotation_add"] = self.annotation
        self.data["text"], self.data["text_add"] = self.text
        self.data["references"] = self.parse_bibliography


    @property
    def text(self):
        text_list = []
        plain_text = ''
        plain_text_add = ''
        text_part = self.soup.find('text').find('body').find_all('p')
        for p in text_part:
            for pp in p.stripped_strings:
                text_list.append(pp)
        plain_text = ' '.join(text_list)
        self.data["main_lang"] = lang(plain_text)
        return plain_text, plain_text_add

    @property
    def doi(self):
        result = ''
        idno_elem = self.soup.find('teiHeader').find('idno', type='DOI')
        if idno_elem:
            result = f'doi="{idno_elem.getText()}"; '
        return result

    @property
    def title(self):
        title = self.soup.find('title', type='main')
        title_main = ''
        title_add = ''
        if lang(elem_to_text(title)) == self.data['main_lang']:
            title_main = elem_to_text(title)
        else:
            self.data['add_lang'] = lang(elem_to_text(title))
            title_add = elem_to_text(title)
        return title_main, title_add

    @property
    def authors(self):
        authors_in_header = self.soup.teiHeader.find_all('author')
        result = []
        author_num = 1
        result_add = []
        author_num_add = 1
        for author in authors_in_header:
            persname = author.persName
            if not persname:
                continue
            firstname = elem_to_text(persname.find("forename", type="first"))
            middlename = elem_to_text(persname.find("forename", type="middle"))
            surname = elem_to_text(persname.surname)
            fio = firstname + ' ' + middlename + ' ' + surname
            email = elem_to_text(author.email)
            position = elem_to_text(persname.rolename)
            affiliations = []
            affiliation_elems = author.find_all('affiliation')
            if affiliation_elems:
                for affiliation_elem in affiliation_elems:
                    affiliation_raw = elem_to_text(affiliation_elem.find('note', type='raw_affiliation'))
                    orgName = elem_to_text(affiliation_elem.find('orgName', type='institution'))
                    address_raw = affiliation_raw.replace(orgName, '')
                    addrLine = elem_to_text(affiliation_elem.addrLine)
                    postCode = elem_to_text(affiliation_elem.postCode)
                    settlement = elem_to_text(affiliation_elem.settlement)
                    country = elem_to_text(affiliation_elem.country)
                    organization = {  # Аффилиация
                        "a_aff_raw": affiliation_raw,  # Исходный текст
                        "a_aff_org": {
                            "org_name": orgName,   # Название организации
                            "org_address": {
                                "addr_raw": address_raw,  # Исходный текст
                                "addr_street": addrLine,  # Улица, дом
                                "addr_postcode": postCode,  # Почтовый индекс
                                "addr_city": settlement,  # Город (Пополняемый перечень?)
                                "addr_country": country  # Страна (Пополняемый перечень?)    
                            },
                            "org_ids": {
                                "org_ror": "",   #ROR
                                "org_isni": "",   #ISNI
                            },
                        },
                        "a_aff_position": position,  # Должность (Пополняемый перечень?)
                    }
                    affiliations.append({"a_affiliation":organization}) # Аффилиации
            
            

            if lang(fio) == self.data['main_lang']:
                author = {
                    "a_num": author_num,  # Порядковый номер автора
                    "a_fio": fio,  # ФИО
                    "a_last_name": surname,  # Фамилия
                    "a_first_name": firstname,  # Имя
                    "a_sec_name": middlename,  # Отчество или остальная часть имени
                    "a_email": email,  # email
                    "a_address": {  # Адрес
                        "addr_raw": "",  # Исходный текст
                        "addr_street": "",  # Улица, дом
                        "addr_postcode": "",  # Почтовый индекс
                        "addr_city": "",  # Город (Пополняемый перечень?)
                        "addr_country": ""  # Страна (Пополняемый перечень?)
                    },
                    "a_degree": "",  # Ученая степень (согласованный перечень)
                    "a_rank": "",  # Звание (согласованный перечень)
                    "a_affiliations": affiliations,  # Аффилиации
                    "a_workplace": {
                        "org_name": "",   # Название организации
                                "org_address": {
                                    "addr_raw": "",  # Исходный текст
                                    "addr_street": "",  # Улица, дом
                                    "addr_postcode": "",  # Почтовый индекс
                                    "addr_city": "",  # Город (Пополняемый перечень?)
                                    "addr_country": ""  # Страна (Пополняемый перечень?)    
                                },
                                "org_ids": {
                                    "org_ror": "",   #ROR
                                    "org_isni": "",   #ISNI
                                },
                    },
                    "a_ids": {  # Идентификаторы
                        "a_spin": "",  # SPIN
                        "a_researcherid": "",  # ResearcherID
                        "a_orcid": "",  # ORCID
                        "a_scopusid": "",  # ScopusID
                        "a_prnd": "",  # ИД ПРНД
                        "a_rinc": "",  # ИД РИНЦ
                    }
                }
                result.append({"author":author})
                author_num += 1
            else:
                author = {
                    "a_num": author_num_add,  # Порядковый номер автора
                    "a_fio": fio,  # ФИО
                    "a_last_name": surname,  # Фамилия
                    "a_first_name": firstname,  # Имя
                    "a_sec_name": middlename,  # Отчество или остальная часть имени
                    "a_email": email,  # email
                    "a_address": {  # Адрес
                        "addr_raw": "",  # Исходный текст
                        "addr_street": "",  # Улица, дом
                        "addr_postcode": "",  # Почтовый индекс
                        "addr_city": "",  # Город (Пополняемый перечень?)
                        "addr_country": ""  # Страна (Пополняемый перечень?)
                    },
                    "a_degree": "",  # Ученая степень (согласованный перечень)
                    "a_rank": "",  # Звание (согласованный перечень)
                    "a_affiliations": affiliations,  # Аффилиации
                    "a_workplace": {
                        "org_name": "",   # Название организации
                                "org_address": {
                                    "addr_raw": "",  # Исходный текст
                                    "addr_street": "",  # Улица, дом
                                    "addr_postcode": "",  # Почтовый индекс
                                    "addr_city": "",  # Город (Пополняемый перечень?)
                                    "addr_country": ""  # Страна (Пополняемый перечень?)    
                                },
                                "org_ids": {
                                    "org_ror": "",   #ROR
                                    "org_isni": "",   #ISNI
                                },
                    },
                    "a_ids": {  # Идентификаторы
                        "a_spin": "",  # SPIN
                        "a_researcherid": "",  # ResearcherID
                        "a_orcid": "",  # ORCID
                        "a_scopusid": "",  # ScopusID
                        "a_prnd": "",  # ИД ПРНД
                        "a_rinc": "",  # ИД РИНЦ
                    }
                }
                self.data['add_lang'] = lang(author['a_fio'])
                result_add.append({"author":author})
                author_num_add += 1
        return result, result_add
        
    @property
    def keywords(self):
        result = []
        k = 1
        result_add = []
        k_add = 1
        if self.soup.teiHeader.find('keywords'):
            keywords = self.soup.teiHeader.keywords.find_all('term')
            for keyword in keywords:
                if lang(keyword.getText()) == self.data['main_lang']:
                    result_add.append({"p_key_word": {
                        "p_key_word_num": k,  # Номер
                        "p_key_word_value": keyword.getText()}})   # Значение
                    k += 1
                else:
                    self.data['add_lang'] = lang(keyword.getText())
                    result_add.append({"p_key_word": {
                        "p_key_word_num": k_add,  # Номер
                        "p_key_word_value": keyword.getText()}})   # Значение
                    k_add += 1
        return result, result_add

    @property
    def annotation(self):
        result = ''
        result_add = ''
        if self.soup.teiHeader.abstract:
            abstract = self.soup.teiHeader.abstract.getText(separator=' ', strip=True)
            if lang(abstract) == self.data['main_lang']:
                result = abstract
            else:
                self.data['add_lang'] = lang(abstract)
                result_add = abstract
        return result, result_add


    @property
    def parse_bibliography(self):
        k = 0
        references = []
        list_bibl = self.soup.find('listBibl')
        if list_bibl:
            bibl_elems = list_bibl.find_all('biblStruct')
            if bibl_elems:
                for bibl in bibl_elems:
                    text = elem_to_text(bibl.find('note', type="raw_reference"))
                    k += 1
                    number = k
                    authors = []
                    if bibl.find_all('author'):
                        for author in bibl.find_all('author'):
                            firstname = elem_to_text(author.find("forename", type="first"))
                            middlename = elem_to_text(author.find("forename", type="middle"))
                            surname = elem_to_text(author.surname)
                            fio = firstname + ' ' + middlename + ' ' + surname
                            person = {
                                "r_a_fio": fio,  # ФИО
                                "r_a_last_name": surname,  # Фамилия
                                "r_a_first_name": firstname,  # Имя
                                "r_a_sec_name": middlename,  # Отчество
                                "r_a_ids": {    # Идентификаторы автора
                                    "a_spin": "",        # SPIN
                                    "a_researcherid": "",        # ResearcherID
                                    "a_orcid": "",        # ORCID
                                    "a_scopusid": "",        # ScopusID
                                    "a_prnd": "",        # ИД ПРНД
                                    "a_rinc": "",        # ИД РИНЦ
                                },
                            }
                            authors.append({"r_author": person})
                    source_title = ''
                    title = ''
                    if bibl.find('title', level='a'):
                        title = elem_to_text(bibl.find('title', level='a'))
                        if bibl.find('title', level='j'):
                            source_title = elem_to_text(bibl.find('title', level='j'))
                        elif bibl.find('title', level='m'):
                            source_title = elem_to_text(bibl.find('title', level='m'))
                    elif bibl.find('title', level='m'):
                        title = elem_to_text(bibl.find('title', level='m'))
                    url = bibl.find('ptr')['target'] if bibl.find('ptr') else ''
                    doi = elem_to_text(bibl.find('idno', type='DOI'))
                    city = elem_to_text(bibl.find('pubPlace'))
                    volume = elem_to_text(bibl.find('biblScope', unit="volume"))
                    issue = elem_to_text(bibl.find('biblScope', unit="issue")) 
                    section = elem_to_text(bibl.find('title', level='s'))
                    # pages = f"{(bibl.find('biblScope', unit='page')['from'])}-{(bibl.find('biblScope', unit='page')['to'])}" if bibl.find('biblScope', unit='page') else ''
                    
                    bibl_scope = bibl.find('biblScope', unit='page')
                    if bibl_scope:
                        from_value = bibl_scope.get('from', '')
                        to_value = bibl_scope.get('to', '')
                        pages = f"{from_value}-{to_value}" if from_value and to_value else bibl_scope.text
                    else:
                        pages = ''
                    
                    publisher = elem_to_text(bibl.find('publisher'))
                    year = bibl.find('date',type='published')['when'] if bibl.find('date',type='published') else ''
                    year = re.search(r"\d{4}", year).group(0) if re.search(r"\d{4}", year) else 0

                    reference = { # Библиогр. ссылка
                        "r_number": number,  # Номер ссылки
                        "r_raw": text,  # Исходный текст
                        "r_authors": authors,  # Авторы ссылки
                        "r_name": title,  # Название
                        "r_doi": doi,  # DOI
                        "r_url": url,  # URL
                        "r_rus_text": "",  # Русская версия
                        "r_source": {  # Источник
                            "s_title": source_title,  # Название
                            "s_city": city,  # Город (Пополняемый перечень?)
                            "s_volume": volume,  # Том
                            "s_issue": issue,  # Выпуск
                            "s_section": section,  # Раздел
                            "s_pages": "",  # Страницы
                            "s_year": "",  # Год издания
                            "s_date": "", # Формат даты «dd.mm.yyyy»
                            "s_publisher": {  # Издательство
                                "org_name": publisher,  # Название
                                "org_address": {  # Адрес
                                    "addr_raw": "",  # Исходный текст
                                    "addr_street": "",  # Улица, дом
                                    "addr_postcode": "",  # Почтовый индекс
                                    "addr_city": "",  # Город (Пополняемый перечень?)
                                    "addr_country": ""  # Страна (Пополняемый перечень?)
                                },
                                "org_ids": {
                                    "org_ror": "",   #ROR
                                    "org_isni": "",   #ISNI
                                },
                            },
                            "s_codes": "",  # Коды
                            "s_edition": "",  # Версия издания
                            "s_type": ""  # Тип источника
                        },
                        "r_year": year,  # Год
                        "r_pages": pages,  # Страницы
                        "r_ids": {
                            "id_openalex": "",  # Ид-р Open Alex
                        },
                        "r_is_short": "False", # Это короткий формат ссылки
                    }
                    references.append({"reference": reference })
        return references
        

def create_json_structure(data, ext_source=""):
    json_structure = {
        "version": "3.4",
        "ext_source": ext_source,
        "creation_date": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),  # Дата создания файла «dd.MM.yyyy 24HH:mi:ss»
        "publications": [  # Массив публикаций
            {
                "publication": {  # Публикация
                    "p_type": "",  # Тип публикации (Согласованный перечень: доклад, статья, монография…)
                    "p_reference": "",  # Ссылка на файл (Либо на файл, либо URL)
                    "p_title": data['title'],  # Название
                    "p_title_add": data['title_add'],  # Название на дополн. языке
                    "p_annotation": data['annotation'],  # Аннотация
                    "p_annotation_add": data['annotation_add'],  # Аннотация на дополн. языке
                    "p_text": data['text'],  # Текст
                    "p_text_add": data['text_add'],  # Текст на дополн. языке
                    "p_key_words": data['keywords'],  # Ключевые слова
                    "p_key_words_add": data['keywords_add'],  # Ключевые слова на дополн. языке
                    "references": data['references'],
                    "references_by": [
                        {
                            "reference": {
                                "r_number": 1,  # Номер ссылки
                                "r_raw": "",  # Исходный текст
                                "r_authors": [  # Авторы ссылки
                                    {
                                        "r_author": {
                                            "r_a_fio": "",  # ФИО
                                            "r_a_last_name": "",  # Фамилия
                                            "r_a_first_name": "",  # Имя
                                            "r_a_sec_name": "",  # Отчество
                                            "r_a_ids": {    # Идентификаторы автора
                                                "a_spin": "",        # SPIN
                                                "a_researcherid": "",        # ResearcherID
                                                "a_orcid": "",        # ORCID
                                                "a_scopusid": "",        # ScopusID
                                                "a_prnd": "",        # ИД ПРНД
                                                "a_rinc": "",        # ИД РИНЦ
                                            },
                                        },
                                    }
                                ],  
                                "r_name": "",  # Название
                                "r_doi": "",  # DOI
                                "r_url": "",  # URL
                                "r_rus_text": "",  # Русская версия
                                "r_source": {  # Источник
                                    "s_title": "",  # Название
                                    "s_city": "",  # Город (Пополняемый перечень?)
                                    "s_volume": "",  # Том
                                    "s_issue": "",  # Выпуск
                                    "s_section": "",  # Раздел
                                    "s_pages": "",  # Страницы
                                    "s_year": "",  # Год издания
                                    "s_date": "", # Формат даты «dd.mm.yyyy»
                                    "s_publisher": {  # Издательство
                                        "org_name": "",  # Название
                                        "org_address": {  # Адрес
                                            "addr_raw": "",  # Исходный текст
                                            "addr_street": "",  # Улица, дом
                                            "addr_postcode": "",  # Почтовый индекс
                                            "addr_city": "",  # Город (Пополняемый перечень?)
                                            "addr_country": ""  # Страна (Пополняемый перечень?)
                                        },
                                        "org_ids": {
                                            "org_ror": "",   #ROR
                                            "org_isni": "",   #ISNI
                                        },
                                    },
                                    "s_codes": "",  # Коды
                                    "s_edition": "",  # Версия издания
                                    "s_type": ""  # Тип источника
                                },
                                "r_year": "",  # Год
                                "r_pages": "",  # Страницы
                                "r_ids": {
                                    "id_openalex": "",  # Ид-р Open Alex
                                },
                                "r_is_short": "", # Это короткий формат ссылки
                            },
                        }
                    ],
                    "authors": data['authors'],  # Авторы
                    "authors_add": data['authors_add'],  # Авторы на дополн. языке
                    "source": {  # Источник
                        "s_title": "",  # Название
                        "s_city": "",  # Город (Пополняемый перечень?)
                        "s_volume": "",  # Том
                        "s_issue": "",  # Выпуск
                        "s_section": "",  # Раздел
                        "s_pages": "",  # Страницы
                        "s_year": "",  # Год издания
                        "s_date": "", # Формат даты «dd.mm.yyyy»
                        "s_publisher": {  # Издательство
                            "org_name": "",  # Название
                            "org_address": {  # Адрес
                                "addr_raw": "",  # Исходный текст
                                "addr_street": "",  # Улица, дом
                                "addr_postcode": "",  # Почтовый индекс
                                "addr_city": "",  # Город (Пополняемый перечень?)
                                "addr_country": ""  # Страна (Пополняемый перечень?)
                            },
                            "org_ids": {
                                "org_ror": "",   #ROR
                                "org_isni": "",   #ISNI
                            },
                        },
                        "s_codes": "",  # Коды
                        "s_edition": "",  # Версия издания
                        "s_type": ""  # Тип источника
                    },
                    "source_add": {  # Источник на дополн. языке
                        "s_title": "",  # Название
                        "s_city": "",  # Город (Пополняемый перечень?)
                        "s_volume": "",  # Том
                        "s_issue": "",  # Выпуск
                        "s_section": "",  # Раздел
                        "s_pages": "",  # Страницы
                        "s_year": "",  # Год издания
                        "s_date": "", # Формат даты «dd.mm.yyyy»
                        "s_publisher": {  # Издательство
                            "org_name": "",  # Название
                            "org_address": {  # Адрес
                                "addr_raw": "",  # Исходный текст
                                "addr_street": "",  # Улица, дом
                                "addr_postcode": "",  # Почтовый индекс
                                "addr_city": "",  # Город (Пополняемый перечень?)
                                "addr_country": ""  # Страна (Пополняемый перечень?)
                            },
                            "org_ids": {
                                "org_ror": "",   #ROR
                                "org_isni": "",   #ISNI
                            },
                        },
                        "s_codes": "",  # Коды
                        "s_edition": "",  # Версия издания
                        "s_type": ""  # Тип источника
                    },
                    "ids": data['doi'],  # Идентификаторы
                    "codes": "",  # Коды
                    "p_pages": "",  # Страницы
                    "p_finans_source": "",  # Источник финансирования
                    "p_lang": data['main_lang'],  # Основной язык публикации (Двухбуквенный код языка)
                    "p_lang_add": data['add_lang'],  # Дополн. язык публикации (Двухбуквенный код языка)
                    "p_categories": {   # Категории  (Различные для разных источников данных)
                        "p_c_openalexes": [  # Cписок концептов Open Alex
                            {
                                "p_c_openalex": { # концепт Open Alex
                                    "p_c_id": "",  # ИД  (Идентификатор концепта Open Alex)
                                    "p_c_wikidata": "",  # URL Wikidata  (Ссылка на значение в Wikidata)
                                    "p_c_display_name": "",  # Наименование  (Пример = "Computer science")
                                    "p_c_level": "",  # Уровень  (0, 1, 2, … 8)
                                    "p_c_score": "",  # Рейтинг  ( Пример = 0.7289707)
                                }   
                            }
                        ]
                    }
                }
            }
        ]
    }

    return json_structure

def lang(text):
    language = detector.detect_language_of(text)
    if language:
        return language.iso_code_639_1.name
    return ''

# Загружаем нужные языки в распознавалку.
# Если загрузить все, то займет около 1 GB памяти
detector = LanguageDetectorBuilder.from_languages(Language.RUSSIAN, Language.ENGLISH, 
                                                  Language.UKRAINIAN, Language.BELARUSIAN, Language.KAZAKH, 
                                                  Language.FRENCH, Language.GERMAN, Language.CHINESE).build()




# Пример использования:
if __name__ == '__main__':
    '''
    # Пример использования:
    tei_file = TEIFile("/home/unreal_dodic/062_116..grobid.tei.xml")
    tei_file.parse()
    parsed_data = tei_file.data

    # Параметр для указания ВНЕШНЕГО ИСТОЧНИКА
    ext_source = ""

    json_data = create_json_structure(parsed_data, ext_source)
    with open("/home/unreal_dodic/062_116.segmented.json", "w", encoding="utf-8") as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=2)

    print(json_data)
    '''
    pass