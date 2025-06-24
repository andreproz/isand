from os.path import abspath, dirname
import sys

sys.path.append(dirname(abspath(__file__)))

CODE_WORD: str = '2Erx9354PWPm_:a2'

URL_PRND: str = 'https://www.ipu.ru/isand/json/'
URL_PARAM: list[str] = ['card', 'publication', 'cites', 'conferences', 'divisions', 'publishers', 'sources']
URL_PUBLICATIONS: str = 'https://www.ipu.ru/isand/json/publication/'
URL_PDF_PUBLICATION: str = 'https://www.ipu.ru/sites/default/files'

FILE_SYSTEM_PATH: str = '/var/storages/data/'
FILE_SYSTEM_SOURCE_DATA_PRND: str = 'source_data/prnd'
FILE_SYSTEM_PUBLICATION: str = 'publications/'
FILE_SYSTEM_PUBLICATION_PRND: str = 'publications/prnd/'
SITE_INFO_PATH: str = '../static/'
FILE_SYSTEM_WORKGROUP: str = 'workgroup/'
WORKGROUP_TEMP: str = 'temp'
TMP_ARCHIVE: str = 'archive'