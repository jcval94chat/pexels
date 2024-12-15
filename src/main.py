import os
import json
import random
import requests
import time
import logging
import traceback
from nltk.corpus import stopwords
from pypexels import PyPexels
from google_drive import get_latest_doc_words, upload_files_to_drive
from email_notify import send_email

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('youtube_data.log', mode='a')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Cargar variables de entorno
DOCS_FOLDER_ID = os.environ.get("DOCS_FOLDER_ID")           # ID de la carpeta de Drive para documentos
VIDEOS_FOLDER_ID = os.environ.get("VIDEOS_FOLDER_ID")       # ID de la carpeta de Drive para videos
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")         # Email del destinatario
API_KEY = os.environ.get("PEXELS_API_KEY")                  # API key de Pexels
GCP_CREDENTIALS_ENV = os.environ.get("GCP_CREDENTIALS")     # Credenciales JSON de la cuenta de servicio

# Validar que GCP_CREDENTIALS exista y no esté vacío
if not GCP_CREDENTIALS_ENV:
    logger.error("La variable de entorno GCP_CREDENTIALS no está definida o está vacía.")
    exit(1)

try:
    # Comprobar que se puede cargar el JSON
    creds_dict = json.loads(GCP_CREDENTIALS_ENV)
    logger.info("Credenciales GCP cargadas correctamente desde la variable de entorno.")
except json.JSONDecodeError as e:
    logger.error(f"Error decodificando GCP_CREDENTIALS: {e}")
    exit(1)

# Archivos locales para mantener el historial
KEYWORDS_DICT_FILE = 'keywords_dict.json'
USED_KEYWORDS_FILE = 'used_keywords.txt'

# Cargar stopwords en español
STOPWORDS = set(stopwords.words('spanish'))

def load_keywords_dict():
    logger.info("Cargando diccionario de keywords.")
    if os.path.exists(KEYWORDS_DICT_FILE):
        with open(KEYWORDS_DICT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info("keywords_dict cargado correctamente.")
        return data
    logger.info("No existe keywords_dict.json, se retorna diccionario vacío.")
    return {}

def save_keywords_dict(keywords_dict, doc_name, last_10_words):
    logger.info("Guardando keywords_dict actualizado.")
    logger.info(f"Se agregan/actualizan las últimas 10 palabras para el documento '{doc_name}': {last_10_words}")
    with open(KEYWORDS_DICT_FILE, 'w', encoding='utf-8') as f:
        json.dump(keywords_dict, f, ensure_ascii=False, indent=4)
    logger.info("keywords_dict guardado correctamente.")

def load_used_keywords():
    logger.info("Cargando palabras clave usadas.")
    used = set()
    if os.path.exists(USED_KEYWORDS_FILE):
        with open(USED_KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                used.add(line.strip())
        logger.info("used_keywords cargado correctamente.")
    else:
        logger.info("No existe used_keywords.txt, se retorna conjunto vacío.")
    return used

def save_used_keywords(used):
    logger.info("Guardando palabras clave usadas actualizadas.")
    with open(USED_KEYWORDS_FILE, 'w', encoding='utf-8') as f:
        for w in used:
            f.write(w+'\n')
    logger.info("used_keywords guardado correctamente.")

def filter_new_keywords(all_keywords, used_keywords_set):
    logger.info("Filtrando nuevas palabras clave.")
    filtered = [w for w in all_keywords if w.lower() not in STOPWORDS]
    new = [w for w in filtered if w not in used_keywords_set]
    logger.info(f"Se encontraron {len(new)} nuevas palabras clave: {new}")
    old = [w for w in all_keywords if w in used_keywords_set]
    logger.info(f"Estas palabras ya estaban usadas y se descartan: {old}")
    discarded = [w for w in all_keywords if w.lower() in STOPWORDS]
    logger.info(f"Estas palabras fueron descartadas por ser stopwords: {discarded}")
    return new

def obtener_videos(py_pexel, query, max_retries=3):
    retries = 0
    while retries < max_retries:
        page = random.randint(1,4)
        search_videos_page = py_pexel.videos_search(query=query, page=page, per_page=4)
        entries = list(search_videos_page.entries)
        if len(entries) == 0:
            logger.warning(f"No se encontraron videos para '{query}' en la página {page}. Reintentando...")
            retries += 1
            time.sleep(2)
        else:
            logger.info(f"Se encontraron {len(entries)} videos para '{query}' en la página {page}.")
            return search_videos_page
    logger.error(f"No se encontraron videos para '{query}' después de {max_retries} intentos.")
    return None

def download_vids(search_videos_page, videos_descargados, prefijo='', verbose=True):
    logger.info("Iniciando descarga de videos.")
    archvi = []
    nueva_info = False
    download_folder = './temp_videos'
    if not os.path.exists(download_folder):
        os.makedirs(download_folder, exist_ok=True)
        logger.info(f"Carpeta {download_folder} creada.")

    for i, video in enumerate(search_videos_page.entries):
        nombre_archivo = prefijo + video.url.split('/')[-2] + '.mp4'
        archvi.append(nombre_archivo)

        if nombre_archivo not in videos_descargados:
            if verbose:
                logger.info(f'Descargando {nombre_archivo}')
            data_url = 'https://www.pexels.com/video/' + str(video.id) + '/download'
            r = requests.get(data_url)

            rcod = r.status_code
            if rcod >= 200 and rcod < 300:
                file_path = os.path.join(download_folder, nombre_archivo)
                with open(file_path, 'wb') as outfile:
                    outfile.write(r.content)
                videos_descargados.add(nombre_archivo)
                nueva_info = True
                logger.info(f"Video {nombre_archivo} descargado correctamente en {file_path}.")
            else:
                logger.warning(f"No se pudo descargar {nombre_archivo}. Código: {rcod}")

            time.sleep(5)

    logger.info("Descarga de videos finalizada.")
    return archvi, nueva_info

logger.info("Iniciando proceso principal.")

try:
    doc_name, last_10_words = get_latest_doc_words(DOCS_FOLDER_ID, GCP_CREDENTIALS_ENV)
    if doc_name is None:
        logger.info("No se encontraron documentos, se termina el proceso.")
        exit(0)
    logger.info(f"Documento obtenido: {doc_name}, últimas palabras: {last_10_words}")

    # Cargar keywords_dict y used_keywords
    keywords_dict = load_keywords_dict()
    used_keywords = load_used_keywords()

    # Actualizar keywords_dict con el doc actual
    keywords_dict[doc_name] = last_10_words
    save_keywords_dict(keywords_dict, doc_name, last_10_words)

    # Filtrar nuevas palabras
    new_keywords = filter_new_keywords(last_10_words, used_keywords)

    if not new_keywords:
        logger.info("No hay palabras nuevas, no se descargan videos.")
        nueva_info = False
    else:
        nueva_info = False
        py_pexel = PyPexels(api_key=API_KEY)
        for query in new_keywords:
            logger.info(f"Buscando videos con la palabra clave: {query}")
            search_videos_page = obtener_videos(py_pexel, query)
            if search_videos_page is None:
                logger.info(f"No se encontraron videos para '{query}' tras reintentos.")
                continue
            else:
                videos_descargados = set(used_keywords)
                archivi, info_descargada = download_vids(search_videos_page, videos_descargados, prefijo='', verbose=True)
                if info_descargada:
                    used_keywords.add(query)
                    nueva_info = True
        save_used_keywords(used_keywords)

    # Subir o enviar correo
    if nueva_info:
        logger.info("Se encontraron nuevos videos, intentando subir a Drive.")
        try:
            upload_files_to_drive('./temp_videos', VIDEOS_FOLDER_ID, GCP_CREDENTIALS_ENV)
        except Exception as e:
            logger.error(f"No se pudo subir a Drive: {e}")
            traceback.print_exc()
            send_email(RECIPIENT_EMAIL, "Información lista en el repositorio", "Hubo un problema subiendo a Drive. Verifique el repositorio.")
        else:
            logger.info("Videos subidos a Drive exitosamente.")
            send_email(RECIPIENT_EMAIL, "Información lista en Drive", "La información ha sido subida exitosamente a Google Drive.")
    else:
        logger.info("No hubo nueva info, notificando vía correo.")
        send_email(RECIPIENT_EMAIL, "Sin nueva información", "No hubo nueva información esta vez.")

except Exception as e:
    logger.error(f"Error en el proceso principal: {e}")
    traceback.print_exc()
    send_email(RECIPIENT_EMAIL, "Error en el proceso", f"Ha ocurrido un error: {e}")
    exit(1)

logger.info("Proceso finalizado con éxito.")
