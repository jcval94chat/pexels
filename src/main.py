import os
import json
import random
import requests
import time
import logging
import traceback

from pypexels import PyPexels
from google_drive import get_latest_doc_words, upload_files_to_drive
from email_notify import send_email

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear formato de logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Crear manejador para archivo
file_handler = logging.FileHandler('youtube_data.log', mode='a')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Crear manejador para consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Cargar secretos desde variables de entorno
DOCS_FOLDER_ID = os.environ.get("DOCS_FOLDER_ID")  # ID de la carpeta de Drive
VIDEOS_FOLDER_ID = os.environ.get("VIDEOS_FOLDER_ID")  # ID de la carpeta de Drive
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")  # Email del destinatario
API_KEY = os.environ.get("PEXELS_API_KEY")  # API key de Pexels

# Archivos locales para mantener el historial
KEYWORDS_DICT_FILE = 'keywords_dict.json'
USED_KEYWORDS_FILE = 'used_keywords.txt'
CREDENTIALS_FILE = 'credentials.json'  # Descargado desde el secreto GCP_CREDENTIALS

def load_keywords_dict():
    logger.info("Cargando diccionario de keywords.")
    if os.path.exists(KEYWORDS_DICT_FILE):
        with open(KEYWORDS_DICT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info("keywords_dict cargado correctamente.")
        return data
    logger.info("No existe keywords_dict.json, se retorna diccionario vacío.")
    return {}

def save_keywords_dict(keywords_dict):
    logger.info("Guardando keywords_dict actualizado.")
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
    new = [w for w in all_keywords if w not in used_keywords_set]
    logger.info(f"Se encontraron {len(new)} nuevas palabras clave.")
    return new

def download_vids(search_videos_page, videos_descargados, prefijo='', verbose=True):
    logger.info("Iniciando descarga de videos.")
    archvi = []
    nueva_info = False
    download_folder = './temp_videos'
    if not os.path.exists(download_folder):
        os.makedirs(download_folder, exist_ok=True)
        logger.info(f"Carpeta {download_folder} creada.")

    for i, video in enumerate(search_videos_page.entries):
        nombre_archivo = prefijo+video.url.split('/')[-2] +'.mp4'
        archvi.append(nombre_archivo)

        if nombre_archivo not in videos_descargados:
            if verbose:
                logger.info(f'Descargando {nombre_archivo}')
            data_url = 'https://www.pexels.com/video/' + str(video.id) + '/download'
            r = requests.get(data_url)

            rcod = r.status_code
            tipo_archivo = r.headers.get('content-type')

            if rcod >= 200 and rcod < 300 and 'mp4' in tipo_archivo:
                file_path = os.path.join(download_folder, nombre_archivo)
                with open(file_path, 'wb') as outfile:
                    outfile.write(r.content)
                videos_descargados.add(nombre_archivo)
                nueva_info = True
                logger.info(f"Video {nombre_archivo} descargado correctamente en {file_path}.")
            else:
                logger.warning(f"No se pudo descargar {nombre_archivo}. Código: {rcod}, Tipo: {tipo_archivo}")

            time.sleep(5)
    
    logger.info("Descarga de videos finalizada.")
    return archvi, nueva_info

# --- LÓGICA PRINCIPAL ---
logger.info("Iniciando proceso principal.")


if os.path.exists(CREDENTIALS_FILE):
    logger.info(f"El archivo de credenciales existe en la ruta: {CREDENTIALS_FILE}")
    with open(CREDENTIALS_FILE, 'r') as f:
        logger.info(f"Contenido de credentials.json: {f.read()[:100]}...")  # Muestra los primeros 100 caracteres
else:
    logger.error("El archivo de credenciales no existe.")
    exit(1)

try:
    doc_name, last_10_words = get_latest_doc_words(DOCS_FOLDER_ID, CREDENTIALS_FILE)
    if doc_name is None:
        logger.info("No se encontraron documentos, se termina el proceso.")
        exit(0)
    logger.info(f"Documento obtenido: {doc_name}, últimas palabras: {last_10_words}")

    # Cargar keywords_dict y used_keywords
    keywords_dict = load_keywords_dict()
    used_keywords = load_used_keywords()

    # Actualizar keywords_dict con el doc actual
    keywords_dict[doc_name] = last_10_words
    save_keywords_dict(keywords_dict)

    # Filtrar nuevas palabras
    new_keywords = filter_new_keywords(last_10_words, used_keywords)

    if not new_keywords:
        logger.info("No hay palabras nuevas, no se descargan videos.")
        nueva_info = False
    else:
        query = random.choice(new_keywords)
        logger.info(f"Buscando videos con la palabra clave: {query}")
        py_pexel = PyPexels(api_key=API_KEY)
        search_videos_page = py_pexel.videos_search(query=query, page=random.randint(1,4), per_page=4)

        # Descarga de videos
        videos_descargados = set(used_keywords)
        archivi, nueva_info = download_vids(search_videos_page, videos_descargados, prefijo='', verbose=True)

        for w in new_keywords:
            used_keywords.add(w)
        save_used_keywords(used_keywords)

    # Subir o enviar correo
    if nueva_info:
        logger.info("Se encontraron nuevos videos, intentando subir a Drive.")
        try:
            upload_files_to_drive('./temp_videos', VIDEOS_FOLDER_ID, CREDENTIALS_FILE)
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
