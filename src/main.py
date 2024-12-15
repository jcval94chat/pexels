import os
import json
import random
import requests
import time
from py_pexels import PyPexels
from google_drive import get_latest_doc_words, upload_files_to_drive
from email_notify import send_email

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
    if os.path.exists(KEYWORDS_DICT_FILE):
        with open(KEYWORDS_DICT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_keywords_dict(keywords_dict):
    with open(KEYWORDS_DICT_FILE, 'w', encoding='utf-8') as f:
        json.dump(keywords_dict, f, ensure_ascii=False, indent=4)

def load_used_keywords():
    used = set()
    if os.path.exists(USED_KEYWORDS_FILE):
        with open(USED_KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                used.add(line.strip())
    return used

def save_used_keywords(used):
    with open(USED_KEYWORDS_FILE, 'w', encoding='utf-8') as f:
        for w in used:
            f.write(w+'\n')

def filter_new_keywords(all_keywords, used_keywords_set):
    return [w for w in all_keywords if w not in used_keywords_set]

def download_vids(search_videos_page, videos_descargados, prefijo='', verbose=True):
    archvi = []
    nueva_info = False
    download_folder = './temp_videos'
    if not os.path.exists(download_folder):
        os.makedirs(download_folder, exist_ok=True)
    
    for i, video in enumerate(search_videos_page.entries):
        nombre_archivo = prefijo+video.url.split('/')[-2] +'.mp4'
        archvi.append(nombre_archivo)

        if nombre_archivo not in videos_descargados:
            if verbose:
                print('Descargando', nombre_archivo)
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

            time.sleep(5)
    
    return archvi, nueva_info

# --- LÓGICA PRINCIPAL ---
# 1. Obtener las últimas 10 palabras del doc más reciente en la carpeta de Drive
doc_name, last_10_words = get_latest_doc_words(DOCS_FOLDER_ID, CREDENTIALS_FILE)
if doc_name is None:
    print("No se encontraron documentos, no se hace nada.")
    exit(0)

# Cargar keywords_dict y used_keywords
keywords_dict = load_keywords_dict()
used_keywords = load_used_keywords()

# Actualizar keywords_dict con el doc actual
keywords_dict[doc_name] = last_10_words
save_keywords_dict(keywords_dict)

# Filtrar nuevas palabras
new_keywords = filter_new_keywords(last_10_words, used_keywords)

if not new_keywords:
    print("No hay palabras nuevas, no se descargan videos.")
    nueva_info = False
else:
    # Usar una palabra al azar
    query = random.choice(new_keywords)
    py_pexel = PyPexels(api_key=API_KEY)
    search_videos_page = py_pexel.videos_search(query=query, page=random.randint(1,4), per_page=4)

    # Descarga de videos
    videos_descargados = set(used_keywords)  # Para evitar duplicados, reusamos used_keywords
    archivi, nueva_info = download_vids(search_videos_page, videos_descargados, prefijo='', verbose=True)

    # Actualizar used_keywords con los videos descargados (ej. añadir el query)
    for w in new_keywords:
        used_keywords.add(w)
    save_used_keywords(used_keywords)

# 2. Si nueva_info es True, subir a Drive. Si no, enviar correo.
if nueva_info:
    try:
        # Subir videos a Drive
        upload_files_to_drive('./temp_videos', VIDEOS_FOLDER_ID, CREDENTIALS_FILE)
    except Exception as e:
        print(f"No se pudo subir a Drive: {e}")
        send_email(RECIPIENT_EMAIL, "Información lista en el repositorio", "Hubo un problema subiendo a Drive. Verifique el repositorio.")
    else:
        # Suponiendo subida exitosa, opcionalmente informar por correo
        send_email(RECIPIENT_EMAIL, "Información lista en Drive", "La información ha sido subida exitosamente a Google Drive.")
else:
    # No hubo nueva info, se notifica por correo
    send_email(RECIPIENT_EMAIL, "Sin nueva información", "No hubo nueva información esta vez.")
