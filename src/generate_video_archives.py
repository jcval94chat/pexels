import os
import json
import logging
import shutil
import zipfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.service_account import Credentials
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

GCP_CREDENTIALS_ENV = os.environ.get("GCP_CREDENTIALS")
VIDEOS_FOLDER_ID = os.environ.get("VIDEOS_FOLDER_ID")

if not GCP_CREDENTIALS_ENV:
    logger.error("La variable de entorno GCP_CREDENTIALS no está definida o está vacía.")
    exit(1)

if not VIDEOS_FOLDER_ID:
    logger.error("La variable de entorno VIDEOS_FOLDER_ID no está definida o está vacía.")
    exit(1)

def get_drive_service(creds_env):
    creds_info = json.loads(creds_env)
    creds = Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/drive"])
    service = build('drive', 'v3', credentials=creds)
    return service

def list_files_in_folder(service, folder_id):
    files_in_folder = []
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()
        for file in response.get('files', []):
            files_in_folder.append({'id': file['id'], 'name': file['name']})
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return files_in_folder

def create_folder(service, name, parent_id=None):
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]

    folder = service.files().create(body=file_metadata, fields='id').execute()
    logger.info(f"Carpeta '{name}' creada en Drive con ID: {folder.get('id')}")
    return folder.get('id')

def search_videos_by_keyword(service, folder_id, keyword):
    # Busca archivos en folder_id cuyo nombre contenga 'keyword' (insensible a mayúsculas).
    # Ajustar la consulta si se requiere otro criterio.
    # Para insensibilidad a mayúsculas, utilizamos la función LOWER en la consulta
    query = f"'{folder_id}' in parents and trashed=false and contains(name, '{keyword}')"
    result = service.files().list(q=query, fields="files(id, name)").execute()
    files = result.get('files', [])
    return files

def copy_file(service, file_id, new_name, parent_id):
    body = {
        'name': new_name,
        'parents': [parent_id]
    }
    copied_file = service.files().copy(fileId=file_id, body=body, fields='id').execute()
    logger.info(f"Archivo copiado con ID: {copied_file['id']} a la carpeta {parent_id}")
    return copied_file['id']

def download_file(service, file_id, destination_path):
    request = service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            logger.info(f"Descargando {destination_path}: {int(status.progress() * 100)}%")
    with open(destination_path, 'wb') as f:
        f.write(fh.getbuffer())
    logger.info(f"Archivo descargado en {destination_path}")

def upload_file(service, file_path, parent_id):
    file_name = os.path.basename(file_path)
    file_metadata = {
        'name': file_name,
        'parents': [parent_id]
    }
    media = MediaFileUpload(file_path, resumable=True)
    uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    logger.info(f"Archivo {file_name} subido a Drive con ID: {uploaded_file.get('id')}")

def zip_folder(folder_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, folder_path)
                zipf.write(abs_path, rel_path)
    logger.info(f"Carpeta {folder_path} comprimida en {zip_path}")

def main():
    # Cargar keywords_dict.json
    KEYWORDS_DICT_FILE = 'keywords_dict.json'
    if not os.path.exists(KEYWORDS_DICT_FILE):
        logger.error("No se encontró keywords_dict.json.")
        return

    with open(KEYWORDS_DICT_FILE, 'r', encoding='utf-8') as f:
        keywords_dict = json.load(f)

    if not keywords_dict:
        logger.info("keywords_dict.json está vacío. No hay acciones a realizar.")
        return

    # Obtener el último key del diccionario
    last_key = list(keywords_dict.keys())[-1]
    last_word_list = keywords_dict[last_key]
    logger.info(f"Procesando el último key: '{last_key}' con palabras clave: {last_word_list}")

    service = get_drive_service(GCP_CREDENTIALS_ENV)

    # Crear carpeta en Drive con el nombre del último key
    doc_folder_id = create_folder(service, last_key)

    # Por cada palabra en last_word_list, buscar hasta 4 videos que la contengan en su nombre y copiarlos
    for w in last_word_list:
        logger.info(f"Buscando videos con la palabra clave: '{w}'")
        found_videos = search_videos_by_keyword(service, VIDEOS_FOLDER_ID, w)
        # Tomar hasta 4
        videos_to_copy = found_videos[:4]
        if not videos_to_copy:
            logger.info(f"No se encontraron videos para la palabra clave: '{w}'")
            continue
        for vid in videos_to_copy:
            copy_file(service, vid['id'], vid['name'], doc_folder_id)

    # Descargar los archivos de la carpeta recién creada
    doc_files = list_files_in_folder(service, doc_folder_id)
    if not doc_files:
        logger.info(f"No hay archivos en la carpeta '{last_key}' para comprimir.")
        return

    temp_base = './temp_archive'
    if os.path.exists(temp_base):
        shutil.rmtree(temp_base)
    os.makedirs(temp_base, exist_ok=True)

    doc_local_folder = os.path.join(temp_base, last_key)
    os.makedirs(doc_local_folder, exist_ok=True)

    for fobj in doc_files:
        local_file_path = os.path.join(doc_local_folder, fobj['name'])
        download_file(service, fobj['id'], local_file_path)

    # Crear zip local
    zip_path = os.path.join(temp_base, f"{last_key}.zip")
    zip_folder(doc_local_folder, zip_path)

    # Subir el zip a la carpeta doc_folder_id
    upload_file(service, zip_path, doc_folder_id)

    # Limpiar archivos locales
    shutil.rmtree(doc_local_folder)
    if os.path.exists(zip_path):
        os.remove(zip_path)
    shutil.rmtree(temp_base)

    logger.info(f"Archivo ZIP para '{last_key}' creado y subido exitosamente.")

if __name__ == "__main__":
    main()
