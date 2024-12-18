import os
import json
import logging
import shutil
import zipfile
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.service_account import Credentials
from io import BytesIO

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Variables de entorno
GCP_CREDENTIALS_ENV = os.environ.get("GCP_CREDENTIALS")
VIDEOS_FOLDER_ID = os.environ.get("VIDEOS_FOLDER_ID")

if not GCP_CREDENTIALS_ENV:
    logger.error("La variable de entorno GCP_CREDENTIALS no está definida o está vacía.")
    exit(1)

if not VIDEOS_FOLDER_ID:
    logger.error("La variable de entorno VIDEOS_FOLDER_ID no está definida o está vacía.")
    exit(1)

def get_drive_service(creds_env):
    """
    Inicializa y retorna el servicio de Google Drive.
    """
    try:
        creds_info = json.loads(creds_env)
        creds = Credentials.from_service_account_info(
            creds_info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build('drive', 'v3', credentials=creds)
        logger.info("Servicio de Google Drive inicializado correctamente.")
        return service
    except Exception as e:
        logger.error(f"Error al inicializar el servicio de Google Drive: {e}")
        exit(1)

def list_files_in_folder(service, folder_id):
    """
    Lista todos los archivos en una carpeta de Drive.
    """
    files_in_folder = []
    page_token = None
    try:
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
        logger.info(f"Encontrados {len(files_in_folder)} archivos en la carpeta ID: {folder_id}.")
        return files_in_folder
    except Exception as e:
        logger.error(f"Error al listar archivos en la carpeta {folder_id}: {e}")
        return []

def search_videos_by_keyword(service, folder_id, keyword, max_results=4):
    """
    Busca archivos en Drive cuyo nombre contenga la palabra clave.
    Retorna hasta max_results archivos.
    """
    try:
        query = f"'{folder_id}' in parents and trashed=false and name contains '{keyword}'"
        result = service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=max_results
        ).execute()
        files = result.get('files', [])
        logger.info(f"Encontrados {len(files)} videos para la palabra clave: '{keyword}'.")
        return files
    except Exception as e:
        logger.error(f"Error al buscar videos con la palabra clave '{keyword}': {e}")
        return []

def download_file(service, file_id, destination_path):
    """
    Descarga un archivo de Drive a una ruta local.
    """
    try:
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
    except Exception as e:
        logger.error(f"Error al descargar el archivo {file_id}: {e}")

def upload_file(service, file_path, parent_id):
    """
    Sube un archivo local a una carpeta de Drive.
    """
    try:
        file_name = os.path.basename(file_path)
        file_metadata = {
            'name': file_name,
            'parents': [parent_id]
        }
        media = MediaFileUpload(file_path, resumable=True)
        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logger.info(f"Archivo {file_name} subido a Drive con ID: {uploaded_file.get('id')}")
    except Exception as e:
        logger.error(f"Error al subir el archivo {file_path} a Drive: {e}")

def zip_folder(folder_path, zip_path):
    """
    Comprime una carpeta en un archivo ZIP.
    """
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    abs_path = os.path.join(root, file)
                    rel_path = os.path.relpath(abs_path, folder_path)
                    zipf.write(abs_path, rel_path)
        logger.info(f"Carpeta {folder_path} comprimida en {zip_path}")
    except Exception as e:
        logger.error(f"Error al comprimir la carpeta {folder_path}: {e}")

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

    # Directorio temporal local
    temp_base = './temp_archive'
    if os.path.exists(temp_base):
        shutil.rmtree(temp_base)
    os.makedirs(temp_base, exist_ok=True)

    # Crear carpeta temporal para el último key
    doc_local_folder = os.path.join(temp_base, last_key)
    os.makedirs(doc_local_folder, exist_ok=True)
    logger.info(f"Carpeta temporal creada en {doc_local_folder}")

    # Por cada palabra en last_word_list, buscar y descargar hasta 4 videos
    for keyword in last_word_list:
        logger.info(f"Buscando videos con la palabra clave: '{keyword}'")
        found_videos = search_videos_by_keyword(service, VIDEOS_FOLDER_ID, keyword)
        if not found_videos:
            logger.info(f"No se encontraron videos para la palabra clave: '{keyword}'")
            continue
        for vid in found_videos:
            file_id = vid['id']
            file_name = vid['name']
            destination_path = os.path.join(doc_local_folder, file_name)
            logger.info(f"Descargando video: {file_name}")
            download_file(service, file_id, destination_path)

    # Verificar si se descargaron videos
    if not os.listdir(doc_local_folder):
        logger.info(f"No se descargaron videos para el key: '{last_key}'. No se creará un archivo ZIP.")
        shutil.rmtree(doc_local_folder)
        shutil.rmtree(temp_base)
        return

    # Crear archivo ZIP de la carpeta temporal
    zip_path = os.path.join(temp_base, f"{last_key}.zip")
    logger.info(f"Creando archivo ZIP: {zip_path}")
    zip_folder(doc_local_folder, zip_path)

    # Subir el archivo ZIP a VIDEOS_FOLDER_ID
    logger.info(f"Subiendo el archivo ZIP a Drive en la carpeta ID: {VIDEOS_FOLDER_ID}")
    upload_file(service, zip_path, VIDEOS_FOLDER_ID)

    # Limpieza: eliminar la carpeta temporal y el ZIP local
    shutil.rmtree(doc_local_folder)
    if os.path.exists(zip_path):
        os.remove(zip_path)
    shutil.rmtree(temp_base)
    logger.info(f"Proceso completado para el key: '{last_key}'")

if __name__ == "__main__":
    main()
