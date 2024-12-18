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
    query = f"'{folder_id}' in parents and trashed=false and name contains '{keyword}'"
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
    while done is False:
        status, done = downloader.next_chunk()
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
    if not VIDEOS_FOLDER_ID:
        logger.error("La variable de entorno VIDEOS_FOLDER_ID no está definida.")
        return

    # Cargar keywords_dict.json
    KEYWORDS_DICT_FILE = 'keywords_dict.json'
    if not os.path.exists(KEYWORDS_DICT_FILE):
        logger.error("No se encontró keywords_dict.json.")
        return

    with open(KEYWORDS_DICT_FILE, 'r', encoding='utf-8') as f:
        keywords_dict = json.load(f)

    service = get_drive_service(GCP_CREDENTIALS_ENV)

    # Directorio temporal local
    temp_base = './temp_archives'
    if os.path.exists(temp_base):
        shutil.rmtree(temp_base)
    os.makedirs(temp_base, exist_ok=True)

    # Por cada key en keywords_dict, creamos una carpeta en Drive
    for doc_name, word_list in keywords_dict.items():
        # Crear carpeta en Drive con el nombre doc_name
        doc_folder_id = create_folder(service, doc_name)

        # Por cada palabra en word_list, buscamos hasta 4 videos que la contengan en su nombre
        # y los copiamos a la carpeta doc_folder_id
        for w in word_list:
            # Buscar archivos en VIDEOS_FOLDER_ID que contengan w
            found_videos = search_videos_by_keyword(service, VIDEOS_FOLDER_ID, w)
            # Tomar hasta 4
            videos_to_copy = found_videos[:4]
            for vid in videos_to_copy:
                # Copiar archivo
                copy_file(service, vid['id'], vid['name'], doc_folder_id)

        # Una vez copiados, descargamos el contenido de la carpeta doc_folder_id, lo comprimimos
        # y subimos el zip a la misma carpeta.
        doc_local_folder = os.path.join(temp_base, doc_name)
        os.makedirs(doc_local_folder, exist_ok=True)

        # Listar archivos en la carpeta doc_folder_id
        doc_files = list_files_in_folder(service, doc_folder_id)
        # Descargar todos los archivos (asumiendo que son videos)
        for fobj in doc_files:
            local_file_path = os.path.join(doc_local_folder, fobj['name'])
            download_file(service, fobj['id'], local_file_path)

        # Crear zip local
        zip_path = os.path.join(temp_base, f"{doc_name}.zip")
        zip_folder(doc_local_folder, zip_path)

        # Subir el zip a la carpeta doc_folder_id
        upload_file(service, zip_path, doc_folder_id)

        # Opcional: limpiar archivos locales de esa carpeta
        shutil.rmtree(doc_local_folder)
        if os.path.exists(zip_path):
            os.remove(zip_path)

    # Opcional: limpiar el directorio temporal base
    shutil.rmtree(temp_base)

if __name__ == "__main__":
    main()
