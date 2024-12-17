from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import os
import io
import json
import logging
import traceback
import string
import unicodedata
import re

logger = logging.getLogger()

logger = logging.getLogger()

def get_drive_service(creds_env):
    creds_dict = json.loads(creds_env)
    # MODIFICACIÓN: Ajustar uso a service_account.Credentials (no se había definido Credentials antes)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, 
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build('drive', 'v3', credentials=creds)
    return service


def upload_files_to_drive(local_path, drive_folder_id, creds_env):
    try:
        creds_info = json.loads(creds_env)
        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        drive_service = build('drive', 'v3', credentials=creds)

        for file_name in os.listdir(local_path):
            file_path = os.path.join(local_path, file_name)
            if os.path.isfile(file_path):
                media = MediaFileUpload(file_path, resumable=True)
                file_metadata = {
                    'name': file_name,
                    'parents': [drive_folder_id]
                }
                drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                logger.info(f"Subido {file_name} a Google Drive.")
    except Exception as e:
        logger.error(f"Error al subir archivos a Drive: {e}")
        traceback.print_exc()
        raise e

# MODIFICACIÓN: Nueva función para listar archivos en la carpeta de Drive
def list_files_in_folder(folder_id, creds_env):
    logger.info(f"Listando archivos en la carpeta de Drive con ID: {folder_id}")
    creds_info = json.loads(creds_env)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, 
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    drive_service = build('drive', 'v3', credentials=creds)

    files_in_folder = []
    page_token = None
    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()
        for file in response.get('files', []):
            files_in_folder.append(file['name'])
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    logger.info(f"Archivos encontrados en la carpeta: {files_in_folder}")
    return set(files_in_folder)



def clean_and_convert_words(word_list):
    """
    Esta función limpia cada palabra eliminando:
    - Signos de puntuación
    - Caracteres especiales
    - Acentuación (normalizando a ASCII)
    - Dejando solo caracteres alfanuméricos

    Posteriormente convierte todo a mayúsculas.
    """
    cleaned = []
    for w in word_list:
        # Normalizar la palabra para separar acentos del caracter base
        w_norm = unicodedata.normalize('NFD', w)
        # Eliminar caracteres con la categoría "Mn" (acentos)
        w_no_accents = ''.join(ch for ch in w_norm if unicodedata.category(ch) != 'Mn')
        # Convertir a mayúsculas
        w_upper = w_no_accents.upper()
        # Remover cualquier carácter que no sea alfanumérico o espacio
        w_alnum = re.sub(r'[^A-Z0-9 ]', '', w_upper)
        # Eliminar espacios extras
        w_stripped = w_alnum.strip()
        
        if w_stripped:
            cleaned.append(w_stripped)
    return cleaned

def get_key_words(full_text):
    """
    Esta función recibe el texto completo de un documento.
    - Si encuentra una línea con la palabra "KEYWORDS" (en mayúsculas, minúsculas o combinaciones),
      retornará todas las palabras que aparecen después de esa línea.
    - Si no encuentra "KEYWORDS", retorna las últimas 10 palabras del texto completo.
    
    En ambos casos:
    - Las palabras se retornan en mayúsculas
    - Sin signos de puntuación ni acentos
    """
    text_upper = full_text.upper()
    lines = text_upper.split('\n')
    
    keywords_line_index = None
    for i, line in enumerate(lines):
        if line.strip() == "KEYWORDS":
            keywords_line_index = i
            break

    if keywords_line_index is not None:
        # Tomar todas las palabras después de KEYWORDS
        after_keywords_lines = lines[keywords_line_index+1:]
        all_after_keywords_text = ' '.join(after_keywords_lines)
        words = all_after_keywords_text.split()
        return clean_and_convert_words(words)
    else:
        all_words = text_upper.split()
        last_10 = all_words[-10:] if len(all_words) >= 10 else all_words
        return clean_and_convert_words(last_10)

def get_latest_doc_words(drive_folder_id, creds_env):
    try:
        creds_info = json.loads(creds_env)
        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents.readonly']
        )
        drive_service = build('drive', 'v3', credentials=creds)

        results = drive_service.files().list(
            q=f"'{drive_folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
            orderBy='modifiedTime desc',
            pageSize=1,
            fields="files(id, name)"
        ).execute()

        files = results.get('files', [])
        if not files:
            logger.info("No se encontraron documentos en la carpeta de Drive.")
            return None, None

        latest_file = files[0]
        doc_id = latest_file['id']
        doc_name = latest_file['name']

        docs_service = build('docs', 'v1', credentials=creds)
        doc = docs_service.documents().get(documentId=doc_id).execute()

        full_text = ""
        for content in doc.get('body', {}).get('content', []):
            if 'paragraph' in content:
                elements = content['paragraph'].get('elements', [])
                for elem in elements:
                    if 'textRun' in elem:
                        full_text += elem['textRun'].get('content', '')

        last_10_words = get_key_words(full_text)
        
        return doc_name, last_10_words
    except Exception as e:
        logger.error(f"Error al obtener palabras del documento: {e}")
        return None, None




