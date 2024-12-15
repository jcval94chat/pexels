from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import os

def get_latest_doc_words(drive_folder_id, cred_path):
    creds = service_account.Credentials.from_service_account_file(cred_path, scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents.readonly'])
    drive_service = build('drive', 'v3', credentials=creds)
    
    results = drive_service.files().list(
        q=f"'{drive_folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
        orderBy='modifiedTime desc',
        pageSize=1,
        fields="files(id, name)"
    ).execute()
    
    files = results.get('files', [])
    if not files:
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

    words = full_text.strip().split()
    last_10_words = words[-10:] if len(words) >= 10 else words
    return doc_name, last_10_words

def upload_files_to_drive(local_path, drive_folder_id, cred_path):
    creds = service_account.Credentials.from_service_account_file(cred_path, scopes=['https://www.googleapis.com/auth/drive'])
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