# Repositorio de Automatización de Descargas de Videos

Este repositorio contiene scripts para:
- Extraer las últimas 10 palabras de un documento reciente en Google Drive.
- Filtrar nuevas palabras clave y descargar videos relacionados con ellas desde Pexels.
- Subir los videos a Google Drive o, en caso de problemas, notificar por correo electrónico.

## Archivos Principales

- `src/main.py`: Script principal que ejecuta toda la lógica.
- `src/google_drive.py`: Funciones para interactuar con Google Drive y Google Docs.
- `src/email_notify.py`: Función auxiliar para enviar correos.
- `keywords_dict.json`: Diccionario con las palabras clave extraídas históricamente.
- `used_keywords.txt`: Historial de palabras clave ya utilizadas.

## Requisitos

- Python 3.10+
- Credenciales para Google Drive y Docs (service account).
- API Key para Pexels.
- Variables de entorno para Gmail y su App Password.

## Ejecución

El flujo se ejecuta automáticamente con GitHub Actions. También puede ser disparado manualmente desde la pestaña "Actions" del repositorio en GitHub.
