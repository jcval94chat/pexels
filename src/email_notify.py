import os
import smtplib
from email.mime.text import MIMEText
import logging

logger = logging.getLogger()

def send_email(to_email, subject, body):
    sender_email = os.environ.get("GMAIL_USER")
    password = os.environ.get("GMAIL_APP_PASSWORD")

    if not sender_email or not password:
        logger.error("Credenciales de Gmail no disponibles en las variables de entorno.")
        return

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = to_email

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.send_message(msg)
        logger.info(f"Correo enviado a {to_email} con asunto '{subject}'.")
    except Exception as e:
        logger.error(f"Error enviando correo: {e}")
