import os
import smtplib
from email.mime.text import MIMEText

def send_email(to_email, subject, body):
    try:
        sender_email = os.environ.get("GMAIL_USER")  # Tu usuario Gmail desde Secrets
        password = os.environ.get("GMAIL_APP_PASSWORD") # Tu app password desde Secrets
    
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = to_email
    
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.send_message(msg)
    except:
        pass
