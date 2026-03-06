"""Notification utilities."""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage


def send_email_notification(subject: str, body: str, logger) -> None:
    """Send a plain-text email using SMTP environment variables."""
    host = os.getenv("SMTP_HOST")
    from_addr = os.getenv("SMTP_FROM")
    to_addr = os.getenv("SMTP_TO")
    if not host or not from_addr or not to_addr:
        logger.warning(
            "SMTP settings missing (SMTP_HOST/SMTP_FROM/SMTP_TO); skip alert: %s",
            subject,
        )
        return
    port = int(os.getenv("SMTP_PORT", "25"))
    username = os.getenv("SMTP_USERNAME") or os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD")
    use_tls = os.getenv("SMTP_USE_TLS", "false").lower() in {"1", "true", "yes"}
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = from_addr
    message["To"] = to_addr
    message.set_content(body)
    try:
        with smtplib.SMTP(host, port) as server:
            if use_tls:
                server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(message)
    except Exception as exc:
        logger.error("Failed to send alert email: %s", exc)
