"""Notification utilities."""
from __future__ import annotations

import smtplib
from email.message import EmailMessage
from typing import Optional

from config import KalshiSettings


def send_email_notification(
    subject: str,
    body: str,
    logger,
    settings: Optional[KalshiSettings] = None,
) -> None:
    """Send a plain-text email using SMTP configuration from KalshiSettings."""
    cfg = settings or KalshiSettings()

    host = cfg.smtp_host
    from_addr = cfg.smtp_from
    to_addr = cfg.smtp_to
    if not host or not from_addr or not to_addr:
        logger.warning(
            "SMTP settings missing (SMTP_HOST/SMTP_FROM/SMTP_TO); skip alert: %s",
            subject,
        )
        return

    port = cfg.smtp_port
    username = cfg.smtp_username
    password = cfg.smtp_password
    use_tls = cfg.smtp_use_tls

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
