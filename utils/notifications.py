"""Notification utilities."""
from __future__ import annotations

import smtplib
import time
from email.message import EmailMessage
from typing import Dict, Optional

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


_LAST_EMAIL_TS: Dict[str, float] = {}


def send_throttled_email(
    key: str,
    subject: str,
    body: str,
    logger,
    settings: Optional[KalshiSettings] = None,
    min_interval_seconds: float = 300.0,
) -> None:
    """
    Send an email with per-key throttling.

    - key: logical category of the alert (e.g. 'ws_closed', 'ws_auth_error',
      'scheduler:events_job:error').
    - min_interval_seconds: minimum interval between emails for this key.
    """
    now = time.time()
    last_ts = _LAST_EMAIL_TS.get(key, 0.0)
    if now - last_ts < min_interval_seconds:
        logger.info("Skip email for key=%s (throttled): %s", key, subject)
        return

    _LAST_EMAIL_TS[key] = now
    send_email_notification(subject, body, logger, settings=settings)
