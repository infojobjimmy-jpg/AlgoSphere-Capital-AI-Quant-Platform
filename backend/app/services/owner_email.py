from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage

from app.config import settings


def delivery_configured() -> bool:
    return bool(
        settings.auth_session_secret
        and settings.owner_email
        and settings.smtp_host
        and settings.smtp_username
        and settings.smtp_password
    )


def masked_owner_email() -> str:
    value = (settings.owner_email or "").strip()
    if "@" not in value:
        return ""
    local, domain = value.split("@", 1)
    if len(local) <= 2:
        masked_local = local[:1] + "*"
    else:
        masked_local = local[:2] + "*" * max(2, len(local) - 2)
    return f"{masked_local}@{domain}"


def _send_owner_code_sync(code: str, expires_minutes: int) -> None:
    if not delivery_configured():
        raise RuntimeError("Owner email delivery is not configured")

    sender = settings.smtp_from_email or settings.smtp_username or ""
    message = EmailMessage()
    message["Subject"] = f"{settings.product_name} — Code de connexion"
    message["From"] = sender
    message["To"] = settings.owner_email
    message.set_content(
        "Voici votre code temporaire pour ouvrir l’accès propriétaire AlgoSphere:\n\n"
        f"{code}\n\n"
        f"Ce code expire dans {expires_minutes} minutes.\n"
        "Si vous n’avez pas demandé ce code, ignorez ce message."
    )

    smtp_cls = smtplib.SMTP_SSL if settings.smtp_use_ssl else smtplib.SMTP
    with smtp_cls(settings.smtp_host, settings.smtp_port, timeout=20) as client:
        client.ehlo()
        if settings.smtp_starttls and not settings.smtp_use_ssl:
            client.starttls()
            client.ehlo()
        client.login(settings.smtp_username or "", settings.smtp_password or "")
        client.send_message(message)


async def send_owner_code(code: str, expires_minutes: int) -> None:
    await asyncio.to_thread(_send_owner_code_sync, code, expires_minutes)
