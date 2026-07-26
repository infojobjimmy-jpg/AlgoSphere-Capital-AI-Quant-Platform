from __future__ import annotations

import asyncio
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.config import settings

logger = logging.getLogger("acap.license_email")


def _build_message(to_email: str, product_name: str, license_key: str, manage_url: str) -> MIMEMultipart:
    from_addr = settings.smtp_from_email or settings.smtp_username or "noreply@algosphereglobal.com"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Bienvenue sur AlgoSphere — Votre accès {product_name} / Welcome to AlgoSphere"
    msg["From"] = from_addr
    msg["To"] = to_email

    plain = (
        f"Bienvenue sur AlgoSphere Global !\n\n"
        f"Votre abonnement {product_name} est maintenant actif.\n\n"
        f"Clé de licence : {license_key}\n\n"
        f"Pour accéder à la plateforme :\n"
        f"1. Ouvrez https://algosphereglobal.com/app\n"
        f"2. Cliquez sur « Accès membre »\n"
        f"3. Entrez votre clé de licence\n\n"
        f"Gérez votre abonnement : {manage_url}\n\n"
        f"---\n\n"
        f"Welcome to AlgoSphere Global!\n\n"
        f"Your {product_name} subscription is now active.\n\n"
        f"License key: {license_key}\n\n"
        f"To access the platform:\n"
        f"1. Open https://algosphereglobal.com/app\n"
        f"2. Click « Member access »\n"
        f"3. Enter your license key\n\n"
        f"Manage your subscription: {manage_url}\n"
    )

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="utf-8"><title>AlgoSphere — Votre accès</title></head>
<body style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 16px;color:#111">
  <h1 style="font-size:22px;margin:0 0 8px">Bienvenue sur AlgoSphere Global&nbsp;!</h1>
  <p style="color:#555;margin:0 0 24px">
    Votre abonnement <strong>{product_name}</strong> est maintenant actif.
  </p>
  <div style="background:#f4f4f4;border-radius:8px;padding:16px 20px;margin:0 0 24px">
    <p style="margin:0 0 4px;font-size:12px;color:#888;text-transform:uppercase;letter-spacing:.05em">Clé de licence</p>
    <p style="font-family:monospace;font-size:18px;font-weight:700;margin:0;letter-spacing:.05em;word-break:break-all">{license_key}</p>
    <p style="font-size:11px;color:#aaa;margin:8px 0 0">Conservez cette clé — elle est nécessaire pour vous connecter.</p>
  </div>
  <h2 style="font-size:15px;margin:0 0 12px">Comment accéder à la plateforme</h2>
  <ol style="padding-left:20px;color:#333;line-height:1.8">
    <li>Ouvrez <a href="https://algosphereglobal.com/app" style="color:#2563eb">algosphereglobal.com/app</a></li>
    <li>Cliquez sur <strong>« Accès membre »</strong></li>
    <li>Entrez votre clé de licence ci-dessus</li>
  </ol>
  <a href="https://algosphereglobal.com/app"
     style="display:inline-block;margin:20px 0;padding:12px 28px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-weight:600">
    Accéder à la plateforme
  </a>
  <hr style="border:none;border-top:1px solid #eee;margin:24px 0">
  <p style="font-size:13px;color:#555">
    <strong>Welcome to AlgoSphere Global!</strong><br>
    Your <strong>{product_name}</strong> subscription is now active.<br>
    License key: <code style="font-size:13px;word-break:break-all">{license_key}</code><br>
    <a href="https://algosphereglobal.com/app" style="color:#2563eb">algosphereglobal.com/app</a>
    → Member access → Enter key
  </p>
  <p style="font-size:12px;color:#aaa;margin:24px 0 0">
    <a href="{manage_url}" style="color:#aaa">Gérer mon abonnement Whop / Manage my Whop subscription</a>
  </p>
</body>
</html>"""

    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))
    return msg


def _send_sync(to_email: str, product_name: str, license_key: str, manage_url: str) -> None:
    msg = _build_message(to_email, product_name, license_key, manage_url)
    host = settings.smtp_host
    port = settings.smtp_port
    username = settings.smtp_username or ""
    password = settings.smtp_password or ""
    if settings.smtp_use_ssl:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx) as server:
            if username and password:
                server.login(username, password)
            server.sendmail(msg["From"], [to_email], msg.as_string())
    else:
        with smtplib.SMTP(host, port) as server:
            if settings.smtp_starttls:
                server.starttls()
            if username and password:
                server.login(username, password)
            server.sendmail(msg["From"], [to_email], msg.as_string())
    logger.info("license welcome email sent (recipient and key not logged)")


async def send_license_email(to_email: str, product_name: str, license_key: str, manage_url: str) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _send_sync, to_email, product_name, license_key, manage_url)
