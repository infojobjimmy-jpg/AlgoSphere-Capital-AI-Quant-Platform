from __future__ import annotations

import hashlib
import hmac
import secrets

import redis.asyncio as redis
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.config import settings
from app.services.owner_email import delivery_configured, masked_owner_email, send_owner_code
from app.services.whop_auth import configured, create_session, current_member, validate_license

router = APIRouter()


class LicenseLogin(BaseModel):
    license_key: str = Field(min_length=3, max_length=200)


class OwnerLogin(BaseModel):
    access_code: str = Field(min_length=12, max_length=200)


class OwnerOtpVerify(BaseModel):
    code: str = Field(min_length=6, max_length=6, pattern=r"^\d{6}$")


class OwnerCodeChange(BaseModel):
    current_code: str = Field(min_length=12, max_length=200)
    new_code: str = Field(min_length=16, max_length=200)


def _owner_digest(value: str) -> str:
    secret = settings.auth_session_secret or ""
    return hmac.new(secret.encode(), value.strip().encode(), hashlib.sha256).hexdigest()


def _client_key(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    host = forwarded.split(",", 1)[0].strip() or (request.client.host if request.client else "unknown")
    return hashlib.sha256(host.encode()).hexdigest()[:24]


async def _owner_code_matches(client: redis.Redis, supplied: str) -> bool:
    stored_digest = await client.get(f"{settings.app_slug}:owner:access_digest")
    if stored_digest:
        return secrets.compare_digest(_owner_digest(supplied), stored_digest)
    expected = settings.owner_access_code or ""
    return bool(expected) and secrets.compare_digest(supplied.strip(), expected)


def _owner_member() -> dict:
    return {
        "membership_id": "owner",
        "product_id": "algosphere-owner",
        "product_name": "AlgoSphere Global — Propriétaire",
        "status": "active",
        "user_id": "algosphere-owner",
        "username": settings.owner_email,
        "role": "owner",
        "expires_at": None,
        "manage_url": None,
    }


def _set_owner_session(response: Response) -> dict:
    owner = _owner_member()
    token = create_session(owner)
    response.set_cookie(
        "algosphere_session",
        token,
        max_age=int(settings.auth_session_hours) * 3600,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )
    return owner


@router.get("/status")
async def status(request: Request) -> dict:
    member = current_member(request)
    email_ready = delivery_configured()
    permanent_ready = bool(settings.owner_access_code and settings.auth_session_secret)
    return {
        "configured": configured(),
        "owner_access_configured": bool(email_ready or permanent_ready),
        "owner_email_delivery_configured": email_ready,
        "owner_permanent_code_configured": permanent_ready,
        "owner_email_hint": masked_owner_email() if email_ready else "",
        "authenticated": member is not None,
        "member": member,
    }


@router.get("/me")
async def me(request: Request) -> dict:
    member = current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Subscription required")
    return {"authenticated": True, "member": member}


@router.post("/license")
async def license_login(body: LicenseLogin, response: Response) -> dict:
    member = await validate_license(body.license_key)
    token = create_session(member)
    response.set_cookie(
        "algosphere_session",
        token,
        max_age=7 * 24 * 3600,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )
    return {"authenticated": True, "member": member}


@router.post("/owner/request-code")
async def request_owner_code(request: Request) -> dict:
    if not delivery_configured():
        raise HTTPException(status_code=503, detail="Automatic email delivery is not configured")

    client = redis.from_url(settings.redis_url, decode_responses=True)
    client_id = _client_key(request)
    request_key = f"{settings.app_slug}:owner:otp:request:{client_id}"
    global_request_key = f"{settings.app_slug}:owner:otp:request:global"
    digest_key = f"{settings.app_slug}:owner:otp:digest"
    attempts_key = f"{settings.app_slug}:owner:otp:attempts:{client_id}"
    cooldown = max(15, int(settings.owner_otp_request_cooldown_seconds))
    ttl = max(120, int(settings.owner_otp_ttl_seconds))

    try:
        request_ttl = await client.ttl(request_key)
        global_ttl = await client.ttl(global_request_key)
        active_ttl = max(request_ttl, global_ttl)
        if active_ttl > 0:
            raise HTTPException(
                status_code=429,
                detail=f"A code was already sent. Try again in {active_ttl} seconds.",
                headers={"Retry-After": str(active_ttl)},
            )

        code = f"{secrets.randbelow(1_000_000):06d}"
        await client.set(digest_key, _owner_digest(f"otp:{code}"), ex=ttl)
        await client.delete(attempts_key)
        await client.set(request_key, "1", ex=cooldown)
        await client.set(global_request_key, "1", ex=cooldown)

        try:
            await send_owner_code(code, max(1, ttl // 60))
        except Exception as exc:
            await client.delete(digest_key, request_key, global_request_key)
            raise HTTPException(status_code=502, detail="The login email could not be sent") from exc
    finally:
        await client.aclose()

    return {
        "sent": True,
        "expires_in": ttl,
        "email_hint": masked_owner_email(),
    }


@router.post("/owner/verify-code")
async def verify_owner_code(body: OwnerOtpVerify, request: Request, response: Response) -> dict:
    if not settings.auth_session_secret:
        raise HTTPException(status_code=503, detail="Owner access is not configured")

    client = redis.from_url(settings.redis_url, decode_responses=True)
    client_id = _client_key(request)
    digest_key = f"{settings.app_slug}:owner:otp:digest"
    attempts_key = f"{settings.app_slug}:owner:otp:attempts:{client_id}"

    try:
        attempts = int(await client.get(attempts_key) or 0)
        if attempts >= settings.owner_otp_max_attempts:
            ttl = max(1, await client.ttl(attempts_key))
            raise HTTPException(
                status_code=429,
                detail=f"Too many attempts. Try again in {ttl} seconds.",
                headers={"Retry-After": str(ttl)},
            )

        expected_digest = await client.get(digest_key)
        if not expected_digest:
            raise HTTPException(status_code=400, detail="The code has expired. Request a new one.")

        supplied_digest = _owner_digest(f"otp:{body.code.strip()}")
        if not secrets.compare_digest(supplied_digest, expected_digest):
            attempts = await client.incr(attempts_key)
            if attempts == 1:
                remaining = max(60, await client.ttl(digest_key))
                await client.expire(attempts_key, remaining)
            raise HTTPException(status_code=401, detail="Invalid temporary code")

        await client.delete(digest_key, attempts_key)
    finally:
        await client.aclose()

    owner = _set_owner_session(response)
    return {"authenticated": True, "member": owner}


@router.post("/owner")
async def owner_login(body: OwnerLogin, request: Request, response: Response) -> dict:
    if not settings.owner_access_code or not settings.auth_session_secret:
        raise HTTPException(status_code=503, detail="Owner access is not configured")
    client = redis.from_url(settings.redis_url, decode_responses=True)
    attempt_key = f"{settings.app_slug}:owner:login_attempts:{_client_key(request)}"
    try:
        attempts = int(await client.get(attempt_key) or 0)
        if attempts >= settings.owner_login_max_attempts:
            ttl = max(1, await client.ttl(attempt_key))
            raise HTTPException(
                status_code=429,
                detail=f"Too many attempts. Try again in {ttl} seconds.",
                headers={"Retry-After": str(ttl)},
            )
        if not await _owner_code_matches(client, body.access_code):
            attempts = await client.incr(attempt_key)
            if attempts == 1:
                await client.expire(attempt_key, settings.owner_login_window_seconds)
            raise HTTPException(status_code=401, detail="Invalid owner access code")
        await client.delete(attempt_key)
    finally:
        await client.aclose()

    owner = _set_owner_session(response)
    return {"authenticated": True, "member": owner}


@router.post("/owner/change-code")
async def change_owner_code(body: OwnerCodeChange, request: Request, response: Response) -> dict:
    member = current_member(request)
    if not member or member.get("role") != "owner":
        raise HTTPException(status_code=403, detail="Owner access required")
    if body.current_code.strip() == body.new_code.strip():
        raise HTTPException(status_code=400, detail="Choose a different owner code")
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        if not await _owner_code_matches(client, body.current_code):
            raise HTTPException(status_code=401, detail="Current owner code is invalid")
        await client.set(f"{settings.app_slug}:owner:access_digest", _owner_digest(body.new_code))
    finally:
        await client.aclose()
    response.delete_cookie("algosphere_session", path="/")
    return {"changed": True, "authenticated": False}


@router.post("/logout")
async def logout(response: Response) -> dict:
    response.delete_cookie("algosphere_session", path="/")
    return {"authenticated": False}
