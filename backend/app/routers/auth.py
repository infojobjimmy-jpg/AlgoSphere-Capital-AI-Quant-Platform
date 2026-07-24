from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.services.whop_auth import configured, create_session, current_member, validate_license

router = APIRouter()


class LicenseLogin(BaseModel):
    license_key: str = Field(min_length=3, max_length=200)


@router.get("/status")
async def status(request: Request) -> dict:
    member = current_member(request)
    return {"configured": configured(), "authenticated": member is not None, "member": member}


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


@router.post("/logout")
async def logout(response: Response) -> dict:
    response.delete_cookie("algosphere_session", path="/")
    return {"authenticated": False}
