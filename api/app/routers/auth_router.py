"""
Router de autenticación JWT (`/auth/login`).
"""
from fastapi import APIRouter, Depends
from app.auth.security import authenticate_user_from_db, create_access_token
from app.core.config import get_settings
from app.core.rate_limiter import rate_limit_dependency
from app.models.schemas import LoginRequest, TokenResponse

router = APIRouter(prefix="/auth", tags=["Autenticación"])


@router.post(
    "/login",
    response_model=TokenResponse,
    dependencies=[Depends(rate_limit_dependency("login", max_requests=25, window_seconds=60))],
)
async def login(payload: LoginRequest) -> TokenResponse:
    """Autentica al usuario contra `curated.users` con bcrypt y emite un token JWT."""
    s = get_settings()
    user = authenticate_user_from_db(payload.username, payload.password)
    token = create_access_token(user)
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in_minutes=s.jwt_expire_minutes,
        user=user,
    )

