"""
Autenticación JWT con expiración, hashing bcrypt y dependencias por rol (Sección 12).

Reglas de seguridad:
1. Sin token válido: 401 en todo endpoint de datos.
2. Rol `viewer` en una ruta de `admin`: 403 con mensaje claro en español.
3. El ocultamiento de controles en el frontend NO sustituye esta verificación en el backend.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional
import bcrypt
from fastapi import Depends, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt
from app.core.config import get_db_conn, get_settings
from app.core.exceptions import AuthenticationError, PermissionDeniedError
from app.models.schemas import UserInfo

bearer_scheme = HTTPBearer(auto_error=False)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica la contraseña en texto plano contra el hash bcrypt de `curated.users`."""
    try:
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed_password.encode("utf-8"),
        )
    except ValueError:
        return False


def hash_password(plain_password: str) -> str:
    """Genera un hash bcrypt con factor de costo 12."""
    return bcrypt.hashpw(plain_password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def authenticate_user_from_db(username: str, password: str) -> UserInfo:
    """Consulta `curated.users` con `reader_role` y valida el hash bcrypt."""
    conn = get_db_conn(role="reader")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT username, password_hash, rol, activo
                FROM curated.users
                WHERE username = %s;
                """,
                (username.strip(),),
            )
            row = cur.fetchone()
            if not row:
                raise AuthenticationError("Credenciales inválidas: usuario o contraseña incorrectos.")
            db_user, pwd_hash, rol, activo = row
            if not activo:
                raise AuthenticationError("La cuenta de usuario se encuentra deshabilitada.")
            if not verify_password(password, pwd_hash):
                raise AuthenticationError("Credenciales inválidas: usuario o contraseña incorrectos.")
            return UserInfo(username=db_user, role=rol)
    finally:
        conn.close()


def create_access_token(user: UserInfo) -> str:
    """Firma un token JWT HS256 con `sub` (username), `role` y `exp`."""
    s = get_settings()
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=s.jwt_expire_minutes)
    payload = {
        "sub": user.username,
        "role": user.role,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }
    return jwt.encode(payload, s.jwt_secret, algorithm=s.jwt_algorithm)


def decode_access_token(token: str) -> UserInfo:
    """Decodifica y valida la firma y expiración del JWT."""
    s = get_settings()
    try:
        payload = jwt.decode(token, s.jwt_secret, algorithms=[s.jwt_algorithm])
        username = payload.get("sub")
        role = payload.get("role")
        if not username or role not in ("viewer", "admin"):
            raise AuthenticationError("Token JWT inválido: claims incompletos.")
        return UserInfo(username=str(username), role=role)
    except jwt.ExpiredSignatureError as exc:
        raise AuthenticationError("La sesión ha expirado. Por favor inicie sesión nuevamente.") from exc
    except jwt.InvalidTokenError as exc:
        raise AuthenticationError("Token de autenticación inválido o corrupto.") from exc


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    token_query: Optional[str] = Query(default=None, alias="token"),
) -> UserInfo:
    """
    Extrae el JWT del encabezado `Authorization: Bearer <token>` (o query param `?token=`
    para conexiones EventSource SSE del navegador) y verifica su validez.
    """
    raw_token: Optional[str] = None
    if credentials and credentials.scheme.lower() == "bearer":
        raw_token = credentials.credentials
    elif token_query and request.url.path.endswith("/stream"):
        raw_token = token_query

    if not raw_token:
        raise AuthenticationError("Autenticación requerida: no se proporcionó un token Bearer válido.")

    return decode_access_token(raw_token)


async def require_viewer(user: UserInfo = Depends(get_current_user)) -> UserInfo:
    """Dependencia para rutas accesibles por `viewer` y `admin`."""
    if user.role not in ("viewer", "admin"):
        raise PermissionDeniedError("Rol no autorizado para consultar este recurso.")
    return user


async def require_admin(user: UserInfo = Depends(get_current_user)) -> UserInfo:
    """
    Dependencia estricta para rutas exclusivas de `admin`.
    Si un `viewer` intenta invocar la ruta, devuelve HTTP 403 con explicación clara.
    """
    if user.role != "admin":
        raise PermissionDeniedError(
            f"Acceso denegado: el usuario '{user.username}' tiene rol '{user.role}', "
            "pero esta operación es exclusiva para el rol 'admin'."
        )
    return user

