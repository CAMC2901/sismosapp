"""
Pruebas de Seguridad, RBAC (401 / 403), Validador de SQL Seguro y Agente (Sección 16).
"""
import pytest
from fastapi.testclient import TestClient

from app.agent.orchestrator import (
    _compute_cache_key,
    normalize_question,
    verify_numbers_in_response,
)
from app.agent.sql_validator import validate_safe_sql
from app.auth.security import create_access_token, hash_password, verify_password
from app.core.exceptions import UnsafeQueryError
from app.main import app
from app.models.schemas import UserInfo

client = TestClient(app)


def test_unauthenticated_request_receives_401():
    """
    Prueba 4a (Obligatoria): Cualquier llamada sin token Bearer a un endpoint de datos
    recibe HTTP 401 Unauthorized con formato JSON uniforme.
    """
    for endpoint in ("/api/kpis", "/api/events", "/api/reports", "/api/quality/quarantine"):
        resp = client.get(endpoint)
        assert resp.status_code == 401, f"Se esperaba 401 en {endpoint}, llegó {resp.status_code}"
        body = resp.json()
        assert body["status"] == 401
        assert body["error"] == "UNAUTHORIZED"


def test_viewer_on_admin_route_receives_403():
    """
    Prueba 4b (Obligatoria): Un usuario autenticado con rol `viewer` que intenta acceder
    a una ruta exclusiva de `admin` recibe HTTP 403 Forbidden con mensaje claro.
    """
    viewer_token = create_access_token(UserInfo(username="viewer_demo", role="viewer"))
    headers = {"Authorization": f"Bearer {viewer_token}"}

    resp_quarantine = client.get("/api/quality/quarantine", headers=headers)
    assert resp_quarantine.status_code == 403
    body_q = resp_quarantine.json()
    assert body_q["status"] == 403
    assert body_q["error"] == "FORBIDDEN"
    assert "admin" in body_q["message"].lower()

    resp_backfill = client.post(
        "/api/admin/backfill",
        headers=headers,
        json={"days": 30, "minmagnitude": 2.5},
    )
    assert resp_backfill.status_code == 403

    resp_regen = client.post("/api/admin/reports/regenerate", headers=headers)
    assert resp_regen.status_code == 403


def test_bcrypt_password_hashing_and_verification():
    """Verifica el hashing bcrypt y validación de contraseñas."""
    hashed = hash_password("SuperSismoSecret2026!")
    assert hashed.startswith("$2b$") or hashed.startswith("$2a$")
    assert verify_password("SuperSismoSecret2026!", hashed) is True
    assert verify_password("ClaveIncorrecta", hashed) is False


def test_safe_sql_validator_blocks_unsafe_queries():
    """
    Extra: Verifica que `validate_safe_sql` solo permita SELECT sobre tablas públicas
    de `curated` con `LIMIT` obligatorio y bloquee DDL/DML, comentarios, `;` y `raw`/`staging`/`users`.
    """
    valid_sql = "SELECT id, mag, region FROM curated.events WHERE mag >= 5.0 ORDER BY mag DESC LIMIT 10;"
    normalized = validate_safe_sql(valid_sql)
    assert normalized.startswith("SELECT")
    assert "LIMIT 10" in normalized

    # 1. Bloquear DDL/DML
    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("DROP TABLE curated.events LIMIT 1")

    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("DELETE FROM curated.events WHERE id = '1' LIMIT 1")

    # 2. Bloquear múltiples sentencias
    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("SELECT id FROM curated.events LIMIT 1; SELECT * FROM curated.users LIMIT 1")

    # 3. Bloquear comentarios sospechosos
    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("SELECT id FROM curated.events -- comentario malicioso\n LIMIT 5")

    # 4. Bloquear acceso a raw, staging o curated.users
    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("SELECT * FROM raw.ingestions LIMIT 5")

    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("SELECT * FROM staging.events_silver LIMIT 5")

    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("SELECT username, password_hash FROM curated.users LIMIT 5")

    # 5. Bloquear falta de LIMIT
    with pytest.raises(UnsafeQueryError):
        validate_safe_sql("SELECT id, mag FROM curated.events")


def test_agent_cache_key_and_number_verification():
    """Extra: Verifica normalización de preguntas, clave de caché con data_version y validación numérica."""
    q1 = normalize_question("¿Cuáles son los KPIs de las últimas 24h?")
    q2 = normalize_question("cuales son los kpis de las ultimas 24h")
    assert q1 == q2

    # Si cambia data_version o el rol, el hash de caché cambia automáticamente
    k_v1 = _compute_cache_key(q1, "20261027T120000Z", role="viewer")
    k_v2 = _compute_cache_key(q1, "20261027T120500Z", role="viewer")
    k_admin = _compute_cache_key(q1, "20261027T120000Z", role="admin")
    assert k_v1 != k_v2
    assert k_v1 != k_admin

    assert verify_numbers_in_response("Se detectaron 25 sismos con magnitud máxima 5.9", [25.0, 5.9]) is True
    assert verify_numbers_in_response("Se detectaron 80 sismos con magnitud máxima 5.9", [25.0, 5.9]) is False


