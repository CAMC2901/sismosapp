"""
Interfaz común LLMClient con implementaciones Groq y Ollama, cascada de tolerancia a fallos,
verificación anti-alucinación de cifras y auditoría en curated.ai_runs (Sección 11).

Prioridad obligatoria:
1. Groq API (modelo por variable `GROQ_MODEL`, ej. `llama-3.3-70b-versatile`)
2. Ollama local (`qwen2.5:3b`)
3. Plantilla determinista en Python (`fallback=True`)
"""
from abc import ABC, abstractmethod
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Dict, List, Optional, Type
from pydantic import BaseModel, ValidationError
import requests
from common.config import get_config, get_db_connection
from common.exceptions import AIProviderError, AIValidationError
from common.logging_utils import get_structured_logger, log_event, redact_secrets

logger = get_structured_logger("sismowatch.ai.llm_client")

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
NUMBER_REGEX = re.compile(r"(?<![a-zA-Z0-9_])-?\d+(?:\.\d+)?(?![a-zA-Z0-9_])")


def load_versioned_prompt(prompt_name: str) -> str:
    """Carga un prompt versionado desde `airflow/include/prompts/<prompt_name>.md`."""
    path = PROMPTS_DIR / f"{prompt_name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Prompt versionado no encontrado: {path}")
    return path.read_text(encoding="utf-8").strip()


def extract_numbers_from_text(text: str) -> List[float]:
    """Extrae todos los valores numéricos presentes en un texto."""
    matches = NUMBER_REGEX.findall(text or "")
    out: List[float] = []
    for m in matches:
        try:
            out.append(float(m))
        except ValueError:
            continue
    return out


def verify_calculated_numbers(text: str, required_numbers: List[float], tolerance: float = 0.05) -> bool:
    """
    Verifica que todas las cifras clave calculadas por SQL/Python aparezcan en el
    texto redactado por el LLM dentro de la tolerancia numérica especificada.
    """
    extracted = extract_numbers_from_text(text)
    if not required_numbers:
        return True
    if not extracted:
        return False

    for expected in required_numbers:
        found = any(abs(actual - float(expected)) <= tolerance for actual in extracted)
        if not found:
            return False
    return True


def record_ai_run(
    tarea: str,
    modelo: str,
    proveedor: str,
    tokens_entrada: int,
    tokens_salida: int,
    latencia_ms: float,
    costo_estimado: float,
    exito: bool,
    fallback: bool,
    run_id: str = "system",
    error_msg: Optional[str] = None,
    db_role: str = "ai",
) -> None:
    """Registra la telemetría de cada invocación de IA en `curated.ai_runs`."""
    conn = None
    try:
        conn = get_db_connection(role=db_role)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO curated.ai_runs (
                        run_id, tarea, modelo, proveedor, tokens_entrada, tokens_salida,
                        latencia_ms, costo_estimado, exito, fallback, error_msg, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
                    """,
                    (
                        run_id,
                        tarea,
                        modelo,
                        proveedor,
                        int(tokens_entrada),
                        int(tokens_salida),
                        round(float(latencia_ms), 2),
                        round(float(costo_estimado), 6),
                        bool(exito),
                        bool(fallback),
                        redact_secrets(error_msg)[:300] if error_msg else None,
                    ),
                )
    except Exception as exc:
        log_event(logger, "warning", "record_ai_run_failed", run_id, {"error": str(exc)})
    finally:
        if conn is not None:
            conn.close()


class LLMClient(ABC):
    """Interfaz común para proveedores de modelos de lenguaje."""

    provider_name: str = "abstract"
    model_name: str = "abstract"

    @abstractmethod
    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = True,
        timeout_sec: float = 20.0,
    ) -> Dict[str, Any]:
        """
        Retorna `{"content": str, "tokens_in": int, "tokens_out": int, "latency_ms": float, "cost_usd": float}`.
        Lanza `AIProviderError` si ocurre un fallo.
        """


class GroqLLMClient(LLMClient):
    """Cliente hacia la API de Groq con hasta 2 reintentos y backoff exponencial."""

    provider_name = "groq"

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None) -> None:
        cfg = get_config()
        self.api_key = (api_key if api_key is not None else cfg.groq_api_key).strip()
        self.model_name = model or cfg.groq_model
        self.endpoint = "https://api.groq.com/openai/v1/chat/completions"

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = True,
        timeout_sec: float = 18.0,
    ) -> Dict[str, Any]:
        if not self.api_key or self.api_key.startswith("your_groq") or self.api_key.startswith("CHANGE_ME"):
            raise AIProviderError("GROQ_API_KEY no configurada; saltando a proveedor de respaldo.", provider="groq")

        max_attempts = 3  # 1 intento inicial + hasta 2 reintentos con backoff
        base_delay = 1.5

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        body: Dict[str, Any] = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 1024,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}

        for attempt in range(1, max_attempts + 1):
            t0 = time.perf_counter()
            try:
                resp = requests.post(
                    self.endpoint,
                    headers=headers,
                    json=body,
                    timeout=(5.0, timeout_sec),
                )
                latency_ms = (time.perf_counter() - t0) * 1000.0
                if resp.status_code == 200:
                    data = resp.json()
                    content = data["choices"][0]["message"]["content"]
                    usage = data.get("usage", {})
                    t_in = int(usage.get("prompt_tokens", len(user_prompt) // 4))
                    t_out = int(usage.get("completion_tokens", len(content) // 4))
                    # Costo estimado Llama 3.3 70B en Groq (~$0.59/1M in, $0.79/1M out)
                    cost_usd = (t_in * 0.59 + t_out * 0.79) / 1_000_000.0
                    return {
                        "content": content,
                        "tokens_in": t_in,
                        "tokens_out": t_out,
                        "latency_ms": latency_ms,
                        "cost_usd": cost_usd,
                    }

                is_transient = resp.status_code == 429 or resp.status_code >= 500
                err = AIProviderError(
                    f"Groq HTTP {resp.status_code}",
                    provider="groq",
                    is_transient=is_transient,
                )
                if not is_transient or attempt == max_attempts:
                    raise err
                time.sleep(base_delay * (2 ** (attempt - 1)))
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt == max_attempts:
                    raise AIProviderError(
                        f"Timeout/conexión en Groq tras {max_attempts} intentos: {exc}",
                        provider="groq",
                        is_transient=True,
                    ) from exc
                time.sleep(base_delay * (2 ** (attempt - 1)))

        raise AIProviderError("Fallo general en Groq.", provider="groq")


class OllamaLLMClient(LLMClient):
    """Cliente hacia Ollama local (`qwen2.5:3b`) con hasta 2 reintentos y backoff exponencial."""

    provider_name = "ollama"

    def __init__(self, base_url: Optional[str] = None, model: Optional[str] = None) -> None:
        cfg = get_config()
        self.base_url = (base_url or cfg.ollama_url).rstrip("/")
        self.model_name = model or cfg.ollama_fallback_model
        self.endpoint = f"{self.base_url}/api/chat"

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        json_mode: bool = True,
        timeout_sec: float = 25.0,
    ) -> Dict[str, Any]:
        max_attempts = 3
        base_delay = 1.5
        body: Dict[str, Any] = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 768},
        }
        if json_mode:
            body["format"] = "json"

        for attempt in range(1, max_attempts + 1):
            t0 = time.perf_counter()
            try:
                resp = requests.post(self.endpoint, json=body, timeout=(4.0, timeout_sec))
                latency_ms = (time.perf_counter() - t0) * 1000.0
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("message", {}).get("content", "")
                    t_in = int(data.get("prompt_eval_count") or (len(user_prompt) // 4))
                    t_out = int(data.get("eval_count") or (len(content) // 4))
                    return {
                        "content": content,
                        "tokens_in": t_in,
                        "tokens_out": t_out,
                        "latency_ms": latency_ms,
                        "cost_usd": 0.0,
                    }
                is_transient = resp.status_code == 429 or resp.status_code >= 500
                if not is_transient or attempt == max_attempts:
                    raise AIProviderError(
                        f"Ollama HTTP {resp.status_code}: {resp.text[:120]}",
                        provider="ollama",
                        is_transient=is_transient,
                    )
                time.sleep(base_delay * (2 ** (attempt - 1)))
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt == max_attempts:
                    raise AIProviderError(
                        f"Timeout/conexión hacia Ollama ({self.model_name}): {exc}",
                        provider="ollama",
                        is_transient=True,
                    ) from exc
                time.sleep(base_delay * (2 ** (attempt - 1)))

        raise AIProviderError("Fallo general en Ollama.", provider="ollama")


def _extract_json_dict(raw_text: str) -> Dict[str, Any]:
    """Extrae y parsea un objeto JSON desde la respuesta del LLM."""
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    # Buscar el primer bloque {...}
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            parsed = json.loads(cleaned[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError as exc:
            raise AIValidationError(f"JSON malformado en respuesta LLM: {exc}") from exc
    raise AIValidationError("No se encontró un objeto JSON válido en la respuesta del LLM.")


def execute_structured_llm_with_cascade(
    tarea: str,
    system_prompt: str,
    user_prompt: str,
    schema_cls: Type[BaseModel],
    required_numbers: List[float],
    template_fallback_fn: Callable[[], Dict[str, Any]],
    run_id: str = "system",
    db_role: str = "ai",
    timeout_sec: float = 20.0,
) -> Dict[str, Any]:
    """
    Ejecuta la cascada obligatoria Groq → Ollama (qwen2.5:3b) → Plantilla Python:
    1. Valida el esquema con Pydantic (`schema_cls`). Si el JSON es inválido, reintenta 1 vez con prompt de reparación.
    2. Verifica que las cifras obligatorias (`required_numbers`) coincidan con las calculadas por SQL/Python;
       si no coinciden, reintenta 1 vez y si persiste cae al siguiente proveedor / plantilla determinista.
    3. Registra cada intento en `curated.ai_runs`.
    """
    cfg = get_config()
    clients: List[LLMClient] = [GroqLLMClient(), OllamaLLMClient()]
    if cfg.llm_provider.lower() == "ollama":
        # Aun si se prefiere ollama en desarrollo sin API key de Groq, GroqLLMClient salta limpio si GROQ_API_KEY está vacía
        clients = [GroqLLMClient(), OllamaLLMClient()]

    repair_instructions = load_versioned_prompt("daily_report_repair_v1")

    for client in clients:
        try:
            for validation_attempt in range(1, 3):  # intento inicial + 1 reintento de reparación/cifras
                active_user_prompt = (
                    user_prompt
                    if validation_attempt == 1
                    else f"{user_prompt}\n\n[INSTRUCCIÓN DE REPARACIÓN]: {repair_instructions}"
                )
                raw_res = client.generate(
                    system_prompt=system_prompt,
                    user_prompt=active_user_prompt,
                    json_mode=True,
                    timeout_sec=timeout_sec,
                )
                try:
                    json_dict = _extract_json_dict(raw_res["content"])
                    validated_model = schema_cls.model_validate(json_dict)
                    serialized_str = json.dumps(validated_model.model_dump(), ensure_ascii=False)
                    if not verify_calculated_numbers(serialized_str, required_numbers):
                        raise AIValidationError(
                            f"Las cifras generadas por {client.provider_name} no coinciden con las cifras SQL {required_numbers}."
                        )

                    record_ai_run(
                        tarea=tarea,
                        modelo=client.model_name,
                        proveedor=client.provider_name,
                        tokens_entrada=raw_res["tokens_in"],
                        tokens_salida=raw_res["tokens_out"],
                        latencia_ms=raw_res["latency_ms"],
                        costo_estimado=raw_res["cost_usd"],
                        exito=True,
                        fallback=(client.provider_name != "groq"),
                        run_id=run_id,
                        db_role=db_role,
                    )
                    return {
                        "data": validated_model.model_dump(),
                        "provider": client.provider_name,
                        "model": client.model_name,
                        "fallback": (client.provider_name != "groq"),
                    }
                except (ValidationError, AIValidationError) as val_exc:
                    record_ai_run(
                        tarea=tarea,
                        modelo=client.model_name,
                        proveedor=client.provider_name,
                        tokens_entrada=raw_res["tokens_in"],
                        tokens_salida=raw_res["tokens_out"],
                        latencia_ms=raw_res["latency_ms"],
                        costo_estimado=raw_res["cost_usd"],
                        exito=False,
                        fallback=True,
                        run_id=run_id,
                        error_msg=f"Validación fallida (intento {validation_attempt}): {val_exc}",
                        db_role=db_role,
                    )
                    if validation_attempt == 2:
                        raise AIProviderError(
                            f"Proveedor {client.provider_name} falló validación tras reintento: {val_exc}",
                            provider=client.provider_name,
                        ) from val_exc
        except AIProviderError as prov_exc:
            if "GROQ_API_KEY no configurada" not in str(prov_exc):
                record_ai_run(
                    tarea=tarea,
                    modelo=client.model_name,
                    proveedor=client.provider_name,
                    tokens_entrada=0,
                    tokens_salida=0,
                    latencia_ms=0.0,
                    costo_estimado=0.0,
                    exito=False,
                    fallback=True,
                    run_id=run_id,
                    error_msg=str(prov_exc),
                    db_role=db_role,
                )
            log_event(
                logger,
                "warning",
                "llm_provider_fallback",
                run_id,
                {"provider": client.provider_name, "reason": str(prov_exc)},
            )

    # Nivel final de respaldo: plantilla determinista en Python (nunca falla el pipeline)
    t0 = time.perf_counter()
    fallback_dict = template_fallback_fn()
    validated_fallback = schema_cls.model_validate(fallback_dict).model_dump()
    latency_ms = (time.perf_counter() - t0) * 1000.0
    record_ai_run(
        tarea=tarea,
        modelo="python-deterministic-template-v1",
        proveedor="template_fallback",
        tokens_entrada=0,
        tokens_salida=0,
        latencia_ms=latency_ms,
        costo_estimado=0.0,
        exito=True,
        fallback=True,
        run_id=run_id,
        db_role=db_role,
    )
    return {
        "data": validated_fallback,
        "provider": "template_fallback",
        "model": "python-deterministic-template-v1",
        "fallback": True,
    }
