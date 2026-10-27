"""
Sanitización de texto externo y mitigación de Prompt Injection (OWASP LLM Top 10).

Por qué es necesario:
Los campos de texto provenientes de fuentes externas (como `properties.place` en USGS)
se persisten en base de datos y se incluyen como contexto en prompts de IA. Para evitar
inyección de instrucciones, caracteres de control o desbordamiento de tokens, se truncan,
limpian de secuencias maliciosas y delimitan explícitamente como datos no ejecutables.
"""
import re
from typing import Optional

CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")
INJECTION_PATTERNS_RE = re.compile(
    r"(ignore\s+(all\s+)?(previous|prior)\s+instructions|"
    r"ignora\s+(todas\s+las\s+)?instrucciones|"
    r"system\s*prompt|you\s+are\s+now|olvida\s+todo|"
    r"<\s*/?\s*(script|system|instruction|prompt)[^>]*>)",
    re.IGNORECASE,
)
MULTI_SPACE_RE = re.compile(r"\s+")

MAX_PLACE_LENGTH = 180


def sanitize_place(raw_place: Optional[str]) -> str:
    """
    Limpia el campo `place` de caracteres de control, patrones de prompt injection
    y limita su longitud a MAX_PLACE_LENGTH caracteres.
    """
    if not raw_place or not str(raw_place).strip():
        return "Ubicación desconocida"

    cleaned = str(raw_place)
    cleaned = CONTROL_CHARS_RE.sub(" ", cleaned)
    cleaned = INJECTION_PATTERNS_RE.sub("[FILTRADO]", cleaned)
    cleaned = cleaned.replace("<", "(").replace(">", ")").replace("{", "(").replace("}", ")")
    cleaned = MULTI_SPACE_RE.sub(" ", cleaned).strip()
    if not cleaned:
        return "Ubicación desconocida"
    return cleaned[:MAX_PLACE_LENGTH]


def derive_region(sanitized_place: Optional[str]) -> str:
    """
    Deriva la región geográfica a partir del texto `place` de USGS.
    Ejemplo: '24 km SSW of Puerto El Triunfo, El Salvador' -> 'El Salvador'
             'central Mid-Atlantic Ridge' -> 'central Mid-Atlantic Ridge'
    """
    place = sanitize_place(sanitized_place)
    if place == "Ubicación desconocida":
        return "Región no especificada"

    if "," in place:
        region_part = place.split(",")[-1].strip()
        if region_part:
            return region_part[:100]

    if " of " in place:
        region_part = place.split(" of ")[-1].strip()
        if region_part:
            return region_part[:100]

    return place[:100]


def delimit_untrusted_data(text: str, tag: str = "dato_geofisico") -> str:
    """
    Envuelve un texto ya sanitizado en delimitadores XML explícitos para que el LLM
    lo trate estrictamente como dato pasivo y nunca como instrucción del sistema.
    """
    safe = sanitize_place(text)
    return f"<{tag}>{safe}</{tag}>"

