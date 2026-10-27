"""
Router del Agente Conversacional (`/api/agent/chat`) con cascada de 4 niveles, RBAC por rol y rate limiting.
"""
from fastapi import APIRouter, Depends
from app.agent.orchestrator import process_agent_chat
from app.auth.security import require_viewer
from app.core.rate_limiter import rate_limit_dependency
from app.models.schemas import ChatRequest, ChatResponse, UserInfo

router = APIRouter(prefix="/api/agent", tags=["Agente Conversacional IA"])


@router.post(
    "/chat",
    response_model=ChatResponse,
    dependencies=[Depends(rate_limit_dependency("chat", max_requests=40, window_seconds=60))],
)
async def chat_with_agent(
    payload: ChatRequest,
    user: UserInfo = Depends(require_viewer),
) -> ChatResponse:
    """Procesa la pregunta del usuario en cascada (Nivel 0 → Nivel 1 → Nivel 2 → Nivel 3) aislando el rol."""
    return process_agent_chat(question=payload.question, username=user.username, role=user.role)
