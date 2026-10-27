"""
Rate limiter en memoria por ventana deslizante para proteger `/auth/login` y `/api/agent/chat`.
"""
from collections import defaultdict, deque
import time
from typing import Callable, Deque, Dict
from fastapi import Request
from app.core.exceptions import RateLimitExceededError


class SlidingWindowRateLimiter:
    def __init__(self) -> None:
        self._buckets: Dict[str, Deque[float]] = defaultdict(deque)

    def check(self, key: str, max_requests: int, window_seconds: int) -> None:
        now = time.monotonic()
        dq = self._buckets[key]
        while dq and (now - dq[0]) > window_seconds:
            dq.popleft()
        if len(dq) >= max_requests:
            raise RateLimitExceededError(
                f"Límite de peticiones excedido ({max_requests} por {window_seconds}s). Por favor aguarde unos segundos."
            )
        dq.append(now)

    def reset(self) -> None:
        self._buckets.clear()


rate_limiter = SlidingWindowRateLimiter()


def rate_limit_dependency(scope: str, max_requests: int = 30, window_seconds: int = 60) -> Callable:
    async def _dep(request: Request) -> None:
        client_ip = request.client.host if request.client else "unknown"
        rate_limiter.check(f"{scope}:{client_ip}", max_requests=max_requests, window_seconds=window_seconds)

    return _dep

