from __future__ import annotations

from contextvars import ContextVar, Token


_request_id_context: ContextVar[str | None] = ContextVar(
    "request_id",
    default=None,
)


def get_request_id() -> str | None:
    """Return the request ID for the current execution context."""

    return _request_id_context.get()


def set_request_id(request_id: str) -> Token[str | None]:
    """
    Set the request ID for the current execution context.

    The returned token must later be passed to reset_request_id().
    """

    return _request_id_context.set(request_id)


def reset_request_id(token: Token[str | None]) -> None:
    """Restore the previous request context."""

    _request_id_context.reset(token)