"""Client LiteLLM avec fallback conservateur côté daemon.

Philosophie charte §7 :
  - timeouts longs (qualité > latence)
  - fallback primary → fallback **uniquement** sur ConnectionError ou HTTP 5xx
  - **PAS** de fallback sur timeout (préférer attendre le 35B qu'utiliser le 14B)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from .config import LITELLM_BASE, LITELLM_KEY, Role

log = logging.getLogger(__name__)


class LLMError(Exception):
    pass


class LLMTimeout(LLMError):
    """Timeout sur appel LLM. Pas de fallback automatique (cf. §7)."""


@dataclass
class LLMResult:
    content: str
    model: str          # nom réel servi (peut différer du primary si fallback)
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


async def call(
    role: Role,
    messages: list[dict[str, str]],
    *,
    max_tokens: int = 512,
    temperature: float = 0.2,
    extra_body: dict[str, Any] | None = None,
) -> LLMResult:
    """Appel LLM via LiteLLM avec fallback conservateur.

    Tente `role.primary_alias` ; si ConnectionError ou HTTP 5xx, retry sur
    `role.fallback_alias` (s'il existe). En cas de timeout, lève LLMTimeout
    sans fallback — le caller décide de retry plus tard.
    """
    primary = await _try_one(
        role.primary_alias, messages, max_tokens, temperature, role.llm_timeout_s, extra_body
    )
    if primary is not None:
        return primary

    # primary a renvoyé 5xx ou ConnectionError → tenter le fallback
    if role.fallback_alias is None:
        raise LLMError(f"primary `{role.primary_alias}` indisponible, pas de fallback configuré")

    log.warning(
        "fallback role=%s %s → %s (5xx/connect)",
        role.name, role.primary_alias, role.fallback_alias,
    )
    fb = await _try_one(
        role.fallback_alias, messages, max_tokens, temperature, role.fallback_timeout_s, extra_body
    )
    if fb is None:
        raise LLMError(
            f"primary `{role.primary_alias}` ET fallback `{role.fallback_alias}` indisponibles"
        )
    return fb


async def _try_one(
    alias: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    temperature: float,
    timeout_s: float,
    extra_body: dict[str, Any] | None,
) -> LLMResult | None:
    """Une tentative d'appel. Retourne None si erreur retryable (5xx/connect),
    lève LLMTimeout si timeout, lève LLMError sur erreur définitive."""
    payload: dict[str, Any] = {
        "model": alias,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if extra_body:
        payload.update(extra_body)

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as cli:
            r = await cli.post(
                f"{LITELLM_BASE}/v1/chat/completions",
                headers={"Authorization": f"Bearer {LITELLM_KEY}"},
                json=payload,
            )
    except httpx.ConnectError as e:
        log.warning("LLM connect-error alias=%s : %s", alias, e)
        return None
    except httpx.ReadTimeout:
        raise LLMTimeout(f"timeout {timeout_s}s on alias={alias}")
    except httpx.HTTPError as e:
        raise LLMError(f"http-error alias={alias} : {e}") from e

    if r.status_code >= 500:
        log.warning("LLM 5xx alias=%s status=%s body=%.200s", alias, r.status_code, r.text)
        return None
    if r.status_code >= 400:
        raise LLMError(f"4xx alias={alias} status={r.status_code} body={r.text[:300]}")

    try:
        d = r.json()
        msg = d["choices"][0]["message"]
        content = (msg.get("content") or "").strip()
        usage = d.get("usage") or {}
        return LLMResult(
            content=content,
            model=d.get("model", alias),
            prompt_tokens=int(usage.get("prompt_tokens", 0)),
            completion_tokens=int(usage.get("completion_tokens", 0)),
            total_tokens=int(usage.get("total_tokens", 0)),
        )
    except (KeyError, ValueError, TypeError) as e:
        raise LLMError(f"reponse mal formee alias={alias} : {e} body={r.text[:300]}") from e
