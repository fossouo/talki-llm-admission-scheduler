"""Compteur Redis pour les budgets tokens/jour (charte §7).

Clé Redis : `kola:tokens:{role}:{YYYY-MM-DD}` (TTL 48h, expire d'office).
`check_and_reserve(role, expected_tokens)` reserve avant l'appel LLM.
`commit(role, actual_tokens, reservation)` ajuste après la réponse (économie
si le LLM a consommé moins que prévu).
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from .config import REDIS_URL, Role

log = logging.getLogger(__name__)


class BudgetExceeded(Exception):
    """L'agent a atteint son cap quotidien."""


class BudgetUnavailable(Exception):
    """Redis indisponible — appel à laisser passer en mode dégradé."""


@dataclass
class Reservation:
    role: str
    key: str
    reserved: int


def _key(role_name: str, day: dt.date | None = None) -> str:
    d = day or dt.date.today()
    return f"kola:tokens:{role_name}:{d.isoformat()}"


async def get_client() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)


async def check_and_reserve(
    r: redis.Redis,
    role: Role,
    expected_tokens: int,
) -> Reservation:
    """Réserve `expected_tokens` avant un appel LLM.

    Lève BudgetExceeded si le compteur du jour + expected dépasserait le cap.
    Sinon incrémente le compteur (atomique) et renvoie une Reservation.
    Le caller doit appeler `commit()` après l'appel pour ajuster aux tokens
    réellement consommés.
    """
    key = _key(role.name)
    try:
        new_total = await r.incrby(key, expected_tokens)
        await r.expire(key, 60 * 60 * 48)  # TTL 48h (au cas où jamais committed)
    except (RedisConnectionError, RedisTimeoutError) as e:
        log.warning("Redis indisponible (%s) — budget non vérifié pour role=%s", e, role.name)
        raise BudgetUnavailable(str(e)) from e
    if new_total > role.daily_token_cap:
        # rollback la réservation
        try:
            await r.incrby(key, -expected_tokens)
        except (RedisConnectionError, RedisTimeoutError):
            pass
        raise BudgetExceeded(
            f"role={role.name} cap={role.daily_token_cap} "
            f"déjà={new_total - expected_tokens} reserved={expected_tokens}"
        )
    return Reservation(role=role.name, key=key, reserved=expected_tokens)


async def commit(r: redis.Redis, reservation: Reservation, actual_tokens: int) -> None:
    """Ajuste le compteur après l'appel : applique le delta entre reservé et réel."""
    delta = actual_tokens - reservation.reserved
    if delta == 0:
        return
    try:
        await r.incrby(reservation.key, delta)
    except (RedisConnectionError, RedisTimeoutError) as e:
        log.warning("Redis indisponible au commit role=%s delta=%s : %s", reservation.role, delta, e)


async def remaining(r: redis.Redis, role: Role) -> int:
    """Tokens restants disponibles pour `role` aujourd'hui (peut être négatif)."""
    val = await r.get(_key(role.name))
    used = int(val) if val else 0
    return role.daily_token_cap - used
