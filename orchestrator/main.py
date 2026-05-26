"""Point d'entrée du daemon orchestrateur kola-team.

Lance N coroutines parallèles, une par rôle, chacune avec son propre tempo
heartbeat. Rôles actifs : CEO (revue PRs), Triage (label issues), Backend
(implémente issues area/api). Frontend/QA/Doc à brancher de la même manière.

Run :
  python -m orchestrator.main           # daemon permanent
  python -m orchestrator.main --once    # un seul tick (smoke test)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from .audit import log_event
from .config import HEARTBEAT_SECONDS
from .github_app import GitHubApp
from .roles.backend import BackendAgent
from .roles.ceo import CEOAgent
from .roles.triage import TriageAgent


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def one_tick(agents: list) -> None:
    await asyncio.gather(*(a.heartbeat() for a in agents))


async def loop(agents: list, interval_s: int, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await log_event("orchestrator", "heartbeat-start", agents=[a.role.name for a in agents])
        try:
            await one_tick(agents)
        except Exception as e:
            logging.exception("heartbeat global a échoué : %s", e)
            await log_event("orchestrator", "heartbeat-fail", error=str(e))
        # wait next, mais respecter SIGINT
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


async def amain(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--once", action="store_true", help="un seul tick puis exit")
    p.add_argument("--interval", type=int, default=HEARTBEAT_SECONDS, help="secondes entre heartbeats")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    setup_logging(args.verbose)

    gh = GitHubApp()
    agents = [CEOAgent(gh), TriageAgent(gh), BackendAgent(gh)]

    await log_event(
        "orchestrator", "boot",
        roles=[a.role.name for a in agents],
        interval=args.interval,
        once=args.once,
    )

    if args.once:
        await one_tick(agents)
        return 0

    stop = asyncio.Event()
    loop_task = asyncio.create_task(loop(agents, args.interval, stop))

    def _on_sig(*_) -> None:
        logging.info("signal reçu, arrêt en cours…")
        stop.set()

    asyncio.get_running_loop().add_signal_handler(signal.SIGINT, _on_sig)
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, _on_sig)

    await loop_task
    await log_event("orchestrator", "shutdown")
    return 0


def main() -> None:
    sys.exit(asyncio.run(amain(sys.argv[1:])))


if __name__ == "__main__":
    main()
