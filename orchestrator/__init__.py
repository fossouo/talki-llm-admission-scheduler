"""kola-team orchestrator — daemon asyncio qui anime les 6 rôles d'agents.

Lit `CHARTER.md` et `ORG_CHART.md` au démarrage. Boucle de heartbeat 5 min.
Tous les appels LLM via http://inference-gateway.local:4000 (LiteLLM canonique).
Tous les appels GitHub via le token de l'App `kola-team-bot`.
Toutes les actions auditables dans logs/orchestrator-YYYY-MM-DD.jsonl.

Voir docs/orchestrator-architecture.md pour la vue d'ensemble.
"""
