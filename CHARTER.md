# Talki LLM Admission Scheduler — Charte de l'équipe d'agents

## 0. Acteurs
- **Humain** : marbofinance / Donald
- **CEO Agent** : Applique cette charte, revoit les PRs, merge selon politique semi-auto.

## 1. Zones interdites
- .env*
- deploy/**
- app/main.py
- CHARTER.md

## 2. Scopes autorisés
- app/scheduler/** (logique d'admission)
- benchmarks/**
- tests/**
- README.md

## 3. Politique de merge
- CI verte
- Aucune zone interdite
- Scope §2 uniquement
- Diff < 400 lignes
