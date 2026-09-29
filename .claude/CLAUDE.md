# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Operating principles

- **Capitalize local before spending Claude tokens (2026-09-29, applies regardless of session)**:
  default operating model is **Claude as tech-lead / orchestrator / reviewer**, not primary
  executor for basic/mechanical work. Both `dsh` and `agy` are host-level tools (installed on
  this Mac, not tied to any one repo) and should be the first reflex for delegable sub-tasks:
  log reading, diff summarizing, repetitive grep/search, mechanical checks, first-pass drafts.
  - `dsh` — local LiteLLM mesh (`xeon:4000`, Talki infra) with a fleet of model aliases
    (`chat-fast`, `code`/`code-fast`, `content-worker`, `tool-worker`, `vision`, `embedding`,
    etc.). First delegate for basic tasks.
  - `agy` — local multi-model CLI (`~/.local/bin/agy`), non-interactive via
    `agy -p "..." --model ... --effort ...`, separate Gemini / Claude+GPT-OSS quota pools,
    frequently near-idle. Second delegate, including spinning up its own sub-agents, for tasks
    `dsh`'s aliases aren't suited to.
  - Claude's own reasoning tokens are reserved for: deciding what to delegate and to what,
    reviewing/integrating what `dsh`/`agy` produced, the final edit when it's not mechanical,
    and arbitration/risk calls. For code review specifically, prefer an independent reviewer
    (Codex or equivalent) where that workflow exists; Claude self-review is fallback only.
- **Continuous improvement, not just delegation**: when routing to `dsh`/`agy` surfaces a real
  failure (hallucination, single point of failure, misconfiguration, quota exhaustion, silent
  no-op), harden the local setup in place rather than just noting it or routing around it.
  Full rationale and precedent (talki-app, 2026-09-28): Claude's own memory,
  `feedback_local_first_token_budget_policy_2026_07_07.md`.
