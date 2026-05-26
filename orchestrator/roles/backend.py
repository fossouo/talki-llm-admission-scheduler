"""Backend agent — implémente les issues `area/api` + `ready-for-dev`.

Boucle minimale (PAS Aider) :
  1. Pick une issue dispo
  2. Ajoute label in-progress-by-backend (anti-concurrence)
  3. Crée worktree + identité git + hook pre-commit
  4. Construit prompt système (agents/backend.md) + contexte repo
  5. Appelle LLM via llm.py (alias `code`, 600s timeout)
  6. Parse JSON {files, tests, test_command, summary, why}
  7. Refuse tout fichier matchant zones interdites §1
  8. Écrit fichiers, commit, push
  9. Ouvre PR What/Why/Test, commente l'issue, swap labels
 10. Cleanup worktree

Limites assumées (vs Aider) :
  - Pas de boucle réécriture/correction si tests échouent (1 shot)
  - Pas de diff incrémental ; full-file content (OK pour issues triviales)
  - Plus tard : injecter Aider en remplacement de _generate_changes()
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import textwrap
from dataclasses import dataclass
from pathlib import Path

from ..audit import log_event
from ..budget import (
    BudgetExceeded,
    BudgetUnavailable,
    Reservation,
    check_and_reserve,
    commit,
    get_client,
)
from ..charter_rules import matches_forbidden
from ..config import POC_ROOT, TEAM_ROOT, role
from ..github_app import GitHubApp, GitHubAppError
from ..llm import LLMError, LLMTimeout, call
from ..worktree import (
    Worktree,
    WorktreeError,
    create_worktree,
    ensure_base_uptodate,
    remove_worktree,
    _run,
)

log = logging.getLogger(__name__)

PROMPT_PATH = TEAM_ROOT / "agents" / "backend.md"
AREA_LABEL = "area/api"
READY_LABEL = "ready-for-dev"
INPROGRESS_LABEL = "in-progress-by-backend"
DONE_LABEL = "worked-by-backend"
BLOCKED_LABEL = "blocked-by-backend"
NEEDS_REVIEW_LABEL = "needs-human-review"
MAX_FILE_TREE_LINES = 200
MAX_OPEN_FILE_CHARS = 6000
MAX_TEST_RETRIES = 2  # self-building niveau 1 : 3 essais total (1 + 2 retries)
PYTEST_OUTPUT_TRUNC = 4000

JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _load_prompt() -> str:
    if not PROMPT_PATH.exists():
        raise FileNotFoundError(f"backend prompt manquant : {PROMPT_PATH}")
    return PROMPT_PATH.read_text()


def _extract_json(text: str) -> dict | None:
    m = JSON_RE.search(text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


@dataclass
class GeneratedChanges:
    summary: str
    why: str
    files: list[dict]            # [{"path": "...", "content": "..."}]
    tests: list[dict]
    test_command: str | None


class BackendAgent:
    def __init__(self, gh: GitHubApp) -> None:
        self.gh = gh
        self.role = role("backend")
        self._prompt = _load_prompt()

    # ------------------------------------------------------------------
    # heartbeat
    # ------------------------------------------------------------------
    async def heartbeat(self) -> None:
        import os
        if os.environ.get("KOLA_BACKEND_PAUSED") == "1":
            await log_event("backend", "paused", reason="KOLA_BACKEND_PAUSED=1")
            return
        try:
            issues = await self.gh.list_open_issues(labels=[AREA_LABEL, READY_LABEL])
        except GitHubAppError as e:
            log.error("Backend list_open_issues: %s", e)
            await log_event("backend", "heartbeat-fail", error=str(e))
            return

        # exclure les déjà-prises (par nous ou bloquées)
        candidates = [
            i for i in issues
            if not _has_label(i, INPROGRESS_LABEL)
            and not _has_label(i, DONE_LABEL)
            and not _has_label(i, BLOCKED_LABEL)
            and not _has_label(i, "needs-human-review")
        ]
        if not candidates:
            return

        # une seule à la fois pour ne pas exploser le LLM
        issue = sorted(candidates, key=lambda i: i["number"])[0]
        await self._work_one(issue)

    # ------------------------------------------------------------------
    # main flow
    # ------------------------------------------------------------------
    async def _work_one(self, issue: dict) -> None:
        number = issue["number"]
        title = issue["title"]
        log.info("Backend pick issue #%s : %s", number, title)
        await log_event("backend", "pick", issue=number, title=title)

        # 1. claim
        await self.gh.add_labels(number, [INPROGRESS_LABEL])

        wt: Worktree | None = None
        try:
            await ensure_base_uptodate()
            wt = await create_worktree(self.role, number, title)
        except WorktreeError as e:
            await self._block(number, f"worktree setup failed : {e}")
            return

        try:
            comments = await self._fetch_issue_comments(number)
            changes = await self._generate_changes(issue, wt, comments)
            if changes is None:
                await self._block(number, "LLM n'a pas produit de changements exploitables")
                return

            forbidden = self._validate_paths(changes)
            if forbidden:
                await self._block(number, f"changements touchent zones interdites §1 : {forbidden}")
                return

            self._write_files(wt, changes)

            # self-building niveau 1 : pytest cible + retry + pytest full utils
            test_result = await self._run_tests_with_retries(
                wt, issue, comments, changes,
            )

            ok, msg = await self._commit_and_push(wt, changes, number)
            if not ok:
                await self._block(number, msg)
                return

            extra_labels: list[str] = []
            if not test_result.green:
                extra_labels.append(NEEDS_REVIEW_LABEL)

            pr_num = await self._open_pr(wt, changes, number, extra_labels=extra_labels)
            if not test_result.green:
                await self._comment_pr_with_tests(pr_num, test_result)
            await self.gh.comment_issue(number, f"🤖 Backend Agent — travaillé dans PR #{pr_num}.")
            await self.gh.remove_label(number, INPROGRESS_LABEL)
            await self.gh.add_labels(number, [DONE_LABEL])
            await log_event(
                "backend", "done",
                issue=number, pr=pr_num,
                tests_green=test_result.green,
                attempts=test_result.attempts,
            )
        except Exception as e:
            log.exception("Backend agent crash sur issue #%s", number)
            await self._block(number, f"crash : {e}")
        finally:
            if wt is not None:
                try:
                    await remove_worktree(wt)
                except Exception as e:
                    log.warning("cleanup worktree failed : %s", e)

    # ------------------------------------------------------------------
    # generation
    # ------------------------------------------------------------------
    async def _fetch_issue_comments(self, number: int) -> list[dict]:
        try:
            return await self.gh.list_issue_comments(number) or []
        except GitHubAppError as e:
            log.warning("Backend list_issue_comments #%s: %s", number, e)
            return []

    @staticmethod
    def _format_comments(comments: list[dict]) -> str:
        if not comments:
            return "(aucun commentaire)"
        lines: list[str] = []
        for c in comments[-10:]:
            author = (c.get("user") or {}).get("login") or "?"
            body = (c.get("body") or "").strip()
            if not body:
                continue
            body = textwrap.shorten(body, width=1500, placeholder="…")
            lines.append(f"- @{author}: {body}")
        return "\n".join(lines) or "(aucun commentaire significatif)"

    async def _generate_changes(
        self, issue: dict, wt: Worktree, comments: list[dict],
    ) -> GeneratedChanges | None:
        red = await get_client()
        # ordre de grandeur : prompt~2k + tree~2k + body~1k + réponse jusqu'à 4k
        expected = 8000
        reservation: Reservation | None = None
        try:
            reservation = await check_and_reserve(red, self.role, expected)
        except BudgetExceeded as e:
            log.warning("Backend budget atteint : %s", e)
            await log_event("backend", "budget-exceeded", issue=issue["number"], detail=str(e))
            return None
        except BudgetUnavailable as e:
            await log_event("backend", "budget-unavailable", issue=issue["number"], detail=str(e))

        tree = _repo_tree(wt.path / "apps" / "api", max_lines=MAX_FILE_TREE_LINES)
        comments_block = self._format_comments(comments)
        user_msg = textwrap.dedent(f"""\
            Issue #{issue['number']} : {issue['title']}

            Body :
            {textwrap.shorten(issue.get('body') or '', width=4000, placeholder='…')}

            Commentaires de l'issue (les commentaires CEO complètent/surchargent l'AC) :
            {comments_block}

            Arborescence apps/api/ (limitée à {MAX_FILE_TREE_LINES} lignes) :
            {tree}

            Tu DOIS répondre en JSON strict, **aucun markdown, aucun commentaire**.
            Format :
            {{
              "summary": "<1 ligne>",
              "why": "<lien à l'issue + raison>",
              "files": [{{"path": "apps/api/...", "content": "<contenu complet du fichier>"}}],
              "tests": [{{"path": "apps/api/tests/...", "content": "<contenu complet>"}}],
              "test_command": "pytest -q apps/api/tests/test_xxx.py"
            }}

            Contraintes :
            - Seulement les fichiers de ton scope (charte §6). Refus si tu veux toucher
              une zone interdite — réponds {{"summary":"BLOCKED","why":"<raison>","files":[],"tests":[],"test_command":null}}.
            - Contenu complet de chaque fichier (pas de diff). Si fichier existant, donne
              la nouvelle version intégrale.
            - Au moins 1 test pytest associé à ton changement.
        """)

        messages = [
            {"role": "system", "content": self._prompt},
            {"role": "user", "content": user_msg},
        ]
        try:
            res = await call(self.role, messages, max_tokens=4000, temperature=0.2)
        except LLMTimeout:
            log.warning("Backend timeout sur issue #%s", issue["number"])
            if reservation is not None:
                await commit(red, reservation, expected)
            await log_event("backend", "llm-timeout", issue=issue["number"])
            return None
        except LLMError as e:
            log.error("Backend LLM erreur : %s", e)
            if reservation is not None:
                await commit(red, reservation, expected)
            await log_event("backend", "llm-error", issue=issue["number"], error=str(e))
            return None
        if reservation is not None:
            await commit(red, reservation, res.total_tokens)

        data = _extract_json(res.content)
        if not data:
            await log_event(
                "backend", "json-parse-fail", issue=issue["number"], raw=res.content[:300],
            )
            return None

        if data.get("summary") == "BLOCKED":
            await log_event("backend", "llm-blocked", issue=issue["number"], why=data.get("why"))
            return None

        files = data.get("files") or []
        if not files:
            return None

        return GeneratedChanges(
            summary=str(data.get("summary") or "(no summary)")[:200],
            why=str(data.get("why") or "")[:500],
            files=files,
            tests=list(data.get("tests") or []),
            test_command=data.get("test_command") or None,
        )

    # ------------------------------------------------------------------
    # validation
    # ------------------------------------------------------------------
    def _validate_paths(self, changes: GeneratedChanges) -> list[str]:
        bad: list[str] = []
        for f in changes.files + changes.tests:
            p = (f.get("path") or "").strip().lstrip("/")
            if not p:
                bad.append("(empty)")
                continue
            hit = matches_forbidden(p)
            if hit:
                bad.append(f"{p} (matches {hit})")
        return bad

    def _write_files(self, wt: Worktree, changes: GeneratedChanges) -> None:
        for f in changes.files + changes.tests:
            p = wt.path / f["path"]
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(f["content"])

    # ------------------------------------------------------------------
    # git
    # ------------------------------------------------------------------
    async def _commit_and_push(
        self, wt: Worktree, changes: GeneratedChanges, issue_num: int,
    ) -> tuple[bool, str]:
        code, _, err = await _run(["git", "add", "-A"], cwd=wt.path)
        if code != 0:
            return False, f"git add: {err.strip()}"
        # vérifie qu'on a quelque chose à committer
        code, out, _ = await _run(["git", "diff", "--cached", "--name-only"], cwd=wt.path)
        if not out.strip():
            return False, "rien à committer (LLM a produit du contenu identique)"

        msg = f"{changes.summary}\n\nRefs #{issue_num}\n\n{changes.why}"
        code, _, err = await _run(["git", "commit", "-m", msg], cwd=wt.path)
        if code != 0:
            return False, f"git commit (pre-commit hook?): {err.strip()}"
        code, _, err = await _run(["git", "push", "-u", "origin", wt.branch], cwd=wt.path)
        if code != 0:
            return False, f"git push: {err.strip()}"
        return True, "ok"

    async def _open_pr(
        self, wt: Worktree, changes: GeneratedChanges, issue_num: int,
        extra_labels: list[str] | None = None,
    ) -> int:
        body = textwrap.dedent(f"""\
            ## What
            {changes.summary}

            ## Why
            {changes.why}

            ## Test
            - [ ] `{changes.test_command or '(no test command provided)'}`
            - [ ] CI passe (forbidden-zones-check + tests)

            Closes #{issue_num}.

            ---
            🤖 Auto-généré par Backend Agent (kola-team).
        """)
        pr = await self.gh.create_pull_request(
            title=f"feat: {changes.summary}",
            body=body,
            head=wt.branch,
            base="main",
        )
        pr_num = int(pr["number"])
        if extra_labels:
            try:
                await self.gh.add_labels(pr_num, extra_labels)
            except GitHubAppError as e:
                log.warning("Backend add_labels PR #%s %s: %s", pr_num, extra_labels, e)
        return pr_num

    # ------------------------------------------------------------------
    # self-building niveau 1 : pytest + retry
    # ------------------------------------------------------------------
    async def _run_tests_with_retries(
        self, wt: Worktree, issue: dict, comments: list[dict],
        changes: GeneratedChanges,
    ) -> "TestResult":
        number = issue["number"]
        # 1er essai
        target = [t["path"] for t in changes.tests if t.get("path")]
        result = await self._pytest(wt, target, label="target")
        await log_event(
            "backend", "backend-test-retry",
            issue=number, attempt=1, phase="target",
            exit_code=result.exit_code,
        )
        attempts = 1

        if result.exit_code == 0:
            # vérifie la suite utils complète
            full = _full_utils_tests(wt, exclude=None)
            full_res = await self._pytest(wt, full, label="utils-full")
            await log_event(
                "backend", "backend-test-retry",
                issue=number, attempt=1, phase="utils-full",
                exit_code=full_res.exit_code,
            )
            if full_res.exit_code == 0:
                return TestResult(green=True, attempts=1, output=full_res.output)
            # régression sur d'autres utils — retry sur ce contexte aussi
            result = full_res

        # retries
        current_changes = changes
        for retry in range(1, MAX_TEST_RETRIES + 1):
            attempts += 1
            fix = await self._generate_fix(
                issue, comments, wt, current_changes, result.output,
            )
            if fix is None:
                await log_event(
                    "backend", "backend-test-retry",
                    issue=number, attempt=attempts, phase="fix-llm-fail",
                )
                break

            forbidden = self._validate_paths(fix)
            if forbidden:
                await log_event(
                    "backend", "backend-test-retry",
                    issue=number, attempt=attempts, phase="forbidden-path",
                    paths=forbidden,
                )
                # garde-fou : on rejette le retry, on garde le state précédent
                break

            self._write_files(wt, fix)
            current_changes = fix
            target = [t["path"] for t in fix.tests if t.get("path")]
            r1 = await self._pytest(wt, target, label=f"target-retry{retry}")
            await log_event(
                "backend", "backend-test-retry",
                issue=number, attempt=attempts, phase=f"target-retry{retry}",
                exit_code=r1.exit_code,
            )
            if r1.exit_code != 0:
                result = r1
                continue
            full = _full_utils_tests(wt, exclude=None)
            r2 = await self._pytest(wt, full, label=f"utils-full-retry{retry}")
            await log_event(
                "backend", "backend-test-retry",
                issue=number, attempt=attempts, phase=f"utils-full-retry{retry}",
                exit_code=r2.exit_code,
            )
            if r2.exit_code == 0:
                return TestResult(green=True, attempts=attempts, output=r2.output)
            result = r2

        return TestResult(green=False, attempts=attempts, output=result.output)

    async def _pytest(
        self, wt: Worktree, test_files: list[str], label: str,
    ) -> "PytestRun":
        if not test_files:
            return PytestRun(exit_code=0, output=f"({label}: aucun fichier de test à exécuter)")
        cmd = _resolve_pytest_cmd(wt) + test_files + ["-q"]
        code, out, err = await _run(cmd, cwd=wt.path)
        merged = (out or "") + (("\n" + err) if err else "")
        return PytestRun(exit_code=code, output=merged)

    async def _generate_fix(
        self, issue: dict, comments: list[dict], wt: Worktree,
        previous: GeneratedChanges, pytest_output: str,
    ) -> GeneratedChanges | None:
        red = await get_client()
        expected = 8000
        reservation: Reservation | None = None
        try:
            reservation = await check_and_reserve(red, self.role, expected)
        except BudgetExceeded as e:
            log.warning("Backend budget atteint (retry) : %s", e)
            await log_event("backend", "budget-exceeded", issue=issue["number"], detail=str(e))
            return None
        except BudgetUnavailable as e:
            await log_event("backend", "budget-unavailable", issue=issue["number"], detail=str(e))

        files_block = "\n\n".join(
            f"# {f['path']}\n{f.get('content', '')}"
            for f in (previous.files + previous.tests)
        )
        comments_block = self._format_comments(comments)
        pytest_trunc = pytest_output[:PYTEST_OUTPUT_TRUNC]
        user_msg = textwrap.dedent(f"""\
            Issue #{issue['number']} : {issue['title']}

            Body :
            {textwrap.shorten(issue.get('body') or '', width=2000, placeholder='…')}

            Commentaires de l'issue :
            {comments_block}

            Tu as produit ce code à l'itération précédente :

            {files_block}

            Pytest a échoué. Sortie tronquée :

            ```
            {pytest_trunc}
            ```

            Corrige uniquement ce qui est nécessaire pour que les tests passent.
            Tu DOIS rester dans ton scope (charte §6 — `apps/api/utils/**`, `apps/api/tests/**`,
            etc.). Aucune zone interdite.

            Réponds en JSON strict, **aucun markdown, aucun commentaire**.
            Format identique à ta génération initiale :
            {{
              "summary": "<1 ligne>",
              "why": "<lien à l'issue + raison>",
              "files": [{{"path": "apps/api/...", "content": "<contenu complet>"}}],
              "tests": [{{"path": "apps/api/tests/...", "content": "<contenu complet>"}}],
              "test_command": "pytest -q apps/api/tests/test_xxx.py"
            }}
            Donne le contenu COMPLET de chaque fichier que tu modifies, même si la modif est petite.
        """)
        messages = [
            {"role": "system", "content": self._prompt},
            {"role": "user", "content": user_msg},
        ]
        try:
            res = await call(self.role, messages, max_tokens=4000, temperature=0.2)
        except LLMTimeout:
            log.warning("Backend timeout retry sur #%s", issue["number"])
            if reservation is not None:
                await commit(red, reservation, expected)
            await log_event("backend", "llm-timeout", issue=issue["number"], phase="retry")
            return None
        except LLMError as e:
            log.error("Backend LLM erreur (retry) : %s", e)
            if reservation is not None:
                await commit(red, reservation, expected)
            await log_event("backend", "llm-error", issue=issue["number"], phase="retry", error=str(e))
            return None
        if reservation is not None:
            await commit(red, reservation, res.total_tokens)

        data = _extract_json(res.content)
        if not data:
            await log_event(
                "backend", "json-parse-fail", issue=issue["number"], phase="retry",
                raw=res.content[:300],
            )
            return None
        files = data.get("files") or []
        if not files and not (data.get("tests") or []):
            return None
        return GeneratedChanges(
            summary=str(data.get("summary") or previous.summary)[:200],
            why=str(data.get("why") or previous.why)[:500],
            files=files or previous.files,
            tests=list(data.get("tests") or previous.tests),
            test_command=data.get("test_command") or previous.test_command,
        )

    async def _comment_pr_with_tests(self, pr_num: int, result: "TestResult") -> None:
        trunc = result.output[:PYTEST_OUTPUT_TRUNC]
        body = textwrap.dedent(f"""\
            🛑 Backend Agent — tests rouges après {result.attempts} essai(s).

            Label `{NEEDS_REVIEW_LABEL}` posé. Revue humaine requise.

            <details><summary>pytest output (tronqué)</summary>

            ```
            {trunc}
            ```
            </details>
        """)
        try:
            await self.gh.comment_pr(pr_num, body)
        except GitHubAppError as e:
            log.warning("Backend comment_pr #%s failed: %s", pr_num, e)

    async def _block(self, issue_num: int, reason: str) -> None:
        log.warning("Backend bloqué sur #%s : %s", issue_num, reason)
        await self.gh.comment_issue(
            issue_num,
            f"🛑 Backend Agent — bloqué :\n\n> {reason}\n\nLabel `{BLOCKED_LABEL}` posé. Intervention humaine requise.",
        )
        try:
            await self.gh.remove_label(issue_num, INPROGRESS_LABEL)
        except GitHubAppError:
            pass
        try:
            await self.gh.add_labels(issue_num, [BLOCKED_LABEL])
        except GitHubAppError:
            pass
        await log_event("backend", "blocked", issue=issue_num, reason=reason)


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
@dataclass
class PytestRun:
    exit_code: int
    output: str


@dataclass
class TestResult:
    green: bool
    attempts: int
    output: str


def _resolve_pytest_cmd(wt: Worktree) -> list[str]:
    """Détecte pytest dans le worktree (venv si présent) sinon python3 -m pytest."""
    venv_py = wt.path / "apps" / "api" / "venv" / "bin" / "python"
    if venv_py.exists():
        return [str(venv_py), "-m", "pytest"]
    return ["python3", "-m", "pytest"]


def _full_utils_tests(wt: Worktree, exclude: list[str] | None) -> list[str]:
    """Liste les tests qui correspondent à un module de `apps/api/utils/`.

    On NE globbe PAS aveuglément `apps/api/tests/test_*.py` : `test_main.py` et
    autres tests stack-FastAPI ne sont pas dans le scope du Backend Agent et
    requièrent des dépendances non installées dans le venv utils.
    """
    utils_dir = wt.path / "apps" / "api" / "utils"
    tests_dir = wt.path / "apps" / "api" / "tests"
    if not utils_dir.exists() or not tests_dir.exists():
        return []
    excluded = set(exclude or [])
    out: list[str] = []
    for u in sorted(utils_dir.glob("*.py")):
        if u.name == "__init__.py":
            continue
        candidate = tests_dir / f"test_{u.name}"
        if not candidate.exists():
            continue
        rel = str(candidate.relative_to(wt.path))
        if rel in excluded:
            continue
        out.append(rel)
    return out


def _has_label(issue: dict, name: str) -> bool:
    return any(l["name"] == name for l in issue.get("labels", []))


def _repo_tree(root: Path, max_lines: int) -> str:
    if not root.exists():
        return "(répertoire absent)"
    out: list[str] = []
    for p in sorted(root.rglob("*")):
        if "__pycache__" in p.parts or ".pytest_cache" in p.parts:
            continue
        if p.is_file():
            out.append(str(p.relative_to(root.parent)))
        if len(out) >= max_lines:
            out.append(f"… (tronqué à {max_lines} entrées)")
            break
    return "\n".join(out)
