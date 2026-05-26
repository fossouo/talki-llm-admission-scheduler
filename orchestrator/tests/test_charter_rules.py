"""Tests des règles charte §1/§3/§4 — pure unit, aucun I/O ni dépendance externe."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from orchestrator.charter_rules import (
    check_charter_3,
    check_charter_4,
    is_agent_pr,
    matches_forbidden,
)


def _agent_pr(body="## What\nfoo\n## Why\nbar\n## Test\nbaz"):
    return {
        "number": 99,
        "title": "test",
        "body": body,
        "draft": False,
        "user": {"login": "kola-backend-agent"},
        "head": {"sha": "abc"},
        "labels": [],
    }


def _ok_status():
    return {"check_runs": [{"name": "guard", "conclusion": "success"}]}


def _file(name, additions=10, deletions=2, patch="", status="modified"):
    return {
        "filename": name,
        "additions": additions,
        "deletions": deletions,
        "status": status,
        "patch": patch,
    }


# ---- matches_forbidden ----

def test_forbidden_env():
    assert matches_forbidden(".env")
    assert matches_forbidden(".env.example")
    assert matches_forbidden(".env.local")


def test_forbidden_db_main_config():
    assert matches_forbidden("apps/api/db.py")
    assert matches_forbidden("apps/api/models.py")
    assert matches_forbidden("apps/api/main.py")
    assert matches_forbidden("apps/api/config.py")


def test_forbidden_auth_kyc_aml():
    assert matches_forbidden("apps/api/routers/auth.py")
    assert matches_forbidden("apps/api/routers/auth/login.py")
    assert matches_forbidden("apps/api/services/kyc_service.py")
    assert matches_forbidden("apps/api/services/kyc_service_v2.py")
    assert matches_forbidden("apps/api/services/aml_service.py")


def test_forbidden_infra_ci():
    assert matches_forbidden("docker-compose.yml")
    assert matches_forbidden("docker-compose.prod.yml")
    assert matches_forbidden("Dockerfile.api")
    assert matches_forbidden(".github/workflows/test.yml")


def test_forbidden_frontend_root():
    assert matches_forbidden("apps/web/tsconfig.json")
    assert matches_forbidden("apps/web/package.json")
    assert matches_forbidden("apps/web/middleware.ts")
    assert matches_forbidden("apps/web/app/layout.tsx")


def test_allowed_paths():
    assert not matches_forbidden("apps/api/routers/transactions.py")
    assert not matches_forbidden("apps/api/utils/http_client.py")
    assert not matches_forbidden("apps/web/app/page.tsx")
    assert not matches_forbidden("apps/web/app/components/Foo.tsx")
    assert not matches_forbidden("docs/README.md")
    assert not matches_forbidden("apps/api/tests/test_foo.py")


# ---- is_agent_pr ----

def test_is_agent_pr_roles():
    for u in ("kola-ceo-agent", "kola-backend-agent", "kola-doc-agent",
              "kola-frontend-agent", "kola-qa-agent", "kola-triage-agent"):
        assert is_agent_pr({"user": {"login": u}})


def test_is_agent_pr_bot():
    assert is_agent_pr({"user": {"login": "kola-team-bot[bot]"}})
    assert is_agent_pr({"user": {"login": "kola-team-bot"}})


def test_is_agent_pr_human():
    assert not is_agent_pr({"user": {"login": "fossouo"}})
    assert not is_agent_pr({"user": {"login": "some-contributor"}})


# ---- check_charter_3 ----

def test_charter3_happy_path():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py")]
    res = check_charter_3(pr, files, _ok_status())
    assert res.ok, res.reason


def test_charter3_human_pr():
    pr = _agent_pr()
    pr["user"]["login"] = "fossouo"
    files = [_file("apps/api/utils/foo.py")]
    assert not check_charter_3(pr, files, _ok_status()).ok


def test_charter3_no_ci():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py")]
    assert not check_charter_3(pr, files, {"check_runs": []}).ok


def test_charter3_ci_failed():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py")]
    bad = {"check_runs": [{"name": "guard", "conclusion": "failure"}]}
    assert not check_charter_3(pr, files, bad).ok


def test_charter3_forbidden_zone():
    pr = _agent_pr()
    res = check_charter_3(pr, [_file(".env")], _ok_status())
    assert not res.ok and "interdite" in res.reason


def test_charter3_diff_too_large():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py", additions=300, deletions=200)]
    res = check_charter_3(pr, files, _ok_status())
    assert not res.ok and "diff" in res.reason


def test_charter3_new_dependency():
    pr = _agent_pr()
    files = [_file("apps/api/requirements.txt")]
    res = check_charter_3(pr, files, _ok_status())
    assert not res.ok and "dépendance" in res.reason


def test_charter3_placeholder_pass():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py", patch="+def bar():\n+    pass\n")]
    res = check_charter_3(pr, files, _ok_status())
    assert not res.ok and "placeholder" in res.reason


def test_charter3_todo_introduced():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py", patch="+# TODO fix this\n")]
    res = check_charter_3(pr, files, _ok_status())
    assert not res.ok and "TODO" in res.reason


def test_charter3_missing_sections():
    pr = _agent_pr(body="just text without sections")
    files = [_file("apps/api/utils/foo.py")]
    res = check_charter_3(pr, files, _ok_status())
    assert not res.ok and "section" in res.reason


# ---- check_charter_4 ----

def test_charter4_secret_in_diff():
    pr = _agent_pr()
    secret_line = "+TOKEN = 'sk-d08befbc840d15dfaf346c9f114309eed6ec924dcf93848f3fbc350969327562'\n"
    files = [_file("apps/api/utils/foo.py", patch=secret_line)]
    res = check_charter_4(pr, files)
    assert not res.ok and "secret" in res.reason


def test_charter4_prod_keyword_in_yaml():
    pr = _agent_pr()
    files = [_file("infra/something.yaml", patch="+env: production\n")]
    res = check_charter_4(pr, files)
    assert not res.ok and "prod" in res.reason.lower()


def test_charter4_clean_passes():
    pr = _agent_pr()
    files = [_file("apps/api/utils/foo.py", patch="+def hello():\n+    return 'hi'\n")]
    res = check_charter_4(pr, files)
    assert res.ok


def test_charter4_forbidden_close_immediate():
    pr = _agent_pr()
    files = [_file(".env", patch="+SECRET=xxx\n")]
    res = check_charter_4(pr, files)
    assert not res.ok and ("interdite" in res.reason or "close" in res.reason)
