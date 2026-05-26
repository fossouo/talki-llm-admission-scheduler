"""Unit tests for the YAML config loader and Pydantic models."""

from __future__ import annotations

import os
import textwrap

import pytest
import yaml

from app.config import AppConfig, ModelConfig, load_config

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DEFAULT_CONFIG = os.path.join(_REPO_ROOT, "config", "default.yaml")


def test_load_default_yaml():
    """The default config must load without errors."""
    cfg = load_config(_DEFAULT_CONFIG)
    assert isinstance(cfg, AppConfig)


def test_model_code_priority():
    """config/default.yaml: models.code.priority must equal 3 (per architecture §9)."""
    cfg = load_config(_DEFAULT_CONFIG)
    assert "code" in cfg.models
    assert cfg.models["code"].priority == 3


def test_model_chat_fast_concurrency():
    """config/default.yaml: models.chat-fast.max_concurrency must equal 4."""
    cfg = load_config(_DEFAULT_CONFIG)
    assert cfg.models["chat-fast"].max_concurrency == 4


def test_all_expected_models_present():
    """Default config must define code, chat-fast, brain-exec, tool-worker."""
    cfg = load_config(_DEFAULT_CONFIG)
    for expected in ("code", "chat-fast", "brain-exec", "tool-worker"):
        assert expected in cfg.models, f"Model '{expected}' missing from default config"


def test_missing_file_raises():
    """load_config on a non-existent path must raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("/tmp/this-file-does-not-exist-12345.yaml")


def test_unknown_key_tolerated(tmp_path):
    """Extra unknown keys at the top level must not crash the loader (extra='ignore' or handled)."""
    raw = {
        "scheduler": {"redis_url": "redis://localhost:6379/0"},
        "unknown_future_key": {"foo": "bar"},
    }
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(yaml.dump(raw))
    # Should not raise — Pydantic v2 uses extra='ignore' by default
    cfg = load_config(str(cfg_file))
    assert isinstance(cfg, AppConfig)


def test_model_config_for_known_model():
    """model_config_for('code') returns the specific overrides."""
    cfg = load_config(_DEFAULT_CONFIG)
    mcfg = cfg.model_config_for("code")
    assert mcfg.max_concurrency == 2
    assert mcfg.max_queue_depth == 20


def test_model_config_for_unknown_model_uses_defaults():
    """model_config_for('unknown') returns defaults, not KeyError."""
    cfg = load_config(_DEFAULT_CONFIG)
    mcfg = cfg.model_config_for("unknown-model-xyz")
    assert mcfg.max_concurrency == cfg.defaults.max_concurrency
    assert mcfg.max_queue_depth == cfg.defaults.max_queue_depth


def test_priority_validator_rejects_invalid():
    """ModelConfig.priority must only accept 1, 3, 5, 7, 9."""
    with pytest.raises(Exception):
        ModelConfig(max_concurrency=2, max_queue_depth=10, priority=4)


def test_priority_validator_accepts_valid():
    """ModelConfig.priority must accept 1, 3, 5, 7, 9."""
    for p in (1, 3, 5, 7, 9):
        m = ModelConfig(
            max_concurrency=2, max_queue_depth=10,
            max_wait_seconds=30, sync_timeout_seconds=30, priority=p,
        )
        assert m.priority == p


def test_max_concurrency_must_be_positive():
    """max_concurrency < 1 must raise a validation error."""
    with pytest.raises(Exception):
        ModelConfig(max_concurrency=0, max_queue_depth=10, priority=5)


def test_all_model_names():
    """all_model_names() returns exactly the keys in cfg.models."""
    cfg = load_config(_DEFAULT_CONFIG)
    assert set(cfg.all_model_names()) == set(cfg.models.keys())


def test_empty_yaml_uses_defaults(tmp_path):
    """An empty YAML file must produce a valid AppConfig with defaults."""
    cfg_file = tmp_path / "empty.yaml"
    cfg_file.write_text("")
    cfg = load_config(str(cfg_file))
    assert isinstance(cfg, AppConfig)
    assert cfg.defaults.max_concurrency == 3
