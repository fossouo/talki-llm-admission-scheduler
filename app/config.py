"""Pydantic models for YAML config and loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class FallbackPolicyConfig(BaseModel):
    """Fallback chain config — out of scope for v0.1 but YAML shape frozen."""

    enabled: bool = False
    trigger_on: list[str] = Field(default_factory=list)
    chain: list[dict[str, Any]] = Field(default_factory=list)


class ModelConfig(BaseModel):
    """Per-model capacity and timeout overrides."""

    max_concurrency: int = 3
    max_queue_depth: int = 50
    max_wait_seconds: int = 120
    sync_timeout_seconds: int = 120
    priority: int = 5
    fallback_policy: FallbackPolicyConfig = Field(default_factory=FallbackPolicyConfig)

    @field_validator("priority")
    @classmethod
    def priority_in_range(cls, v: int) -> int:
        """Priority must be a valid tier value (1, 3, 5, 7, 9)."""
        if v not in (1, 3, 5, 7, 9):
            raise ValueError(f"priority must be one of 1,3,5,7,9 — got {v}")
        return v

    @field_validator("max_concurrency", "max_queue_depth")
    @classmethod
    def positive_int(cls, v: int) -> int:
        """Concurrency and depth must be positive."""
        if v < 1:
            raise ValueError("must be >= 1")
        return v


class DefaultsConfig(BaseModel):
    """Default values applied when a model is not explicitly listed."""

    max_concurrency: int = 3
    max_queue_depth: int = 50
    max_wait_seconds: int = 120
    sync_timeout_seconds: int = 120
    priority: int = 5
    fallback_policy: FallbackPolicyConfig = Field(default_factory=FallbackPolicyConfig)


class SchedulerConfig(BaseModel):
    """Scheduler infrastructure settings."""

    redis_url: str = "redis://localhost:6379/0"
    backend_litellm_url: str = "http://xeon:4000"
    worker_poll_timeout_seconds: int = 5
    lease_watcher_interval_seconds: int = 10
    integrity_check_interval_seconds: int = 60
    log_dir: str = "/tmp/admission-scheduler"
    log_rotate_utc_midnight: bool = True


class AppConfig(BaseModel):
    """Root configuration object loaded from YAML."""

    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    models: dict[str, ModelConfig] = Field(default_factory=dict)

    def model_config_for(self, model: str) -> ModelConfig:
        """Return the ModelConfig for the given model alias, falling back to defaults."""
        if model in self.models:
            return self.models[model]
        # Build a ModelConfig from defaults
        return ModelConfig(
            max_concurrency=self.defaults.max_concurrency,
            max_queue_depth=self.defaults.max_queue_depth,
            max_wait_seconds=self.defaults.max_wait_seconds,
            sync_timeout_seconds=self.defaults.sync_timeout_seconds,
            priority=self.defaults.priority,
            fallback_policy=self.defaults.fallback_policy,
        )

    def all_model_names(self) -> list[str]:
        """Return all explicitly configured model names."""
        return list(self.models.keys())


def load_config(config_path: str) -> AppConfig:
    """Load and validate AppConfig from a YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with path.open("r") as fh:
        raw = yaml.safe_load(fh) or {}

    return AppConfig.model_validate(raw)
