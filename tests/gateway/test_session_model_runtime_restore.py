"""Regression tests for durable gateway /model runtime overrides."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

from gateway.config import Platform
from gateway.session import SessionEntry, SessionSource, build_session_key
from hermes_cli.model_switch import ModelSwitchResult


ZAI_BASE_URL = "https://api.z.ai/api/coding/paas/v4"


def _source() -> SessionSource:
    return SessionSource(
        platform=Platform.TELEGRAM,
        user_id="u1",
        chat_id="c1",
        user_name="tester",
        chat_type="dm",
    )


def _runner(session_id: str = "sess-zai"):
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner._session_model_overrides = {}
    src = _source()
    session_key = build_session_key(src)
    entry = SessionEntry(
        session_key=session_key,
        session_id=session_id,
        created_at=datetime.now(),
        updated_at=datetime.now(),
        platform=Platform.TELEGRAM,
        chat_type="dm",
    )
    runner.session_store = MagicMock()
    runner.session_store._entries = {session_key: entry}
    runner.session_store.get_or_create_session.return_value = entry
    return runner, src, session_key, entry


class FakeRuntimeDB:
    def __init__(self, row=None):
        self.row = row or {}
        self.update = None

    def get_session(self, session_id):
        return self.row

    def update_session_runtime(
        self,
        session_id,
        model_config_json,
        model=None,
        *,
        billing_provider=None,
        billing_base_url=None,
        billing_mode=None,
    ):
        self.update = {
            "session_id": session_id,
            "model_config": json.loads(model_config_json),
            "model": model,
            "billing_provider": billing_provider,
            "billing_base_url": billing_base_url,
            "billing_mode": billing_mode,
        }


def test_model_switch_persists_non_secret_runtime_bundle_and_busts_billing_route():
    runner, src, _session_key, _entry = _runner()
    db = FakeRuntimeDB(
        {
            "model_config": json.dumps(
                {
                    "max_iterations": 200,
                    "reasoning_config": {"enabled": True, "effort": "high"},
                    "api_key": "must-not-survive",
                }
            ),
            "billing_provider": "openai-codex",
            "billing_base_url": "https://chatgpt.com/backend-api/codex",
        }
    )
    runner._session_db = db

    result = ModelSwitchResult(
        success=True,
        new_model="glm-5.2",
        target_provider="zai",
        api_key="secret-runtime-key",
        base_url=ZAI_BASE_URL,
        api_mode="chat_completions",
    )

    runner._persist_session_model_runtime_override(src, result)

    assert db.update is not None
    assert db.update["session_id"] == "sess-zai"
    assert db.update["model"] == "glm-5.2"
    assert db.update["billing_provider"] == "zai"
    assert db.update["billing_base_url"] == ZAI_BASE_URL
    assert db.update["billing_mode"] == "unknown"
    assert db.update["model_config"]["model"] == "glm-5.2"
    assert db.update["model_config"]["provider"] == "zai"
    assert db.update["model_config"]["base_url"] == ZAI_BASE_URL
    assert db.update["model_config"]["api_mode"] == "chat_completions"
    assert db.update["model_config"]["reasoning_config"] == {
        "enabled": True,
        "effort": "high",
    }
    assert "api_key" not in db.update["model_config"]


def test_restart_restores_zai_runtime_from_session_model_config(monkeypatch):
    """A fresh GatewayRunner must resolve GLM 5.2/Z.AI, not global Codex."""
    runner, src, session_key, _entry = _runner()
    runner._session_db = FakeRuntimeDB(
        {
            "model": "glm-5.2",
            "model_config": json.dumps(
                {
                    "model": "glm-5.2",
                    "provider": "zai",
                    "base_url": ZAI_BASE_URL,
                    "api_mode": "chat_completions",
                }
            ),
            "billing_provider": "openai-codex",
            "billing_base_url": "https://chatgpt.com/backend-api/codex",
        }
    )

    calls = []

    def fake_resolve_runtime_agent_kwargs(**kwargs):
        calls.append(kwargs)
        return {
            "provider": "zai",
            "api_key": "resolved-zai-key",
            "base_url": ZAI_BASE_URL,
            "api_mode": "chat_completions",
            "max_tokens": None,
        }

    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda _cfg=None: "gpt-5.5")
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        fake_resolve_runtime_agent_kwargs,
    )

    model, runtime = runner._resolve_session_agent_runtime(source=src, user_config={})

    assert model == "glm-5.2"
    assert runtime["provider"] == "zai"
    assert runtime["api_key"] == "resolved-zai-key"
    assert runtime["base_url"] == ZAI_BASE_URL
    assert runtime["api_mode"] == "chat_completions"
    assert runner._session_model_overrides[session_key] == {
        "model": "glm-5.2",
        "provider": "zai",
        "base_url": ZAI_BASE_URL,
        "api_mode": "chat_completions",
    }
    assert calls == [
        {
            "requested_provider": "zai",
            "explicit_base_url": ZAI_BASE_URL,
            "target_model": "glm-5.2",
        }
    ]


def test_restart_restore_ignores_plain_model_without_runtime_route(monkeypatch):
    runner, src, _session_key, _entry = _runner()
    runner._session_db = FakeRuntimeDB(
        {
            "model": "glm-5.2",
            "model_config": json.dumps({"reasoning_config": {"effort": "high"}}),
        }
    )

    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda _cfg=None: "gpt-5.5")
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda **_kwargs: {
            "provider": "openai-codex",
            "api_key": "codex-key",
            "base_url": "https://chatgpt.com/backend-api/codex",
            "api_mode": "codex_responses",
        },
    )

    model, runtime = runner._resolve_session_agent_runtime(source=src, user_config={})

    assert model == "gpt-5.5"
    assert runtime["provider"] == "openai-codex"
    assert runner._session_model_overrides == {}
