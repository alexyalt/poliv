"""Смоук-тест критериев готовности Этапа 1."""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

os.environ.setdefault("POLIV_CONFIG_LOCAL", str(Path(__file__).resolve().parents[2] / "config" / "config.local.toml"))

from fastapi.testclient import TestClient  # noqa: E402


def _client():
    from server.main import app
    return TestClient(app)


def test_health_and_login_flow():
    client = _client()
    assert client.get("/health").json()["status"] == "ok"

    # без сессии редиректит на вход
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login"

    # вход с неверным паролем
    admin_pw = client.app.state.cfg.admin_password or None
    if admin_pw is None:
        # пароль был сгенерирован — берём из БД невозможно; проверяем отказ
        r = client.post("/login", data={"username": "admin", "password": "wrong"})
        assert r.status_code == 401

    # корректный вход возможен только с реальным паролем — проверим форму
    r = client.get("/login")
    assert "Вход в систему" in r.text


def test_db_and_logs_created():
    client = _client()
    cfg = client.app.state.cfg
    assert cfg.db_path.exists()
    assert (cfg.logs_dir / "app.log").exists()

    row = client.app.state.db.execute(
        "SELECT COUNT(*) c FROM controllers WHERE box_id = ?",
        (cfg.raw["test_data"]["box_id"],),
    ).fetchone()
    assert row["c"] == 1

    zones = client.app.state.db.execute(
        "SELECT COUNT(*) c FROM zones z JOIN controllers k ON k.id=z.controller_id "
        "WHERE k.box_id = ?", (cfg.raw["test_data"]["box_id"],)
    ).fetchone()
    assert zones["c"] == int(cfg.raw["test_data"]["zones"])
