"""Регресс-тесты stage2_hotfix_v3 (аудит п. 2.3 — секреты вне URL; п. 3 — строгая
валидация JSON API). Веб-формы не затрагиваются: проверяются только API и редиректы.

Изоляция от боевой БД — как в test_stage2.py (_fresh_client/_admin_client).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from server.tests.test_stage2 import _admin_client, _url  # noqa: E402


def _create_zone(client, controller_id: int, name: str = "Зона тест", **extra) -> dict:
    payload = {"name": name, "controller_id": controller_id, "zone_number": 1, **extra}
    r = client.post("/api/zones", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


def _create_controller(client, box_id: str) -> dict:
    # name обязателен (правка валидации); box_id нормализуется в верхний регистр,
    # поэтому уникальные имена боксов на один тестовый клиент.
    r = client.post("/api/controllers", json={"name": f"Бокс {box_id}", "box_id": box_id})
    assert r.status_code == 201, r.text
    return r.json()


# ---------------------------------------------------------------- БЛОК 3: API
def test_api_zone_number_float_rejected():
    """zone_number=1.9 через API -> 422 «Ожидается целое число»."""
    client = _admin_client()
    c = _create_controller(client, "B1")
    r = client.post("/api/zones", json={
        "name": "Зона", "controller_id": c["id"], "zone_number": 1.9,
    })
    assert r.status_code == 422
    assert "целое число" in r.json()["detail"]
    # строка с точкой тоже отклоняется
    r2 = client.post("/api/zones", json={
        "name": "Зона", "controller_id": c["id"], "zone_number": "1.9",
    })
    assert r2.status_code == 422
    # допустимый целочисленный float проходит
    r3 = client.post("/api/zones", json={
        "name": "Зона", "controller_id": c["id"], "zone_number": 2.0,
    })
    assert r3.status_code == 201, r3.text


def test_api_program_zones_bad_id_rejected():
    """zone_ids=["not-an-id"] -> 422 (не 500 и не молчаливый пропуск)."""
    client = _admin_client()
    c = _create_controller(client, "B1")
    z = _create_zone(client, c["id"])
    pr = client.post("/api/programs", json={
        "name": "Программа", "schedule_type": "weekdays", "weekdays": [1],
    })
    assert pr.status_code == 201, pr.text
    pid = pr.json()["id"]
    r = client.put(f"/api/programs/{pid}/zones", json={"zone_ids": ["not-an-id"]})
    assert r.status_code == 422, r.text
    # состав не изменился (молчаливого пропуска нет)
    p2 = client.get(f"/api/programs/{pid}").json()
    assert [zz["zone_id"] for zz in p2["zones"]] == []


def test_api_program_weekdays_not_list():
    """weekdays передан не списком (3) -> 422 «Ожидается список дней недели»."""
    client = _admin_client()
    r = client.post("/api/programs", json={
        "name": "Программа", "schedule_type": "weekdays", "weekdays": 3,
    })
    assert r.status_code == 422, r.text
    assert "список дней недели" in r.json()["detail"]


def test_api_settings_broken_json_400():
    """Битый JSON в PUT /api/settings/{key} -> 400, значение в БД не изменилось."""
    client = _admin_client()
    db = client.app.state.db
    before = db.execute(
        "SELECT value_json FROM settings WHERE key='adjustment.rain_delay_hours'"
    ).fetchone()[0]
    r = client.put(
        "/api/settings/adjustment.rain_delay_hours",
        content=b"{broken json",
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 400, r.text
    assert r.json()["detail"] == "Некорректный JSON"
    after = db.execute(
        "SELECT value_json FROM settings WHERE key='adjustment.rain_delay_hours'"
    ).fetchone()[0]
    assert after == before
    # JSON не-словарь (массив) -> тоже 400
    r2 = client.put("/api/settings/adjustment.rain_delay_hours", json=[1, 2])
    assert r2.status_code == 400


def test_api_settings_null_value_422():
    """{"value": null} в настройку -> 422, БД не меняется."""
    client = _admin_client()
    db = client.app.state.db
    before = db.execute(
        "SELECT value_json FROM settings WHERE key='adjustment.rain_delay_hours'"
    ).fetchone()[0]
    r = client.put("/api/settings/adjustment.rain_delay_hours", json={"value": None})
    assert r.status_code == 422, r.text
    after = db.execute(
        "SELECT value_json FROM settings WHERE key='adjustment.rain_delay_hours'"
    ).fetchone()[0]
    assert after == before


def test_api_settings_ranges_and_types():
    """Диапазоны/типы настроек: rain [0..720], temp_min [0.1..1.0],
    temp_max [1.0..5.0], soak bool."""
    client = _admin_client()
    bad = [
        ("adjustment.rain_delay_hours", 800),
        ("adjustment.temp_factor_min", 1.5),
        ("adjustment.temp_factor_max", 0.5),
        ("adjustment.soak_default_enabled", "yes"),
    ]
    for key, val in bad:
        r = client.put(f"/api/settings/{key}", json={"value": val})
        assert r.status_code == 422, f"{key}={val}: {r.status_code} {r.text}"
    good = [
        ("adjustment.rain_delay_hours", 36),
        ("adjustment.temp_factor_min", 0.5),
        ("adjustment.temp_factor_max", 2.0),
        ("adjustment.soak_default_enabled", True),
    ]
    for key, val in good:
        r = client.put(f"/api/settings/{key}", json={"value": val})
        assert r.status_code == 200, f"{key}={val}: {r.status_code} {r.text}"


# ------------------------------------------------------- БЛОК 1: секреты вне URL
def test_web_user_create_password_not_in_url():
    """POST /users/save (создание): пароль НЕ в Location и НЕ в URL редиректа;
    присутствует только в теле ответа страницы с Cache-Control: no-store."""
    client = _admin_client()
    r = client.post("/users/save", data={
        "username": "secretuser", "role": "viewer", "enabled": "on",
    }, follow_redirects=False)
    # POST -> 200 (рендер страницы напрямую), а не 302 с секретом в query
    assert r.status_code == 200, r.text
    location = r.headers.get("location", "")
    assert "ok=" not in location and "password" not in location.lower()
    body = r.text
    assert "Пароль показан один раз" in body or "показан один раз" in body
    assert r.headers.get("cache-control") == "no-store"
    # сам пароль показан ровно один раз на странице
    import re
    m = re.search(r'Пароль:\s*<code[^>]*>([^<]+)</code>', body)
    assert m, body[:500]
    generated = m.group(1).strip()
    assert len(generated) >= 8
    # ...и его нигде нет в URL-подобных constructах ответа
    assert "?ok=" not in body
    assert _url(r) == str(client.base_url) + "/users/save"


def test_web_user_edit_without_password_still_redirect():
    """Обычное редактирование пользователя (без генерации пароля) — 302 без секретов."""
    client = _admin_client()
    created = client.post("/api/users", json={
        "username": "editme", "role": "viewer", "password": "Passw0rd!",
    })
    assert created.status_code == 201, created.text
    uid = created.json()["id"]
    r = client.post("/users/save", data={
        "id": str(uid), "username": "editme", "role": "operator", "enabled": "on",
    }, follow_redirects=False)
    assert r.status_code == 302
    loc = r.headers["location"]
    assert "password" not in loc.lower()
    assert "Passw0rd" not in loc


# ------------------------------------------- БЛОК 3 (регресс hotfix): частичный PUT
def test_partial_put_zone_keeps_name_and_duration():
    """Частичный PUT зоны только с icon: name и base_duration сохранены (регресс)."""
    client = _admin_client()
    c = _create_controller(client, "B1")
    z = _create_zone(client, c["id"], name="Сад", base_duration_minutes=25, icon="🌿")
    r = client.put(f"/api/zones/{z['id']}", json={"icon": "🌷"})
    assert r.status_code == 200, r.text
    updated = r.json()
    assert updated["icon"] == "🌷"
    assert updated["name"] == "Сад"
    assert updated["base_duration_minutes"] == 25
