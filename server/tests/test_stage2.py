"""Тесты Этапа 2: справочники, CRUD, роли, валидация.

Изоляция от боевой БД: тесты работают на отдельном временном файле БД
(см. _isolate_db_path), чтобы артефакты прогонов не мешали повторному запуску.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

os.environ.setdefault(
    "POLIV_CONFIG_LOCAL",
    str(Path(__file__).resolve().parents[2] / "config" / "config.local.toml"),
)

from fastapi.testclient import TestClient  # noqa: E402


def _url(r) -> str:
    """Финальный URL ответа в виде строки (httpx.URL не поддерживает `in`)."""
    return str(r.url)


def _fresh_client():
    """Свежее приложение на временной БД (изоляция от боевой data/poliv.db)."""
    import tempfile

    from server.app.infra.config import load_config
    from server.main import create_app

    tmpdir = Path(tempfile.mkdtemp(prefix="poliv-test-"))
    cfg = load_config()
    type(cfg).db_path = property(lambda self: tmpdir / "test.db")
    app = create_app(cfg)
    client = TestClient(app)
    client._poliv_tmpdir = tmpdir  # для отладки/уборки
    return client


def _admin_client():
    """Клиент с входом администратора (пароль из config.local)."""
    client = _fresh_client()
    cfg = client.app.state.cfg
    password = cfg.admin_password
    if not password:
        # пароль был сгенерирован при первом старте — сбрасываем напрямую в БД
        from server.app.infra.security import hash_password

        password = "test-stage2-pw"
        with client.app.state.db:
            client.app.state.db.execute(
                "UPDATE users SET password_hash=?, must_change_password=0 WHERE username='admin'",
                (hash_password(password),),
            )
    r = client.post("/login", data={"username": "admin", "password": password})
    assert r.status_code == 200, r.text
    return client


def test_migrations_applied():
    client = _admin_client()
    db = client.app.state.db
    for table in ("programs", "program_zones", "zone_locks"):
        assert db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone(), f"таблица {table} не создана"
    cols = {c["name"] for c in db.execute("PRAGMA table_info(zones)")}
    assert "deleted_at" in cols
    # Правки v1/v2 Этапа 2: сезонность убрана из схемы — колонок season_* нет.
    assert not ({"season_start", "season_end"} & cols)
    cols_p = {c["name"] for c in db.execute("PRAGMA table_info(programs)")}
    assert not ({"season_start", "season_end"} & cols_p)
    cols_pz = {c["name"] for c in db.execute("PRAGMA table_info(program_zones)")}
    assert "parallel_group" in cols_pz  # ADR-12
    cols_c = {c["name"] for c in db.execute("PRAGMA table_info(controllers)")}
    assert {"deleted_at", "model"} <= cols_c
    # stage2_hotfix (блок 1): миграция 0003 должна присутствовать и применяться
    # (на старых БД добавляет parallel_group и сносит легаси season_*).
    applied = {r["version"] for r in db.execute("SELECT version FROM schema_migrations")}
    assert "0003_stage2_hotfix" in applied, applied


def test_migration_0003_hotfix_legacy_db():
    """stage2_hotfix (блок 1): миграция 0003 чинит «старую» БД (0002 применена ДО правок).

    Моделируем разрыв схемы: в базе, где 0002 уже записана в schema_migrations,
    нет program_zones.parallel_group и остались zones/programs.season_*.
    Миграция 0003 должна идемпотентно привести схему к ожидаемой; повторный
    вызов на чистой схеме не падает. Правка самой 0002 запрещена.
    """
    import sqlite3 as _sqlite3

    def _loader(path):
        import importlib.util

        spec = importlib.util.spec_from_file_location("mig0003", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    from pathlib import Path as _P

    mig_path = (_P(__file__).resolve().parents[2]
                / "server" / "migrations" / "0003_stage2_hotfix.py")
    upgrade = _loader(mig_path).upgrade

    conn = _sqlite3.connect(":memory:")
    conn.row_factory = _sqlite3.Row
    # «Старая» схема: как после первоначальной (до-hotfix) версии 0002
    conn.execute("CREATE TABLE zones (id INTEGER PRIMARY KEY, name TEXT,"
                 " season_start TEXT, season_end TEXT)")
    conn.execute("CREATE TABLE programs (id INTEGER PRIMARY KEY, name TEXT,"
                 " season_start TEXT, season_end TEXT)")
    conn.execute("CREATE TABLE program_zones (program_id INTEGER, zone_id INTEGER,"
                 " seq INTEGER)")
    conn.execute("CREATE TABLE schema_migrations (version TEXT, applied_at TEXT)")
    conn.execute("INSERT INTO schema_migrations VALUES ('0001_initial','x')")
    conn.execute("INSERT INTO schema_migrations VALUES ('0002_stage2_catalogs','x')")

    upgrade(conn)

    pz_cols = {c["name"] for c in conn.execute("PRAGMA table_info(program_zones)")}
    assert "parallel_group" in pz_cols
    z_cols = {c["name"] for c in conn.execute("PRAGMA table_info(zones)")}
    p_cols = {c["name"] for c in conn.execute("PRAGMA table_info(programs)")}
    assert not ({"season_start", "season_end"} & z_cols)
    assert not ({"season_start", "season_end"} & p_cols)
    # идемпотентность: повторный вызов на новой схеме — без падения (no-op)
    upgrade(conn)
    conn.close()


def test_controller_crud_web():
    client = _admin_client()
    r = client.post(
        "/controllers/save",
        data={"name": "Тест-бокс", "box_id": "testbox-api-1", "model": "PolyBoks 2"},
    )
    assert r.status_code == 200 and "error" not in _url(r), _url(r)
    db = client.app.state.db
    row = db.execute(
        "SELECT id, box_id FROM controllers WHERE box_id='TESTBOX-API-1'"
    ).fetchone()
    assert row and row["box_id"] == "TESTBOX-API-1"  # upper-case нормализация
    cid = row["id"]
    # дубль box_id -> ошибка
    r = client.post("/controllers/save", data={"name": "Дубль", "box_id": "testbox-api-1"})
    assert "error=" in _url(r)
    # soft-delete контроллера без зон разрешён (зоны не удаляются — только отключение)
    r = client.post(f"/controllers/{cid}/delete")
    assert "error=" not in _url(r), _url(r)  # у нового контроллера зон нет -> ок
    # (у нового контроллера зон нет, поэтому удаление разрешено)
    row = db.execute("SELECT deleted_at FROM controllers WHERE id=?", (cid,)).fetchone()
    assert row["deleted_at"] is not None


def test_zone_rules():
    client = _admin_client()
    db = client.app.state.db
    # свой контроллер: на тестовом TESTBOX-0001 зоны 1..8 уже заняты тест-данными
    r = client.post("/controllers/save", data={"name": "Зоны-тест", "box_id": "ZONE-TEST-1"})
    assert "error=" not in _url(r), _url(r)
    ctrl = db.execute(
        "SELECT id FROM controllers WHERE deleted_at IS NULL AND box_id='ZONE-TEST-1'"
    ).fetchone()
    cid = ctrl["id"]
    r = client.post(
        "/zones/save",
        data={
            "controller_id": str(cid), "zone_number": "7", "name": "Газон перед домом",
            "base_duration_minutes": "15", "icon": "🌿", "enabled": "on",
            "watering_adjustment_percent": "120",
        },
    )
    assert "error=" not in _url(r), _url(r)
    zrow = db.execute(
        "SELECT id FROM zones WHERE controller_id=? AND zone_number=7", (cid,)
    ).fetchone()
    assert zrow
    zid = zrow["id"]
    # дубликат номера зоны -> ошибка
    r = client.post(
        "/zones/save",
        data={"controller_id": str(cid), "zone_number": "7", "name": "Дубль",
              "base_duration_minutes": "10", "enabled": "on"},
    )
    assert "error=" in _url(r)
    # Правки 3.5/3.10: сезон у зон убран из системы — невалид season_start
    # игнорируется (колонок season_* в схеме нет), зона сохраняется.
    r = client.post(
        "/zones/save",
        data={"id": str(zid), "controller_id": str(cid), "zone_number": "7",
              "name": "X", "base_duration_minutes": "10", "season_start": "13-01",
              "enabled": "on"},
    )
    assert "error=" not in _url(r), _url(r)
    # отключение (soft-delete): физически строка остаётся
    r = client.post(f"/zones/{zid}/delete")
    assert "ok=" in _url(r)
    row = db.execute("SELECT deleted_at, enabled FROM zones WHERE id=?", (zid,)).fetchone()
    assert row["deleted_at"] is not None and row["enabled"] == 0
    # Правка 3.9: отключённую зону можно включить обратно
    r = client.post(f"/zones/{zid}/enable")
    assert "ok=" in _url(r), _url(r)
    row = db.execute("SELECT deleted_at, enabled FROM zones WHERE id=?", (zid,)).fetchone()
    assert row["deleted_at"] is None and row["enabled"] == 1
    client.post(f"/zones/{zid}/delete")  # оставляем отключённой для проверки занятости номера
    # номер 7 остаётся занят даже после soft-delete
    r = client.post(
        "/zones/save",
        data={"controller_id": str(cid), "zone_number": "7", "name": "Снова 7",
              "base_duration_minutes": "10", "enabled": "on"},
    )
    assert "error=" in _url(r)


def test_program_weekdays_and_interval():
    client = _admin_client()
    db = client.app.state.db
    r = client.post(
        "/programs/save",
        data={
            "name": "Утро будних", "schedule_type": "weekdays", "start_time": "06:30",
            "weekdays": ["1", "2", "3", "4", "5"], "enabled": "on",
        },
    )
    assert "error=" not in _url(r), _url(r)
    p = db.execute("SELECT * FROM programs WHERE name='Утро будних'").fetchone()
    assert p["weekdays_mask"] == "0111110"
    assert p["start_time"] == "06:30"
    # интервальная программа
    r = client.post(
        "/programs/save",
        data={"name": "Раз в 3 дня", "schedule_type": "interval", "interval_days": "3",
              "start_time": "7:00", "enabled": "on"},
    )  # без поля enabled при редактировании программа остаётся включённой
    assert "error=" not in _url(r), _url(r)
    p2 = db.execute("SELECT * FROM programs WHERE name='Раз в 3 дня'").fetchone()
    assert p2["interval_days"] == 3 and p2["start_time"] == "07:00"
    # без дней недели и без интервала -> ошибка
    r = client.post(
        "/programs/save",
        data={"name": "Без расписания", "schedule_type": "weekdays", "start_time": "06:00"},
    )
    assert "error=" in _url(r)
    # пустая программа допустима (без зон)
    detail = client.get(f"/programs/{p2['id']}")
    assert detail.status_code == 200 and "Программа без зон" in detail.text
    # удаление программы
    r = client.post(f"/programs/{p2['id']}/delete")
    assert "ok=" in _url(r)
    assert db.execute("SELECT COUNT(*) c FROM programs WHERE id=?", (p2["id"],)).fetchone()["c"] == 0


def test_program_zones_order_and_multi_controller():
    client = _admin_client()
    db = client.app.state.db
    # программа и зоны создаются внутри теста (каждый тест на своей БД)
    r = client.post("/programs/save", data={"name": "Тест-программа", "schedule_type": "weekdays",
                                            "start_time": "06:00", "weekdays": ["1"],
                                            "enabled": "on"})
    assert "error=" not in _url(r), _url(r)
    prog = db.execute("SELECT id FROM programs WHERE name='Тест-программа'").fetchone()
    zones = db.execute(
        "SELECT id FROM zones WHERE deleted_at IS NULL ORDER BY id LIMIT 3"
    ).fetchall()
    ids = [str(z["id"]) for z in zones]
    r = client.post(f"/programs/{prog['id']}/zones", data={"zone_ids": list(reversed(ids))})
    assert "ok=" in _url(r)
    rows = db.execute(
        "SELECT zone_id, seq FROM program_zones WHERE program_id=? ORDER BY seq",
        (prog["id"],),
    ).fetchall()
    assert [r_["zone_id"] for r_ in rows] == list(reversed([int(i) for i in ids]))
    # дубликаты убираются
    r = client.post(
        f"/programs/{prog['id']}/zones",
        data={"zone_ids": [ids[0], ids[0]]},
    )
    rows = db.execute(
        "SELECT COUNT(*) c FROM program_zones WHERE program_id=?", (prog["id"],)
    ).fetchone()
    assert rows["c"] == 1


def test_users_crud_and_last_admin_protection():
    client = _admin_client()
    db = client.app.state.db
    r = client.post(
        "/users/save",
        data={"username": "operator1", "role": "operator", "password": "secret123"},
    )
    assert "error=" not in _url(r), _url(r)
    u = db.execute("SELECT id, role FROM users WHERE username='operator1'").fetchone()
    assert u and u["role"] == "operator"
    # последний админ не может быть отключён/понижен
    admin = db.execute("SELECT id FROM users WHERE username='admin'").fetchone()
    r = client.post(f"/users/{admin['id']}/delete")
    assert "error=" in _url(r)  # нельзя удалить себя
    r = client.post("/users/save", data={"id": str(admin["id"]), "username": "admin",
                                         "role": "viewer", "enabled": "on"})
    assert "error=" in _url(r)  # нельзя снять роль последнего админа
    # оператор не видит раздел пользователей
    from server.app.services.auth_service import SESSION_COOKIE

    c2 = TestClient(client.app)
    rr = c2.post("/login", data={"username": "operator1", "password": "secret123"})
    assert rr.status_code == 200
    page = c2.get("/users")
    assert page.status_code == 200
    assert "только администратору" in page.text or "Вход в систему" in page.text or "Пользователи" not in page.text
    # ...и его не видно в навигации оператора
    assert "/users" not in page.text.split("</nav>")[0]


def test_api_crud_and_roles():
    client = _admin_client()
    # API: создание контроллера
    r = client.post("/api/controllers", json={"name": "API box", "box_id": "API-BOX-9"})
    assert r.status_code == 201, r.text
    cid = r.json()["id"]
    # валидация
    r = client.post("/api/controllers", json={"name": "", "box_id": "API-BOX-9"})
    assert r.status_code == 422
    # чтение viewer-ролью через API под админом — ок
    r = client.get("/api/controllers")
    assert any(c["id"] == cid for c in r.json())
    # настройки доступны для чтения всем вошедшим
    r = client.get("/api/settings")
    assert r.status_code == 200 and len(r.json()) >= 4
    key = r.json()[0]["key"]
    r = client.put(f"/api/settings/{key}", json={"value": "48"})
    assert r.status_code == 200
    # оператор не может писать через API
    client.post("/users/save", data={"username": "viewer1", "role": "viewer", "password": "secret123"})
    c2 = TestClient(client.app)
    c2.post("/login", data={"username": "viewer1", "password": "secret123"})
    r = c2.get("/api/zones")
    assert r.status_code == 200
    r = c2.post("/api/zones", json={"controller_id": cid, "zone_number": 1, "name": "x"})
    assert r.status_code == 403
    # неавторизованный API -> 401
    c3 = TestClient(client.app)
    assert c3.get("/api/controllers").status_code == 401


def test_settings_page_and_storage():
    client = _admin_client()
    r = client.get("/settings")
    assert r.status_code == 200 and "rain_delay_hours" in r.text
    r = client.post("/settings/save", data={"setting:adjustment.rain_delay_hours": "36"})
    assert "ok=" in _url(r)
    val = client.app.state.db.execute(
        "SELECT value_json FROM settings WHERE key='adjustment.rain_delay_hours'"
    ).fetchone()
    assert val["value_json"] == '"36"' or val["value_json"] == "36"


def test_logs_recorded_for_crud():
    client = _admin_client()
    # выполняем цепочку действий в изолированной БД этого теста
    r = client.post("/controllers/save", data={"name": "Лог-бокс", "box_id": "LOG-BOX-1"})
    assert "error=" not in _url(r), _url(r)
    db = client.app.state.db
    cid = db.execute("SELECT id FROM controllers WHERE box_id='LOG-BOX-1'").fetchone()["id"]
    r = client.post("/zones/save", data={"controller_id": str(cid), "zone_number": "1",
                                         "name": "Зона для журнала",
                                         "base_duration_minutes": "10", "enabled": "on"})
    assert "error=" not in _url(r), _url(r)
    zid = db.execute("SELECT id FROM zones WHERE controller_id=?", (cid,)).fetchone()["id"]
    client.post(f"/zones/{zid}/delete")
    r = client.post("/programs/save", data={"name": "Лог-программа", "schedule_type": "weekdays",
                                            "start_time": "06:00", "weekdays": ["1"],
                                            "enabled": "on"})
    assert "error=" not in _url(r), _url(r)
    client.post("/users/save", data={"username": "loguser", "role": "viewer",
                                     "password": "secret123"})
    logs = client.app.state.auth.recent_logs(200)
    actions = {l["action"] for l in logs}
    assert {"controller.created", "zone.created", "zone.disabled", "program.created",
            "user.created"} <= actions, actions


def test_zone_icon_preserved_on_partial_update():
    """stage2_hotfix (блок 3): icon не должен теряться при сохранении зоны.

    Сценарий: создать зону с icon="🌿" -> изменить только name (частичный
    PUT без поля icon) -> сохранить -> icon в БД остаётся "🌿".
    Также проверяется полный POST формы (поле отправлено) и явная очистка "".
    """
    client = _admin_client()
    db = client.app.state.db
    r = client.post("/controllers/save", data={"name": "Хотфикс-бокс",
                                               "box_id": "HOTFIX-BOX-1"})
    assert "error=" not in _url(r), _url(r)
    cid = db.execute("SELECT id FROM controllers WHERE box_id='HOTFIX-BOX-1'"
                     ).fetchone()["id"]

    # 1) создание зоны с иконкой (API, JSON)
    import json as _json
    body = {"controller_id": cid, "zone_number": 15, "name": "Газон у дома",
            "base_duration_minutes": 10, "icon": "\U0001F33F"}
    r = client.post("/api/zones", content=_json.dumps(body))
    assert r.status_code in (200, 201), (r.status_code, r.text)
    zid = r.json()["id"]
    assert r.json()["icon"] == "\U0001F33F"

    # 2) частичное обновление: меняем только name — icon НЕ пришёл
    r = client.put(f"/api/zones/{zid}", content=_json.dumps({"name": "Газон перед домом"}))
    assert r.status_code == 200, (r.status_code, r.text)
    row = db.execute("SELECT name, icon FROM zones WHERE id=?", (zid,)).fetchone()
    assert row["name"] == "Газон перед домом"
    assert row["icon"] == "\U0001F33F", "иконка потерялась при частичном обновлении"

    # 3) веб-форма: сохранение с заполненным icon (полный POST) — значение сохраняется
    r = client.post("/zones/save", data={
        "id": str(zid), "controller_id": str(cid), "zone_number": "15",
        "name": "Газон перед домом", "base_duration_minutes": "12",
        "icon": "\U0001F33F", "enabled": "on",
    })
    assert "error=" not in _url(r), _url(r)
    row = db.execute("SELECT icon FROM zones WHERE id=?", (zid,)).fetchone()
    assert row["icon"] == "\U0001F33F"

    # 4) явное очищение: поле прислано пустым -> NULL (намерение пользователя).
    #    PUT — частичная операция, name не трогаем (он уже проверен шагом 2).
    r = client.put(f"/api/zones/{zid}", content=_json.dumps({"icon": ""}))
    assert r.status_code == 200, (r.status_code, r.text)
    row = db.execute("SELECT icon FROM zones WHERE id=?", (zid,)).fetchone()
    assert row["icon"] is None


def test_program_zones_parallel_group_ui_roundtrip():
    """stage2_hotfix (блок 4): UI-поля pg_<zone_id> сохраняют группы параллельности.

    Две зоны одного контроллера с группой "A" -> после сохранения состава
    parallel_group="A" читается из БД; пустая группа -> NULL (последовательно).
    """
    client = _admin_client()
    db = client.app.state.db
    r = client.post("/controllers/save", data={"name": "PG-бокс", "box_id": "PG-BOX-1"})
    assert "error=" not in _url(r), _url(r)
    cid = db.execute("SELECT id FROM controllers WHERE box_id='PG-BOX-1'").fetchone()["id"]
    zids = []
    for n in (1, 2, 3):
        r = client.post("/zones/save", data={
            "controller_id": str(cid), "zone_number": str(n),
            "name": f"Зона {n}", "base_duration_minutes": "10", "enabled": "on"})
        assert "error=" not in _url(r), _url(r)
        zids.append(db.execute(
            "SELECT id FROM zones WHERE controller_id=? AND zone_number=?",
            (cid, n)).fetchone()["id"])
    r = client.post("/programs/save", data={
        "name": "PG-программа", "schedule_type": "weekdays",
        "start_time": "06:00", "weekdays": ["1"], "enabled": "on"})
    assert "error=" not in _url(r), _url(r)
    pid = db.execute("SELECT id FROM programs WHERE name='PG-программа'").fetchone()["id"]

    # страница редактирования состава открывается без 500 (блок 2 косвенно);
    # редактор состава зон живёт на /programs/{id} (program_detail.html)
    r = client.get(f"/programs?edit={pid}")
    assert r.status_code == 200, r.status_code
    r = client.get(f"/programs/{pid}?edit_zones=1")
    assert r.status_code == 200, r.status_code
    # редактор содержит текстовые поля Группы параллельности pg_<zone_id>
    for zid in zids:
        assert f'name="pg_{zid}"' in r.text, f"нет поля pg_{zid} в program_detail"

    # сохраняем: зоны 1 и 2 — группа "A", зона 3 — без группы (последовательно)
    r = client.post(f"/programs/{pid}/zones", data={
        "zone_ids": [str(zids[0]), str(zids[1]), str(zids[2])],
        f"pg_{zids[0]}": "A", f"pg_{zids[1]}": "A", f"pg_{zids[2]}": "",
    })
    assert "error=" not in _url(r), _url(r)
    rows = db.execute(
        "SELECT zone_id, seq, parallel_group FROM program_zones WHERE program_id=?"
        " ORDER BY seq", (pid,)).fetchall()
    groups = {row["zone_id"]: row["parallel_group"] for row in rows}
    assert groups[zids[0]] == "A" and groups[zids[1]] == "A"
    assert groups[zids[2]] is None
    # предзаполнение полей group_of при повторном открытии редактора
    r = client.get(f"/programs/{pid}?edit_zones=1")
    assert r.status_code == 200
    assert f'value="A"' in r.text
