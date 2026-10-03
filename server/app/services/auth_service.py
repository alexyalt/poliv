"""Сервис авторизации и сессий (Этап 1).

- логин/выход;
- сессии в БД (хранится только хеш токена);
- ограничение частоты попыток входа (login_rate_limit);
- создание первого администратора из конфига;
- журнал действий (таблица logs).
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from ..infra.config import Config
from ..infra.logging import get_logger
from ..infra.security import (
    expires_iso,
    generate_password,
    hash_password,
    hash_token,
    new_session_token,
    utcnow_iso,
    verify_password,
)

log = get_logger("poliv.auth")

SESSION_COOKIE = "poliv_session"


class AuthService:
    def __init__(self, conn: sqlite3.Connection, cfg: Config):
        self.conn = conn
        self.cfg = cfg
        # ip -> список меток неудачных попыток (в памяти, достаточно для Этапа 1)
        self._failed: dict[str, list[float]] = {}

    # ------------------------------------------------------------------ users
    def ensure_admin(self) -> Optional[str]:
        """Создаёт первого администратора из конфига. Возвращает сгенерированный пароль."""
        row = self.conn.execute(
            "SELECT id FROM users WHERE username = ?", (self.cfg.admin_username,)
        ).fetchone()
        if row:
            return None
        generated: Optional[str] = None
        password = self.cfg.admin_password
        if not password:
            password = generated = generate_password()
        with self.conn:
            self.conn.execute(
                """INSERT INTO users(username, password_hash, role, enabled,
                                      must_change_password, created_at)
                   VALUES (?, ?, 'admin', 1, ?, ?)""",
                (
                    self.cfg.admin_username,
                    hash_password(password),
                    1 if self.cfg.admin_must_change_password else 0,
                    utcnow_iso(),
                ),
            )
        self.write_log(None, self.cfg.admin_username, "admin.created", "user", None, source="system")
        log.info("Создан первый администратор: %s", self.cfg.admin_username)
        if generated:
            log.warning(
                "Пароль администратора %s: %s (сохраните и смените при первом входе)",
                self.cfg.admin_username,
                generated,
            )
        return generated

    def change_password(self, user_id: int, new_password: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE users SET password_hash = ?, must_change_password = 0 WHERE id = ?",
                (hash_password(new_password), user_id),
            )

    # ------------------------------------------------------------------ login
    def _rate_limited(self, ip: str) -> bool:
        import time

        now = time.monotonic()
        window = 60.0
        attempts = [t for t in self._failed.get(ip, []) if now - t < window]
        self._failed[ip] = attempts
        return len(attempts) >= self.cfg.login_rate_limit

    def _register_failure(self, ip: str) -> None:
        import time

        self._failed.setdefault(ip, []).append(time.monotonic())

    def _clear_failures(self, ip: str) -> None:
        self._failed.pop(ip, None)

    def login(
        self, username: str, password: str, ip: str, user_agent: str
    ) -> tuple[Optional[str], Optional[dict[str, Any]], str]:
        """Возвращает (token, user, message). token=None — вход не выполнен."""
        if self._rate_limited(ip):
            return None, None, "Слишком много попыток входа. Подождите минуту."
        row = self.conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()
        if not row or not row["enabled"] or not verify_password(password, row["password_hash"]):
            self._register_failure(ip)
            self.write_log(None, username, "login.failed", "user", None, {"ip": ip})
            return None, None, "Неверный логин или пароль."
        self._clear_failures(ip)

        token = new_session_token()
        now = utcnow_iso()
        with self.conn:
            self.conn.execute(
                """INSERT INTO sessions(user_id, token_hash, created_at, expires_at,
                                        last_seen_at, ip, user_agent)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    row["id"],
                    hash_token(token),
                    now,
                    expires_iso(self.cfg.session_timeout_min),
                    now,
                    ip,
                    user_agent[:200],
                ),
            )
            self.conn.execute(
                "UPDATE users SET last_login_at = ? WHERE id = ?", (now, row["id"])
            )
        self.write_log(row["id"], row["username"], "login", "user", row["id"], {"ip": ip})
        return token, dict(row), ""

    def logout(self, token: str) -> None:
        user = self.get_user_by_token(token, touch=False)
        with self.conn:
            self.conn.execute(
                "DELETE FROM sessions WHERE token_hash = ?", (hash_token(token),)
            )
        if user:
            self.write_log(user["id"], user["username"], "logout", "user", user["id"])

    # ---------------------------------------------------------------- session
    def get_user_by_token(self, token: str, touch: bool = True) -> Optional[sqlite3.Row]:
        row = self.conn.execute(
            """SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id
               WHERE s.token_hash = ? AND s.expires_at > ? AND u.enabled = 1""",
            (hash_token(token), utcnow_iso()),
        ).fetchone()
        if row and touch:
            with self.conn:
                self.conn.execute(
                    """UPDATE sessions SET last_seen_at = ?, expires_at = ?
                       WHERE token_hash = ?""",
                    (utcnow_iso(), expires_iso(self.cfg.session_timeout_min), hash_token(token)),
                )
        return row

    def cleanup_sessions(self) -> None:
        with self.conn:
            self.conn.execute(
                "DELETE FROM sessions WHERE expires_at <= ?", (utcnow_iso(),)
            )

    # ------------------------------------------------------------------- logs
    def write_log(
        self,
        user_id: Optional[int],
        username: Optional[str],
        action: str,
        object_type: Optional[str] = None,
        object_id: Any = None,
        details: Optional[dict] = None,
        ip: Optional[str] = None,
        source: str = "web",
    ) -> None:
        import json

        with self.conn:
            self.conn.execute(
                """INSERT INTO logs(ts, user_id, username, action, object_type,
                                    object_id, details_json, ip, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    utcnow_iso(),
                    user_id,
                    username,
                    action,
                    object_type,
                    str(object_id) if object_id is not None else None,
                    json.dumps(details, ensure_ascii=False) if details else None,
                    ip,
                    source,
                ),
            )

    def recent_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
