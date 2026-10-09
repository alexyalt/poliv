"""CRUD пользователей (Этап 2).

Пользователи: admin / operator / viewer. Правила:
- нельзя удалить или отключить последнего активного администратора;
- нельзя снять роль администратора с последнего администратора;
- пароль генерируется при создании без пароля и печатается в лог один раз;
- все действия пишутся в журнал logs (через AuthService.write_log).
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from ..infra.logging import get_logger
from ..infra.security import generate_password, hash_password, utcnow_iso
from .auth_service import AuthService
from .catalog_service import ValidationError, _clean_str, _parse_bool

log = get_logger("poliv.users")

ROLES = ("admin", "operator", "viewer")


class UserService:
    def __init__(self, conn: sqlite3.Connection, auth: AuthService):
        self.conn = conn
        self.auth = auth

    def list_users(self) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT id, username, role, enabled, must_change_password, "
                "last_login_at, created_at FROM users ORDER BY username"
            ).fetchall()
        ]

    def get_user(self, user_id: int) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT id, username, role, enabled, must_change_password, "
            "last_login_at, created_at FROM users WHERE id=?",
            (user_id,),
        ).fetchone()
        return dict(row) if row else None

    def _active_admins_count(self) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) c FROM users WHERE role='admin' AND enabled=1"
        ).fetchone()["c"]

    def create_user(self, data: dict, actor: str) -> tuple[dict, Optional[str]]:
        """Создаёт пользователя. Возвращает (user, сгенерированный_пароль|None)."""
        username = _clean_str(data.get("username"), "Логин", 50)
        if not username.replace("_", "").replace("-", "").isalnum():
            raise ValidationError("Логин: только буквы, цифры, дефис, подчёркивание")
        role = str(data.get("role") or "").strip()
        if role not in ROLES:
            raise ValidationError(f"Роль должна быть одной из: {', '.join(ROLES)}")
        password = str(data.get("password") or "")
        generated: Optional[str] = None
        if not password:
            password = generated = generate_password()
        elif len(password) < 6:
            raise ValidationError("Пароль не короче 6 символов")
        try:
            with self.conn:
                self.conn.execute(
                    """INSERT INTO users(username, password_hash, role, enabled,
                                         must_change_password, created_at)
                       VALUES (?, ?, ?, 1, 1, ?)""",
                    (username, hash_password(password), role, utcnow_iso()),
                )
        except sqlite3.IntegrityError:
            raise ValidationError(f"Пользователь «{username}» уже существует")
        self.auth.write_log(None, actor, "user.created", "user", username)
        log.info("Пользователь %s создан администратором %s", username, actor)
        row = self.conn.execute(
            "SELECT id, username, role, enabled, must_change_password, created_at "
            "FROM users WHERE username=?",
            (username,),
        ).fetchone()
        return dict(row), generated

    def update_user(self, user_id: int, data: dict, actor: str) -> dict:
        user = self.get_user(user_id)
        if not user:
            raise ValidationError("Пользователь не найден")
        role = str(data.get("role") or user["role"]).strip()
        if role not in ROLES:
            raise ValidationError("Некорректная роль")
        enabled = _parse_bool(data.get("enabled", user["enabled"]))
        # защита от самоблокировки системы
        last_admin = user["role"] == "admin" and user["enabled"] == 1
        if last_admin and (role != "admin" or not enabled):
            if self._active_admins_count() <= 1:
                raise ValidationError(
                    "Нельзя снять роль или отключить последнего активного администратора"
                )
        new_password = str(data.get("password") or "").strip()
        if new_password and len(new_password) < 6:
            raise ValidationError("Пароль: минимум 6 символов")
        with self.conn:
            self.conn.execute(
                "UPDATE users SET role=?, enabled=? WHERE id=?", (role, enabled, user_id)
            )
            if new_password:
                self.conn.execute(
                    "UPDATE users SET password_hash=?, must_change_password=0 WHERE id=?",
                    (hash_password(new_password), user_id),
                )
                # смена пароля администратором — инвалидируем сессии пользователя
                self.conn.execute(
                    "DELETE FROM sessions WHERE user_id=?", (user_id,)
                )
        self.auth.write_log(None, actor, "user.updated", "user", user["username"],
                            {"role": role, "enabled": enabled,
                             "password_changed": bool(new_password)})
        return self.get_user(user_id)  # type: ignore[return-value]

    def reset_password(self, user_id: int, actor: str) -> Optional[str]:
        user = self.get_user(user_id)
        if not user:
            raise ValidationError("Пользователь не найден")
        new_password = generate_password()
        with self.conn:
            self.conn.execute(
                "UPDATE users SET password_hash=?, must_change_password=1 WHERE id=?",
                (hash_password(new_password), user_id),
            )
            self.conn.execute(
                "DELETE FROM sessions WHERE user_id=?", (user_id,)
            )
        self.auth.write_log(None, actor, "user.password_reset", "user", user["username"])
        log.info("Сброс пароля пользователя %s (выполнен %s)", user["username"], actor)
        return new_password

    def delete_user(self, user_id: int, actor: str) -> None:
        """Удаление физическое, но нельзя удалить себя или последнего админа."""
        user = self.get_user(user_id)
        if not user:
            raise ValidationError("Пользователь не найден")
        if user["username"] == actor:
            raise ValidationError("Нельзя удалить самого себя")
        if user["role"] == "admin" and user["enabled"] == 1:
            if self._active_admins_count() <= 1:
                raise ValidationError("Нельзя удалить последнего активного администратора")
        with self.conn:
            self.conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
            self.conn.execute("DELETE FROM users WHERE id=?", (user_id,))
        self.auth.write_log(None, actor, "user.deleted", "user", user["username"])
        log.info("Пользователь %s удалён администратором %s", user["username"], actor)
