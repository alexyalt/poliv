"""Connection ownership by request/task and thread; no cross-request transactions."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import threading
import asyncio
from dataclasses import dataclass

from .db import connect_path

_scope = ContextVar('poliv_database_scope', default=None)


@dataclass
class _ConnectionState:
    connection: object
    depth: int = 0


def _owner():
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return threading.get_ident(), task


@contextmanager
def database_scope():
    if _scope.get() is not None:
        yield
        return
    connections = {}
    token = _scope.set(connections)
    try:
        yield
    finally:
        for conn in connections.values():
            conn.connection.close()  # rollback any uncommitted work
        _scope.reset(token)


def scoped_task(function):
    @wraps(function)
    def run(*args, **kwargs):
        with database_scope():
            return function(*args, **kwargs)
    return run


class ScopedConnection:
    """Compatibility facade: the same services resolve an owned native connection.

    A request scope may be copied into a threadpool; each thread has its own
    connection. Non-request users must use database_scope/scoped_task. The fallback
    is for startup and direct command-line use and is closed at application stop.
    """
    def __init__(self, path):
        self.path = str(path)
        self._fallback = {}
        self._lock = threading.RLock()

    def _state(self):
        registry = _scope.get()
        if registry is None:
            registry = self._fallback
        key = (id(self), *_owner())
        with self._lock:
            if key not in registry:
                registry[key] = _ConnectionState(connect_path(self.path))
            return registry[key]

    def native(self):
        return self._state().connection

    def __getattr__(self, name):
        return getattr(self.native(), name)

    def __enter__(self):
        conn = self.native()
        # Savepoints prevent nested service contexts committing their caller.
        state = self._state()
        depth = state.depth
        if depth:
            conn.execute(f'SAVEPOINT poliv_nested_{depth}')
        elif not conn.in_transaction:
            conn.execute('BEGIN')
        state.depth = depth + 1
        return self

    def __exit__(self, kind, value, trace):
        state = self._state()
        depth = state.depth - 1
        state.depth = depth
        conn = self.native()
        if depth:
            if kind:
                conn.execute(f'ROLLBACK TO poliv_nested_{depth}')
            conn.execute(f'RELEASE poliv_nested_{depth}')
        elif kind:
            conn.rollback()
        else:
            conn.commit()
        return False

    def close(self):
        with self._lock:
            for conn in self._fallback.values():
                conn.connection.close()
            self._fallback.clear()


class DatabaseScopeMiddleware:
    """ASGI scope spans response streaming and propagated threadpool contexts."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        with database_scope():
            await self.app(scope, receive, send)
