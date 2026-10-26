"""Connection ownership by request/task and thread; no cross-request transactions."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import threading

from .db import connect_path

_scope = ContextVar('poliv_database_scope', default=None)


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
            conn.close()  # rollback any uncommitted work
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
        self._depth = threading.local()

    def native(self):
        registry = _scope.get()
        if registry is None:
            registry = self._fallback
        key = (id(self), threading.get_ident())
        with self._lock:
            if key not in registry:
                registry[key] = connect_path(self.path)
            return registry[key]

    def __getattr__(self, name):
        return getattr(self.native(), name)

    def __enter__(self):
        conn = self.native()
        # Savepoints prevent nested service contexts committing their caller.
        depth = getattr(self._depth, 'value', 0)
        if depth:
            conn.execute(f'SAVEPOINT poliv_nested_{depth}')
        elif not conn.in_transaction:
            conn.execute('BEGIN')
        self._depth.value = depth + 1
        return self

    def __exit__(self, kind, value, trace):
        depth = self._depth.value - 1
        self._depth.value = depth
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
                conn.close()
            self._fallback.clear()


class DatabaseScopeMiddleware:
    """ASGI scope spans response streaming and propagated threadpool contexts."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        with database_scope():
            await self.app(scope, receive, send)
