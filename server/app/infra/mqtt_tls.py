"""Strict TLS setup shared by the runtime and connection diagnostics."""
import ssl
from pathlib import Path


def configure_tls(client, settings, project_root):
    enabled = settings.get('tls_enabled', False)
    if not isinstance(enabled, bool):
        raise ValueError('[mqtt].tls_enabled must be a boolean')
    if not enabled:
        return
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    ca_file = settings.get('tls_ca_file', '')
    if ca_file:
        path = Path(ca_file)
        if not path.is_absolute():
            path = Path(project_root) / path
        context.load_verify_locations(cafile=str(path))
    # Never tls_insecure_set: Python's default context requires hostname + chain verification.
    client.tls_set_context(context)
