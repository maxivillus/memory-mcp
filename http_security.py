"""Shared outbound HTTP policy for optional provider integrations."""

import os
from urllib.parse import urlsplit


def validate_http_url(url, headers=None):
    """Reject malformed targets and credential-bearing plaintext HTTP.

    Unauthenticated local HTTP remains supported for Ollama and compatible
    development servers. A bearer token over HTTP requires an explicit opt-in
    because it is otherwise observable by every network hop.
    """
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("provider URL must use http:// or https://")
    has_authorization = any(str(key).lower() == "authorization"
                            for key in (headers or {}))
    if (parsed.scheme == "http" and has_authorization and
            os.environ.get("MEMORY_MCP_ALLOW_INSECURE_HTTP") != "1"):
        raise RuntimeError(
            "refusing to send Authorization over plaintext HTTP; use HTTPS or "
            "set MEMORY_MCP_ALLOW_INSECURE_HTTP=1 for an explicit exception")
