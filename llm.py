"""Shared LLM provider for the optional memory-mcp pipeline modules.

Used by `extract.py` (ingest_turn) and `verify.py` (fact cross-check). The
core server never talks to an LLM — these modules only load when the caller
sets MEMORY_MCP_EXTRACT=1 / MEMORY_MCP_VERIFY=1, and every failure degrades
to "pipeline off" (best-effort).

Env:
  MEMORY_MCP_LLM_PROVIDER   ollama|openai|test (default ollama)
  MEMORY_MCP_LLM_URL        base URL (ollama default http://localhost:11434)
  MEMORY_MCP_LLM_MODEL      model name (ollama default qwen2.5:14b)
  MEMORY_MCP_LLM_KEY        bearer token for openai-compatible endpoints
                           (only send over TLS — never plaintext http)
  MEMORY_MCP_LLM_TIMEOUT    seconds per call (default 60)
"""

import json
import os
import urllib.request

DEFAULT_URLS = {"ollama": "http://localhost:11434", "openai": "http://localhost:8000/v1"}
DEFAULT_MODELS = {"ollama": "qwen2.5:14b", "openai": "gpt-4o-mini", "test": "test-chat-v1"}
MAX_RETRIES = 3


def provider():
    p = (os.environ.get("MEMORY_MCP_LLM_PROVIDER") or "ollama").strip().lower()
    return p if p in DEFAULT_MODELS else "ollama"


def model_name():
    return os.environ.get("MEMORY_MCP_LLM_MODEL") or DEFAULT_MODELS[provider()]


def base_url():
    return os.environ.get("MEMORY_MCP_LLM_URL") or DEFAULT_URLS.get(provider(), "http://localhost:11434")


def api_key():
    return os.environ.get("MEMORY_MCP_LLM_KEY") or ""


def _timeout():
    try:
        return max(10, int(os.environ.get("MEMORY_MCP_LLM_TIMEOUT", "60")))
    except ValueError:
        return 60


def _http_json(url, payload, headers=None):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=_timeout()) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _chat_ollama(messages):
    out = _http_json(base_url().rstrip("/") + "/api/chat", {
        "model": model_name(), "messages": messages, "stream": False,
        "format": "json", "think": False})  # qwen3: disable reasoning tokens (CPU speed)
    return out["message"]["content"]


def _chat_openai(messages):
    headers = {"Authorization": "Bearer " + api_key()} if api_key() else {}
    out = _http_json(base_url().rstrip("/") + "/chat/completions", {
        "model": model_name(), "messages": messages, "temperature": 0,
        "response_format": {"type": "json_object"}}, headers)
    return out["choices"][0]["message"]["content"]


def _chat_test(messages):
    """Deterministic fake for tests: answers from markers in the transcript.

    - lines starting with `FACT: ` become extracted facts;
    - a verify prompt whose "New fact" contains the word "supersede" yields a
      supersedes verdict against the stored candidate sharing the most words.
    """
    import re
    joined = "\n".join(m.get("content", "") for m in messages)
    facts = [{"text": line[6:].strip(), "type": "project", "trust": "medium",
              "strong": False, "scope": "project", "importance": 0.7}
             for line in joined.splitlines() if line.startswith("FACT: ")]
    if facts:
        return json.dumps({"facts": facts})
    m = re.search(r"New fact:\s*(.+)", joined)
    if m and "supersede" in m.group(1).lower():
        cands = re.findall(r"- id=(\d+): (.+)", joined)
        # the fact under test is itself in the store (it was ingested before
        # verification); exclude it so the verdict targets a real older fact
        cands = [c for c in cands if c[1].strip() != m.group(1).strip()]
        words = set(re.findall(r"[a-z]+", m.group(1).lower()))
        best = max(cands, key=lambda c: len(words & set(re.findall(r"[a-z]+", c[1].lower()))),
                   default=None)
        if best:
            return json.dumps({"action": "supersedes", "target_id": int(best[0]),
                               "reason": "test provider", "confidence": 1.0})
    return json.dumps({"action": "add", "target_id": None, "reason": "",
                       "confidence": 1.0})


def chat_json(messages):
    """One JSON-mode chat completion. Raises on provider failure; callers treat
    failures as 'pipeline off for this call'."""
    p = provider()
    content = _chat_test(messages) if p == "test" else (
        _chat_ollama(messages) if p == "ollama" else _chat_openai(messages))
    return json.loads(content)
