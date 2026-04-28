"""HTTP client for the local LLM (Ollama by default, easily swappable to vLLM).

Public surface: a single synchronous `generate(prompt) -> str`. The convertor's main
path is rule-based, so the LLM is only consulted for fallback gaps and the optional
final polish step. Errors surface as `LLMUnavailableError` so callers can degrade
gracefully (fall back to rule output without LLM polish).
"""

from __future__ import annotations

import httpx

from ibm_network.config.settings import Settings, get_settings


class LLMUnavailableError(RuntimeError):
    pass


class LLMClient:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def generate(self, prompt: str, *, system: str | None = None) -> str:
        url = f"{self.settings.ollama_url.rstrip('/')}/api/generate"
        body: dict[str, object] = {
            "model": self.settings.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1},
        }
        if system:
            body["system"] = system
        try:
            resp = httpx.post(url, json=body, timeout=self.settings.llm_timeout_seconds)
            resp.raise_for_status()
        except (httpx.HTTPError, httpx.RequestError) as e:
            raise LLMUnavailableError(f"local LLM unreachable at {url}: {e}") from e

        data = resp.json()
        text = data.get("response")
        if not isinstance(text, str):
            raise LLMUnavailableError(f"unexpected LLM response shape: {data!r}")
        return text
