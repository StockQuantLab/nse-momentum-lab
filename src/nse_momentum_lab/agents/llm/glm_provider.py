"""ZAI LLM provider using Phidata's Anthropic-compatible interface.

ZAI maps Anthropic model names to GLM models:
- claude-sonnet-4-5-20250929 → GLM-4.7 (default, recommended)
- claude-haiku-4-20250514 → GLM-4.5-Air (faster, cheaper)

Set in Doppler:
- ANTHROPIC_API_KEY = <your ZAI key>
- ANTHROPIC_BASE_URL = https://api.z.ai/api/anthropic
"""

from __future__ import annotations

import os

from phi.model.anthropic import Claude

# ZAI Anthropic-compatible endpoint
ZAI_BASE_URL = "https://api.z.ai/api/anthropic"
DEFAULT_MODEL = "claude-sonnet-4-5-20250929"  # Maps to GLM-4.7


def create_glm_model(
    model_id: str | None = None,
    api_key: str | None = None,
    temperature: float = 0.7,
    max_tokens: int = 2048,
) -> Claude:
    """
    Create a ZAI LLM model instance via Anthropic-compatible API.

    Args:
        model_id: Anthropic model name (ZAI will map to GLM)
                 - claude-sonnet-4-5-20250929 (GLM-4.7)
                 - claude-haiku-4-20250514 (GLM-4.5-Air)
        api_key: ZAI API key (from ANTHROPIC_API_KEY in Doppler)
        temperature: Sampling temperature
        max_tokens: Maximum tokens to generate

    Returns:
        Claude instance configured for ZAI

    Note:
        Requires ANTHROPIC_API_KEY and ANTHROPIC_BASE_URL set in Doppler.
    """
    resolved_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not resolved_key:
        raise ValueError("ANTHROPIC_API_KEY is required. Set it in Doppler.")

    return Claude(
        id=model_id or DEFAULT_MODEL,
        api_key=resolved_key,
        temperature=temperature,
        max_tokens=max_tokens,
    )


# Backward-compatible alias
GLMProvider = create_glm_model
