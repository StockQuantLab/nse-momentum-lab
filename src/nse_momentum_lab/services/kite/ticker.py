from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from urllib.parse import urlencode

SubscriptionMode = Literal["ltp", "quote", "full"]


@dataclass(slots=True)
class KiteTickerSettings:
    api_key: str
    access_token: str
    max_tokens_per_connection: int = 3000
    mode: SubscriptionMode = "full"


@dataclass(slots=True)
class KiteSubscriptionBatch:
    tokens: list[int]
    mode: SubscriptionMode = "full"


@dataclass(slots=True)
class KiteTickerPlan:
    api_key: str
    access_token: str
    batches: list[KiteSubscriptionBatch] = field(default_factory=list)

    @property
    def websocket_url(self) -> str:
        return build_websocket_url(self.api_key, self.access_token)


@dataclass(slots=True)
class KiteFeedState:
    status: str = "DISCONNECTED"
    is_stale: bool = False
    subscribed_tokens: list[int] = field(default_factory=list)
    mode: SubscriptionMode = "full"
    last_tick_at: datetime | None = None
    last_quote_at: datetime | None = None
    last_bar_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    reconnect_attempts: int = 0
    metadata_json: dict[str, Any] = field(default_factory=dict)


def build_websocket_url(api_key: str, access_token: str) -> str:
    return f"wss://ws.kite.trade?{urlencode({'api_key': api_key, 'access_token': access_token})}"


def normalize_tokens(tokens: Iterable[int]) -> list[int]:
    unique = dict.fromkeys(int(token) for token in tokens if int(token) > 0)
    return list(unique.keys())


def chunk_instrument_tokens(tokens: Iterable[int], *, chunk_size: int = 3000) -> list[list[int]]:
    normalized = normalize_tokens(tokens)
    return [normalized[idx : idx + chunk_size] for idx in range(0, len(normalized), chunk_size)]


def plan_subscription_batches(
    tokens: Iterable[int],
    *,
    mode: SubscriptionMode = "full",
    chunk_size: int = 3000,
) -> list[KiteSubscriptionBatch]:
    return [
        KiteSubscriptionBatch(tokens=batch, mode=mode)
        for batch in chunk_instrument_tokens(tokens, chunk_size=chunk_size)
    ]


def build_subscription_frames(
    tokens: Iterable[int],
    *,
    mode: SubscriptionMode = "full",
    chunk_size: int = 3000,
) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for batch in chunk_instrument_tokens(tokens, chunk_size=chunk_size):
        frames.append({"a": "subscribe", "v": batch})
        frames.append({"a": "mode", "v": [mode, batch]})
    return frames
