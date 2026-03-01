"""Security middleware and utilities for API protection.

Provides:
- API key authentication (optional for local dev)
- Rate limiting
- Input validation
- Security headers
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.security import APIKeyHeader
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass
class RateLimitConfig:
    requests_per_minute: int = 60
    requests_per_hour: int = 1000
    burst_limit: int = 10
    trust_proxy: bool = False  # Only trust X-Forwarded-For behind a known proxy
    max_entries: int = 10_000  # Max unique IPs to track (memory DoS protection)


@dataclass
class RateLimitEntry:
    minute_count: int = 0
    hour_count: int = 0
    burst_count: int = 0
    last_minute: float = 0
    last_hour: float = 0
    last_burst_reset: float = 0


class RateLimiter:
    def __init__(self, config: RateLimitConfig | None = None) -> None:
        self.config = config or RateLimitConfig()
        self._clients: dict[str, RateLimitEntry] = {}
        self._request_count = 0  # Track requests for periodic cleanup

    def _get_client_id(self, request: Request) -> str:
        """Get client IP, only trusting X-Forwarded-For if trust_proxy is True."""
        # Only trust X-Forwarded-For behind a known proxy (e.g., nginx, cloudflare)
        if self.config.trust_proxy:
            forwarded = request.headers.get("X-Forwarded-For")
            if forwarded:
                return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _cleanup_stale_entries(self) -> None:
        """Remove entries that haven't been accessed in over an hour to prevent memory DoS."""
        now = time.time()
        stale_threshold = 3600  # 1 hour
        stale_keys = [
            k
            for k, v in self._clients.items()
            if now - max(v.last_minute, v.last_hour, v.last_burst_reset) > stale_threshold
        ]
        for k in stale_keys:
            del self._clients[k]

    def is_allowed(self, request: Request) -> tuple[bool, str]:
        client_id = self._get_client_id(request)

        # Enforce max entries limit (memory DoS protection)
        if len(self._clients) >= self.config.max_entries and client_id not in self._clients:
            return False, "Rate limit capacity exceeded (too many unique clients)"

        # Get or create entry for this client
        if client_id not in self._clients:
            self._clients[client_id] = RateLimitEntry()
        entry = self._clients[client_id]

        now = time.time()

        current_minute = int(now // 60)
        current_hour = int(now // 3600)

        if int(entry.last_minute // 60) != current_minute:
            entry.minute_count = 0
            entry.last_minute = now

        if int(entry.last_hour // 3600) != current_hour:
            entry.hour_count = 0
            entry.last_hour = now

        if now - entry.last_burst_reset > 1:
            entry.burst_count = 0
            entry.last_burst_reset = now

        if entry.burst_count >= self.config.burst_limit:
            return False, f"Burst limit exceeded ({self.config.burst_limit}/sec)"

        if entry.minute_count >= self.config.requests_per_minute:
            return False, f"Rate limit exceeded ({self.config.requests_per_minute}/min)"

        if entry.hour_count >= self.config.requests_per_hour:
            return False, f"Hourly limit exceeded ({self.config.requests_per_hour}/hour)"

        entry.burst_count += 1
        entry.minute_count += 1
        entry.hour_count += 1

        # Periodic cleanup (every 100 requests) to prevent unbounded growth
        self._request_count += 1
        if self._request_count % 100 == 0:
            self._cleanup_stale_entries()

        return True, ""


class SecurityMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        rate_limiter: RateLimiter | None = None,
        require_api_key: bool = False,
        api_keys: set[str] | None = None,
    ) -> None:
        super().__init__(app)
        self.rate_limiter = rate_limiter or RateLimiter()
        self.require_api_key = require_api_key
        self.api_keys = api_keys or set()
        self._exempt_paths = {"/health", "/docs", "/openapi.json", "/redoc"}

    def _get_api_key(self, request: Request) -> str | None:
        return request.headers.get("X-API-Key")

    def _is_exempt_path(self, path: str) -> bool:
        return path in self._exempt_paths or path.startswith("/docs") or path.startswith("/openapi")

    async def dispatch(self, request: Request, call_next):
        if not self._is_exempt_path(request.url.path):
            allowed, reason = self.rate_limiter.is_allowed(request)
            if not allowed:
                logger.warning(
                    f"Rate limit blocked: {request.client.host if request.client else 'unknown'} - {reason}"
                )
                return JSONResponse(
                    status_code=429,
                    content={"error": "Rate limit exceeded", "detail": reason},
                    headers={"Retry-After": "60"},
                )

            if self.require_api_key:
                api_key = self._get_api_key(request)
                if not api_key:
                    return JSONResponse(
                        status_code=401,
                        content={"error": "API key required"},
                    )
                if api_key not in self.api_keys:
                    logger.warning(
                        f"Invalid API key attempt from {request.client.host if request.client else 'unknown'}"
                    )
                    return JSONResponse(
                        status_code=403,
                        content={"error": "Invalid API key"},
                    )

        response = await call_next(request)

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        # Content-Security-Policy for API endpoints (restricts resource loading)
        response.headers["Content-Security-Policy"] = "default-src 'self'"

        return response


def validate_date_string(date_str: str) -> bool:
    import re

    if not date_str:
        return False
    pattern = r"^\d{4}-\d{2}-\d{2}$"
    if not re.match(pattern, date_str):
        return False
    return True


def sanitize_symbol(symbol: str) -> str:
    import re

    if not symbol:
        return ""
    sanitized = re.sub(r"[^A-Z0-9\-]", "", symbol.upper())
    return sanitized[:20]


def sanitize_symbols_csv(symbols_csv: str) -> list[str]:
    if not symbols_csv:
        return []
    parts = symbols_csv.split(",")
    return [sanitize_symbol(p.strip()) for p in parts if p.strip()][:50]


def validate_positive_int(value: int | None, max_value: int = 10000) -> int | None:
    if value is None:
        return None
    if value < 0:
        raise ValueError("Value must be positive")
    if value > max_value:
        raise ValueError(f"Value exceeds maximum ({max_value})")
    return value


def validate_hash(hash_str: str | None) -> str | None:
    if not hash_str:
        return None
    import re

    if not re.match(r"^[a-f0-9]{8,64}$", hash_str.lower()):
        raise ValueError("Invalid hash format")
    return hash_str.lower()


def get_security_config() -> dict[str, Any]:
    return {
        "require_api_key": os.getenv("API_KEY_REQUIRED", "false").lower() == "true",
        "api_keys": {k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()},
        "rate_limit_rpm": int(os.getenv("RATE_LIMIT_RPM", "60")),
        "rate_limit_hourly": int(os.getenv("RATE_LIMIT_HOURLY", "1000")),
        "trust_proxy": os.getenv("TRUST_PROXY", "false").lower() == "true",
    }


def create_security_middleware(app) -> SecurityMiddleware:
    config = get_security_config()
    rate_limiter = RateLimiter(
        RateLimitConfig(
            requests_per_minute=config["rate_limit_rpm"],
            requests_per_hour=config["rate_limit_hourly"],
            trust_proxy=config["trust_proxy"],
        )
    )

    # Warn if authentication is disabled
    if not config["require_api_key"]:
        logger.warning(
            "API key authentication is DISABLED. Set API_KEY_REQUIRED=true for production."
        )

    # Warn if trusting X-Forwarded-For without explicit config
    if config["trust_proxy"]:
        logger.warning(
            "Trusting X-Forwarded-For header for rate limiting. "
            "Only enable this behind a known proxy."
        )

    return SecurityMiddleware(
        app,
        rate_limiter=rate_limiter,
        require_api_key=config["require_api_key"],
        api_keys=config["api_keys"],
    )
