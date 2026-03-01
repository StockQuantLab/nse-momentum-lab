"""Tests for api/security.py"""

from unittest.mock import Mock

import pytest

from nse_momentum_lab.api.security import (
    RateLimitConfig,
    RateLimitEntry,
    RateLimiter,
    SecurityMiddleware,
    create_security_middleware,
    get_security_config,
)


class TestRateLimitConfig:
    def test_default_values(self) -> None:
        config = RateLimitConfig()
        assert config.requests_per_minute == 60
        assert config.requests_per_hour == 1000
        assert config.burst_limit == 10
        assert config.trust_proxy is False
        assert config.max_entries == 10_000

    def test_custom_values(self) -> None:
        config = RateLimitConfig(
            requests_per_minute=30,
            requests_per_hour=500,
            burst_limit=5,
            trust_proxy=True,
            max_entries=5000,
        )
        assert config.requests_per_minute == 30
        assert config.requests_per_hour == 500
        assert config.burst_limit == 5
        assert config.trust_proxy is True
        assert config.max_entries == 5000


class TestRateLimitEntry:
    def test_default_values(self) -> None:
        entry = RateLimitEntry()
        assert entry.minute_count == 0
        assert entry.hour_count == 0
        assert entry.burst_count == 0
        assert entry.last_minute == 0
        assert entry.last_hour == 0
        assert entry.last_burst_reset == 0


class TestRateLimiter:
    def test_init_default_config(self) -> None:
        limiter = RateLimiter()
        assert limiter.config.requests_per_minute == 60
        assert limiter.config.burst_limit == 10
        assert len(limiter._clients) == 0

    def test_init_custom_config(self) -> None:
        config = RateLimitConfig(burst_limit=5, requests_per_minute=30)
        limiter = RateLimiter(config)
        assert limiter.config.burst_limit == 5
        assert limiter.config.requests_per_minute == 30

    def test_get_client_id_from_direct_request(self) -> None:
        limiter = RateLimiter()
        request = Mock()
        request.client = Mock(host="192.168.1.100")
        request.headers = {}
        assert limiter._get_client_id(request) == "192.168.1.100"

    def test_get_client_id_trust_proxy_disabled(self) -> None:
        limiter = RateLimiter(RateLimitConfig(trust_proxy=False))
        request = Mock()
        request.client = Mock(host="10.0.0.1")
        request.headers = {"X-Forwarded-For": "203.0.113.1"}
        # Should ignore X-Forwarded-For when trust_proxy=False
        assert limiter._get_client_id(request) == "10.0.0.1"

    def test_get_client_id_trust_proxy_enabled(self) -> None:
        limiter = RateLimiter(RateLimitConfig(trust_proxy=True))
        request = Mock()
        request.client = Mock(host="10.0.0.1")
        request.headers = {"X-Forwarded-For": "203.0.113.1, 10.0.0.2"}
        # Should use first IP from X-Forwarded-For
        assert limiter._get_client_id(request) == "203.0.113.1"

    def test_get_client_id_no_client(self) -> None:
        limiter = RateLimiter()
        request = Mock()
        request.client = None
        request.headers = {}
        assert limiter._get_client_id(request) == "unknown"

    def test_is_allowed_new_client(self) -> None:
        limiter = RateLimiter()
        request = Mock()
        request.client = Mock(host="192.168.1.100")
        request.headers = {}
        allowed, reason = limiter.is_allowed(request)
        assert allowed is True
        assert reason == ""

    def test_burst_limit_enforced(self) -> None:
        limiter = RateLimiter(RateLimitConfig(burst_limit=2))
        request = Mock()
        request.client = Mock(host="192.168.1.100")
        request.headers = {}

        # First request allowed
        assert limiter.is_allowed(request)[0] is True
        # Second request allowed
        assert limiter.is_allowed(request)[0] is True
        # Third request blocked (burst limit)
        allowed, reason = limiter.is_allowed(request)
        assert allowed is False
        assert "Burst limit exceeded" in reason

    def test_max_entries_enforced(self) -> None:
        limiter = RateLimiter(RateLimitConfig(max_entries=2))
        request1 = Mock(client=Mock(host="10.0.0.1"), headers={})
        request2 = Mock(client=Mock(host="10.0.0.2"), headers={})
        request3 = Mock(client=Mock(host="10.0.0.3"), headers={})

        # Fill up the rate limiter
        limiter.is_allowed(request1)
        limiter.is_allowed(request2)

        # Third client blocked due to capacity
        allowed, reason = limiter.is_allowed(request3)
        assert allowed is False
        assert "capacity exceeded" in reason

    def test_cleanup_stale_entries(self) -> None:
        import time

        limiter = RateLimiter()
        # Add an entry with old timestamp
        old_client = "192.168.1.100"
        limiter._clients[old_client] = RateLimitEntry(
            last_minute=time.time() - 7200,  # 2 hours ago
            last_hour=time.time() - 7200,
            last_burst_reset=time.time() - 7200,
        )

        # Add a recent entry
        recent_client = "192.168.1.101"
        limiter._clients[recent_client] = RateLimitEntry(last_minute=time.time())

        limiter._cleanup_stale_entries()

        assert old_client not in limiter._clients
        assert recent_client in limiter._clients


class TestSecurityMiddleware:
    def test_init(self) -> None:
        limiter = RateLimiter()
        middleware = SecurityMiddleware(
            app=Mock(),
            rate_limiter=limiter,
            require_api_key=False,
            api_keys={"test-key"},
        )
        assert middleware.rate_limiter is limiter
        assert middleware.require_api_key is False
        assert middleware.api_keys == {"test-key"}

    def test_exempt_paths(self) -> None:
        middleware = SecurityMiddleware(app=Mock())
        assert middleware._is_exempt_path("/health") is True
        assert middleware._is_exempt_path("/docs") is True
        assert middleware._is_exempt_path("/openapi.json") is True
        assert middleware._is_exempt_path("/redoc") is True
        assert middleware._is_exempt_path("/api/test") is False

    def test_get_api_key(self) -> None:
        middleware = SecurityMiddleware(app=Mock())
        request = Mock()
        request.headers = {"X-API-Key": "test-key-123"}
        assert middleware._get_api_key(request) == "test-key-123"

    def test_get_api_key_missing(self) -> None:
        middleware = SecurityMiddleware(app=Mock())
        request = Mock()
        request.headers = {}
        assert middleware._get_api_key(request) is None


class TestSecurityConfig:
    def test_get_security_config_defaults(self) -> None:
        import os

        # Clear env vars for testing
        for key in [
            "API_KEY_REQUIRED",
            "API_KEYS",
            "RATE_LIMIT_RPM",
            "RATE_LIMIT_HOURLY",
            "TRUST_PROXY",
        ]:
            os.environ.pop(key, None)

        config = get_security_config()
        assert config["require_api_key"] is False
        assert config["api_keys"] == set()
        assert config["rate_limit_rpm"] == 60
        assert config["rate_limit_hourly"] == 1000
        assert config["trust_proxy"] is False

    def test_get_security_config_from_env(self) -> None:
        import os

        os.environ["API_KEY_REQUIRED"] = "true"
        os.environ["API_KEYS"] = "key1,key2,key3"
        os.environ["RATE_LIMIT_RPM"] = "120"
        os.environ["RATE_LIMIT_HOURLY"] = "2000"
        os.environ["TRUST_PROXY"] = "true"

        config = get_security_config()
        assert config["require_api_key"] is True
        assert config["api_keys"] == {"key1", "key2", "key3"}
        assert config["rate_limit_rpm"] == 120
        assert config["rate_limit_hourly"] == 2000
        assert config["trust_proxy"] is True

        # Cleanup
        for key in [
            "API_KEY_REQUIRED",
            "API_KEYS",
            "RATE_LIMIT_RPM",
            "RATE_LIMIT_HOURLY",
            "TRUST_PROXY",
        ]:
            os.environ.pop(key, None)


class TestCreateSecurityMiddleware:
    def test_create_middleware(self) -> None:
        import os

        for key in ["API_KEY_REQUIRED", "API_KEYS", "TRUST_PROXY"]:
            os.environ.pop(key, None)

        app = Mock()
        middleware = create_security_middleware(app)
        assert isinstance(middleware, SecurityMiddleware)
        assert middleware.require_api_key is False
