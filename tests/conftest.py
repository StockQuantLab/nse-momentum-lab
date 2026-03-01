"""Pytest configuration for dashboard tests."""

import pytest


@pytest.fixture(scope="session")
def base_url():
    """Dashboard base URL."""
    return "http://localhost:8501"


@pytest.fixture(scope="session")
def api_url():
    """API base URL."""
    return "http://localhost:8004"


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Add permissions for screenshots."""
    return {
        **browser_context_args,
        "viewport": {"width": 1920, "height": 1080},
    }
