from __future__ import annotations

from argparse import Namespace
from datetime import date
from types import SimpleNamespace

import pytest

import nse_momentum_lab.cli.build_features as build_features
import nse_momentum_lab.cli.db_init as db_init
import nse_momentum_lab.cli.market_monitor as market_monitor


def test_build_features_rejects_force_without_ack(monkeypatch) -> None:
    monkeypatch.setattr(
        build_features.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            feature_set=None,
            since=None,
            status=False,
            list=False,
            legacy=False,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        build_features.main()

    assert excinfo.value.code == 2


def test_build_features_legacy_rebuild_rejects_force_without_ack(monkeypatch) -> None:
    monkeypatch.setattr(
        build_features.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            feature_set=None,
            since=None,
            status=False,
            list=False,
            legacy=True,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        build_features.main()

    assert excinfo.value.code == 2


def test_build_features_status_bypasses_force_guard(monkeypatch) -> None:
    calls: list[object] = []

    monkeypatch.setattr(
        build_features.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            feature_set=None,
            since=None,
            status=True,
            list=False,
            legacy=False,
        ),
    )
    monkeypatch.setattr(
        build_features,
        "get_market_db",
        lambda: object(),
    )
    monkeypatch.setattr(
        build_features,
        "show_status",
        lambda db: calls.append(db),
    )

    assert build_features.main() is None
    assert len(calls) == 1


def test_build_features_single_feature_force_stays_allowed(monkeypatch) -> None:
    calls: list[bool] = []
    since_calls: list[object] = []

    class DummyDB:
        con = object()

        def get_status(self):
            return {"tables": {}}

        def build_feat_daily_core(self, *, force: bool = False, since_date=None):
            calls.append(force)
            since_calls.append(since_date)
            return 5

    class DummyRegistry:
        def get(self, feature_set: str):
            return SimpleNamespace(name="feat_daily_core", version="1.0.0")

    monkeypatch.setattr(
        build_features.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            feature_set="daily_core",
            since=None,
            status=False,
            list=False,
            legacy=False,
        ),
    )
    monkeypatch.setattr(build_features, "get_market_db", lambda: DummyDB())
    monkeypatch.setattr(build_features, "get_feature_registry", lambda: DummyRegistry())

    assert build_features.main() == 0
    assert calls == [True]
    assert since_calls == [None]


def test_build_features_single_feature_since_uses_incremental_path(monkeypatch) -> None:
    calls: list[object] = []

    class DummyDB:
        con = object()

        def get_status(self):
            return {"tables": {}}

        def build_feat_intraday_core(self, *, force: bool = False, since_date=None):
            calls.append((force, since_date))
            return 42

    class DummyRegistry:
        def get(self, feature_set: str):
            return SimpleNamespace(name="feat_intraday_core", version="1.0.0")

    monkeypatch.setattr(
        build_features.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=False,
            allow_full_rebuild=False,
            feature_set="intraday_core",
            since=date.fromisoformat("2026-03-23"),
            status=False,
            list=False,
            legacy=False,
        ),
    )
    monkeypatch.setattr(build_features, "get_market_db", lambda: DummyDB())
    monkeypatch.setattr(build_features, "get_feature_registry", lambda: DummyRegistry())

    assert build_features.main() == 0
    assert calls == [(False, date.fromisoformat("2026-03-23"))]


def test_build_features_since_uses_incremental_path(monkeypatch) -> None:
    calls: list[object] = []

    class DummyDB:
        con = object()

        def get_status(self):
            return {"tables": {}}

        def _build_modular_features(self, *, force: bool = False, since_date=None):
            calls.append((force, since_date))

        def build_market_monitor_incremental(self, *, since_date=None, force: bool = False):
            calls.append(("monitor", force, since_date))

    monkeypatch.setattr(
        build_features.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=False,
            allow_full_rebuild=False,
            feature_set=None,
            since=date.fromisoformat("2026-03-23"),
            status=False,
            list=False,
            legacy=False,
        ),
    )
    monkeypatch.setattr(build_features, "get_market_db", lambda: DummyDB())

    assert build_features.main() == 0
    expected_date = date.fromisoformat("2026-03-23")
    assert calls == [(False, expected_date), ("monitor", False, expected_date)]


def test_db_init_rejects_duckdb_force_without_ack(monkeypatch) -> None:
    monkeypatch.setattr(
        db_init.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            postgres_only=False,
            duckdb_only=True,
            force=True,
            allow_full_rebuild=False,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        db_init.main()

    assert excinfo.value.code == 2


def test_market_monitor_rejects_force_without_ack(monkeypatch) -> None:
    monkeypatch.setattr(
        market_monitor.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            status=False,
            incremental=False,
            since=None,
        ),
    )
    monkeypatch.setattr(
        market_monitor,
        "get_market_db",
        lambda *, read_only=False: object(),
    )

    with pytest.raises(SystemExit) as excinfo:
        market_monitor.main()

    assert excinfo.value.code == 2
