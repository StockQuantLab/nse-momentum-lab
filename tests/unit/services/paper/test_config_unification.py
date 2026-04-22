"""Unit tests for config unification: PaperStrategyConfig, bridge method, and factory.

Covers:
- PaperStrategyConfig field alignment (new fields present, correct defaults)
- BacktestParams.to_paper_config() bridge method (LONG + SHORT direction-aware)
- build_paper_config_from_preset() factory (all 4 canonical presets)
- get_paper_strategy_config() with preset_name param
- evaluate_candle() entry_start_minutes gate
- execute_entry() direction-aware trail params
"""

from __future__ import annotations

import pytest

from nse_momentum_lab.services.backtest.backtest_presets import (
    ALL_PRESETS,
    build_params_from_preset,
)
from nse_momentum_lab.services.backtest.engine import PositionSide
from nse_momentum_lab.services.paper.engine.strategy_presets import (
    PaperStrategyConfig,
    get_paper_strategy_config,
)
from nse_momentum_lab.services.paper.paper_backtest_bridge import build_paper_config_from_preset

# ---------------------------------------------------------------------------
# PaperStrategyConfig field alignment
# ---------------------------------------------------------------------------


class TestPaperStrategyConfigFields:
    """Ensure new fields exist and carry correct defaults."""

    def test_entry_cutoff_minutes_default_is_60(self):
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert cfg.entry_cutoff_minutes == 60  # must match backtest ENGINE_DEFAULTS

    def test_entry_start_minutes_default_is_5(self):
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert cfg.entry_start_minutes == 5  # skip 09:15-09:20 opening bar

    def test_trail_activation_pct_default(self):
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert cfg.trail_activation_pct == 0.08

    def test_trail_stop_pct_default(self):
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert cfg.trail_stop_pct == 0.02

    def test_short_trail_activation_pct_default_for_long_is_none(self):
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert cfg.short_trail_activation_pct is None

    def test_h_filter_close_pos_threshold_field_exists(self):
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert hasattr(cfg, "h_filter_close_pos_threshold")
        assert cfg.h_filter_close_pos_threshold == 0.70

    def test_h_filter_threshold_old_name_does_not_exist(self):
        """Old field name must be gone — use h_filter_close_pos_threshold instead."""
        cfg = get_paper_strategy_config("2lynchbreakout")
        assert not hasattr(cfg, "h_filter_threshold")

    def test_breakdown_defaults_time_stop_3(self):
        cfg = get_paper_strategy_config("2lynchbreakdown")
        assert cfg.time_stop_days == 3

    def test_breakdown_default_short_trail_activation(self):
        """2lynchbreakdown default should have short_trail_activation_pct set."""
        cfg = get_paper_strategy_config("2lynchbreakdown")
        assert cfg.short_trail_activation_pct == 0.04


# ---------------------------------------------------------------------------
# BacktestParams.to_paper_config() bridge method
# ---------------------------------------------------------------------------


class TestBacktestParamsToPaperConfig:
    """Ensure bridge method correctly maps BacktestParams → PaperStrategyConfig."""

    def test_breakout_4pct_long(self):
        params = build_params_from_preset("BREAKOUT_4PCT")
        cfg = params.to_paper_config(PositionSide.LONG)
        assert isinstance(cfg, PaperStrategyConfig)
        assert cfg.direction == "LONG"
        assert cfg.strategy_key == "2lynchbreakout"
        assert cfg.breakout_threshold == pytest.approx(params.breakout_threshold)
        assert cfg.entry_cutoff_minutes == params.entry_cutoff_minutes
        assert cfg.entry_start_minutes == params.entry_start_minutes
        assert cfg.time_stop_days == params.time_stop_days
        assert cfg.trail_activation_pct == pytest.approx(params.trail_activation_pct)
        assert cfg.trail_stop_pct == pytest.approx(params.trail_stop_pct)
        assert cfg.h_filter_close_pos_threshold == pytest.approx(
            params.h_filter_close_pos_threshold
        )

    def test_breakout_2pct_long(self):
        params = build_params_from_preset("BREAKOUT_2PCT")
        cfg = params.to_paper_config(PositionSide.LONG)
        assert cfg.direction == "LONG"
        assert cfg.breakout_threshold == pytest.approx(0.02)

    def test_breakdown_4pct_short(self):
        params = build_params_from_preset("BREAKDOWN_4PCT")
        cfg = params.to_paper_config(PositionSide.SHORT)
        assert cfg.direction == "SHORT"
        assert cfg.strategy_key == "2lynchbreakdown"
        # short_time_stop_days should be used
        expected_time_stop = params.short_time_stop_days or params.time_stop_days
        assert cfg.time_stop_days == expected_time_stop
        # short_trail_activation_pct should be set
        assert cfg.short_trail_activation_pct == pytest.approx(params.short_trail_activation_pct)

    def test_breakdown_2pct_short(self):
        params = build_params_from_preset("BREAKDOWN_2PCT")
        cfg = params.to_paper_config(PositionSide.SHORT)
        assert cfg.direction == "SHORT"
        assert cfg.breakout_threshold == pytest.approx(0.02)

    def test_direction_mismatch_breakout_short_raises(self):
        params = build_params_from_preset("BREAKOUT_4PCT")
        with pytest.raises(ValueError, match="LONG strategy"):
            params.to_paper_config(PositionSide.SHORT)

    def test_direction_mismatch_breakdown_long_raises(self):
        params = build_params_from_preset("BREAKDOWN_4PCT")
        with pytest.raises(ValueError, match="SHORT strategy"):
            params.to_paper_config(PositionSide.LONG)

    def test_result_is_paper_strategy_config(self):
        params = build_params_from_preset("BREAKOUT_4PCT")
        cfg = params.to_paper_config(PositionSide.LONG)
        assert isinstance(cfg, PaperStrategyConfig)
        # Paper-only defaults should be present
        assert cfg.max_positions == 10
        assert cfg.flatten_time == "15:15:00"


# ---------------------------------------------------------------------------
# build_paper_config_from_preset() factory
# ---------------------------------------------------------------------------


class TestBuildPaperConfigFromPreset:
    """Test the bridge factory for all 4 canonical presets."""

    @pytest.mark.parametrize(
        "preset_name,direction",
        [
            ("BREAKOUT_4PCT", PositionSide.LONG),
            ("BREAKOUT_2PCT", PositionSide.LONG),
            ("BREAKDOWN_4PCT", PositionSide.SHORT),
            ("BREAKDOWN_2PCT", PositionSide.SHORT),
        ],
    )
    def test_all_presets_produce_valid_config(self, preset_name, direction):
        cfg = build_paper_config_from_preset(preset_name, direction)
        assert isinstance(cfg, PaperStrategyConfig)
        assert cfg.direction == direction.value

    def test_breakdown_presets_differ_in_trail(self):
        """BREAKDOWN_4PCT has short_trail=0.04; BREAKDOWN_2PCT may differ."""
        cfg_4pct = build_paper_config_from_preset("BREAKDOWN_4PCT", PositionSide.SHORT)
        cfg_2pct = build_paper_config_from_preset("BREAKDOWN_2PCT", PositionSide.SHORT)
        # BREAKDOWN_4PCT has explicit short_trail_activation_pct=0.04
        assert cfg_4pct.short_trail_activation_pct == pytest.approx(0.04)
        # Configs differ between variants
        assert cfg_4pct.short_trail_activation_pct != cfg_2pct.short_trail_activation_pct

    def test_paper_overrides_applied(self):
        cfg = build_paper_config_from_preset(
            "BREAKOUT_4PCT", PositionSide.LONG, paper_overrides={"max_positions": 5}
        )
        assert cfg.max_positions == 5
        # Strategy knobs still come from the preset
        assert cfg.breakout_threshold == pytest.approx(0.04)

    def test_wrong_direction_raises(self):
        with pytest.raises(ValueError):
            build_paper_config_from_preset("BREAKOUT_4PCT", PositionSide.SHORT)

    def test_unknown_preset_raises(self):
        with pytest.raises((ValueError, KeyError)):
            build_paper_config_from_preset("NONEXISTENT_PRESET", PositionSide.LONG)


# ---------------------------------------------------------------------------
# get_paper_strategy_config() with preset_name
# ---------------------------------------------------------------------------


class TestGetPaperStrategyConfigWithPresetName:
    """Ensure preset_name lookup produces correct config."""

    def test_preset_name_breakout_4pct(self):
        cfg = get_paper_strategy_config("thresholdbreakout", preset_name="BREAKOUT_4PCT")
        assert isinstance(cfg, PaperStrategyConfig)
        assert cfg.direction == "LONG"
        assert cfg.breakout_threshold == pytest.approx(0.04)

    def test_preset_name_breakdown_4pct(self):
        cfg = get_paper_strategy_config("thresholdbreakdown", preset_name="BREAKDOWN_4PCT")
        assert cfg.direction == "SHORT"
        assert cfg.short_trail_activation_pct == pytest.approx(0.04)

    def test_compat_alias_h_filter_threshold(self):
        """Deprecated key h_filter_threshold should be accepted with a warning."""
        with pytest.warns(DeprecationWarning, match="h_filter_threshold is deprecated"):
            cfg = get_paper_strategy_config(
                "2lynchbreakout",
                overrides={"h_filter_threshold": 0.65},
            )
        assert cfg.h_filter_close_pos_threshold == pytest.approx(0.65)


# ---------------------------------------------------------------------------
# evaluate_candle() entry_start_minutes gate
# ---------------------------------------------------------------------------


class TestEvaluateCandleEntryStartGate:
    """Verify that evaluate_candle skips bars before entry_start_minutes."""

    def _make_runtime_with_candidate(self, symbol, prev_close):
        from datetime import timedelta, timezone

        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker
        from nse_momentum_lab.services.paper.engine.paper_runtime import PaperRuntimeState

        _ist = timezone(timedelta(hours=5, minutes=30))
        tracker = SessionPositionTracker(
            portfolio_value=100_000.0,
            max_positions=10,
            max_position_pct=0.10,
        )
        runtime = PaperRuntimeState()
        state = runtime.for_symbol(symbol)
        state.setup_row = {
            "symbol": symbol,
            "prev_close": prev_close,
            "direction": "LONG",
            "initial_stop": prev_close * 0.98,
            "entry_price_target": prev_close * 1.04,
        }
        state.setup_status = "candidate"
        return runtime, tracker, _ist

    def _epoch_at_ist(self, hour, minute, *, day=1):
        from datetime import datetime, timedelta, timezone

        _ist = timezone(timedelta(hours=5, minutes=30))
        return datetime(2026, 4, day, hour, minute, 0, tzinfo=_ist).timestamp()

    def _make_candle(self, open=100.0, high=105.0, low=98.0, close=103.0, bar_end=None):
        if bar_end is None:
            bar_end = self._epoch_at_ist(9, 20)
        return {
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "bar_end": bar_end,
            "bar_start": bar_end - 300,
            "volume": 1000,
        }

    def _make_session(self, flatten_time="15:15:00"):
        return {
            "session_id": "test-session",
            "flatten_time": flatten_time,
            "direction": "LONG",
        }

    def test_bar_at_open_skipped_when_entry_start_5(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        runtime, tracker, _ist = self._make_runtime_with_candidate("RELIANCE", 1000.0)

        class Cfg:
            direction = "LONG"
            breakout_threshold = 0.04
            entry_cutoff_minutes = 360
            entry_start_minutes = 5  # skip 09:15 bar

        # Bar ends at 09:15 (0 minutes from open)
        ts = self._epoch_at_ist(9, 15)
        candle = self._make_candle(high=1050.0, bar_end=ts)
        result = evaluate_candle(
            symbol="RELIANCE",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=self._make_session(),
            strategy_config=Cfg(),
        )
        assert result["action"] == "SKIP"
        assert result["reason"] == "entry_start_not_reached"

    def test_bar_at_09_20_passes_entry_start_5(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        runtime, tracker, _ist = self._make_runtime_with_candidate("RELIANCE", 1000.0)

        class Cfg:
            direction = "LONG"
            breakout_threshold = 0.04
            entry_cutoff_minutes = 360
            entry_start_minutes = 5  # skip 09:15 bar only; 09:20 is ok

        # Bar ends at 09:20 (5 minutes from open — exactly at boundary, should pass)
        ts = self._epoch_at_ist(9, 20)
        candle = self._make_candle(high=1050.0, bar_end=ts)
        result = evaluate_candle(
            symbol="RELIANCE",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=self._make_session(),
            strategy_config=Cfg(),
        )
        # Should not be skipped for entry_start reason
        assert result.get("reason") != "entry_start_not_reached"


# ---------------------------------------------------------------------------
# execute_entry() direction-aware trail params
# ---------------------------------------------------------------------------


class TestExecuteEntryTrailParams:
    """Verify trail params are read from strategy_config, not hardcoded."""

    def _make_candidate(self, direction="LONG"):
        return {
            "symbol": "TEST",
            "entry_price": 1000.0,
            "initial_stop": 980.0 if direction == "LONG" else 1020.0,
            "direction": direction,
            "signal_id": "sig-001",
        }

    def test_long_trail_from_config(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker
        from nse_momentum_lab.services.paper.engine.paper_runtime import execute_entry

        tracker = SessionPositionTracker(
            portfolio_value=100_000.0,
            max_positions=10,
            max_position_pct=0.10,
        )

        class Cfg:
            direction = "LONG"
            trail_activation_pct = 0.10
            trail_stop_pct = 0.03
            short_trail_activation_pct = None

        result = execute_entry(
            candidate=self._make_candidate("LONG"),
            tracker=tracker,
            session_id="s1",
            session={"session_id": "s1"},
            paper_db=None,
            strategy_config=Cfg(),
        )
        # result is the position dict
        assert result is not None
        trail = tracker.get_open_position("TEST")
        assert trail is not None
        assert trail.trail_state["trail_activation_pct"] == pytest.approx(0.10)
        assert trail.trail_state["trail_stop_pct"] == pytest.approx(0.03)

    def test_short_uses_short_trail_activation_pct(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker
        from nse_momentum_lab.services.paper.engine.paper_runtime import execute_entry

        tracker = SessionPositionTracker(
            portfolio_value=100_000.0,
            max_positions=10,
            max_position_pct=0.10,
        )

        class Cfg:
            direction = "SHORT"
            trail_activation_pct = 0.08
            trail_stop_pct = 0.02
            short_trail_activation_pct = 0.04  # stricter trail for breakdown

        result = execute_entry(
            candidate=self._make_candidate("SHORT"),
            tracker=tracker,
            session_id="s1",
            session={"session_id": "s1"},
            paper_db=None,
            strategy_config=Cfg(),
        )
        assert result is not None
        trail = tracker.get_open_position("TEST")
        assert trail is not None
        # SHORT should use short_trail_activation_pct
        assert trail.trail_state["trail_activation_pct"] == pytest.approx(0.04)

    def test_short_falls_back_to_trail_activation_when_short_trail_is_none(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker
        from nse_momentum_lab.services.paper.engine.paper_runtime import execute_entry

        tracker = SessionPositionTracker(
            portfolio_value=100_000.0,
            max_positions=10,
            max_position_pct=0.10,
        )

        class Cfg:
            direction = "SHORT"
            trail_activation_pct = 0.08
            trail_stop_pct = 0.02
            short_trail_activation_pct = None  # no short override

        result = execute_entry(
            candidate=self._make_candidate("SHORT"),
            tracker=tracker,
            session_id="s1",
            session={"session_id": "s1"},
            paper_db=None,
            strategy_config=Cfg(),
        )
        assert result is not None
        trail = tracker.get_open_position("TEST")
        assert trail is not None
        # Fallback to trail_activation_pct
        assert trail.trail_state["trail_activation_pct"] == pytest.approx(0.08)

    def test_no_strategy_config_uses_defaults(self):
        """strategy_config=None should still work (fallback to 0.08/0.02)."""
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker
        from nse_momentum_lab.services.paper.engine.paper_runtime import execute_entry

        tracker = SessionPositionTracker(
            portfolio_value=100_000.0,
            max_positions=10,
            max_position_pct=0.10,
        )

        result = execute_entry(
            candidate=self._make_candidate("LONG"),
            tracker=tracker,
            session_id="s1",
            session={"session_id": "s1"},
            paper_db=None,
            strategy_config=None,
        )
        assert result is not None
        trail = tracker.get_open_position("TEST")
        assert trail is not None
        assert trail.trail_state["trail_activation_pct"] == pytest.approx(0.08)
        assert trail.trail_state["trail_stop_pct"] == pytest.approx(0.02)

# ---------------------------------------------------------------------------
# paper_backtest_bridge: paper_overrides strategy-field rejection (P3)
# ---------------------------------------------------------------------------


class TestBridgeOverridesValidation:
    """build_paper_config_from_preset must reject strategy-defining fields in paper_overrides."""

    def test_infra_override_accepted(self):
        from nse_momentum_lab.services.backtest.engine import PositionSide
        from nse_momentum_lab.services.paper.paper_backtest_bridge import (
            build_paper_config_from_preset,
        )

        cfg = build_paper_config_from_preset(
            "BREAKOUT_4PCT", PositionSide.LONG, paper_overrides={"max_positions": 5}
        )
        assert cfg.max_positions == 5
        assert cfg.breakout_threshold == pytest.approx(0.04)

    def test_strategy_field_rejected(self):
        from nse_momentum_lab.services.backtest.engine import PositionSide
        from nse_momentum_lab.services.paper.paper_backtest_bridge import (
            build_paper_config_from_preset,
        )

        with pytest.raises(ValueError, match="strategy-defining fields"):
            build_paper_config_from_preset(
                "BREAKOUT_4PCT",
                PositionSide.LONG,
                paper_overrides={"breakout_threshold": 0.02},
            )

    def test_direction_field_rejected(self):
        from nse_momentum_lab.services.backtest.engine import PositionSide
        from nse_momentum_lab.services.paper.paper_backtest_bridge import (
            build_paper_config_from_preset,
        )

        with pytest.raises(ValueError, match="strategy-defining fields"):
            build_paper_config_from_preset(
                "BREAKOUT_4PCT",
                PositionSide.LONG,
                paper_overrides={"direction": "SHORT"},
            )

    def test_time_stop_field_rejected(self):
        from nse_momentum_lab.services.backtest.engine import PositionSide
        from nse_momentum_lab.services.paper.paper_backtest_bridge import (
            build_paper_config_from_preset,
        )

        with pytest.raises(ValueError, match="strategy-defining fields"):
            build_paper_config_from_preset(
                "BREAKDOWN_4PCT",
                PositionSide.SHORT,
                paper_overrides={"time_stop_days": 99},
            )


# ---------------------------------------------------------------------------
# paper_eod_carry: days_held not incremented on entry day (P1)
# ---------------------------------------------------------------------------


class TestEodCarryDaysHeldParity:
    """days_held must not be incremented when EOD carry runs the same day the position was opened."""

    def _make_pos(self, opened_at_str: str, days_held: int = 0, avg_entry: float = 100.0):
        return {
            "position_id": "pos-1",
            "symbol": "TEST",
            "direction": "LONG",
            "avg_entry": avg_entry,
            "qty": 10,
            "state": "OPEN",
            "opened_at": opened_at_str,
            "closed_at": None,
            "stop_loss": avg_entry * 0.98,
            "metadata_json": {"days_held": days_held, "current_sl": avg_entry * 0.98},
        }

    def _make_features(self, close: float = 105.0):
        return {"TEST": {"close": close, "close_pos_in_range": 0.80, "low": 98.0, "high": 110.0}}

    def _run_carry(self, pos, features, trade_date="2026-04-17"):
        from unittest.mock import MagicMock

        from nse_momentum_lab.services.paper.scripts.paper_eod_carry import (
            apply_eod_carry_decisions,
        )

        class Cfg:
            time_stop_days = 3
            h_carry_enabled = True
            h_filter_close_pos_threshold = 0.70

        updated_meta: dict = {}

        def fake_update(pos_id, **kwargs):
            updated_meta.update(kwargs.get("metadata_json", {}))

        paper_db = MagicMock()
        paper_db.list_open_positions.return_value = [pos]
        paper_db.update_position.side_effect = fake_update

        apply_eod_carry_decisions(
            session_id="s1",
            trade_date=trade_date,
            paper_db=paper_db,
            daily_features=features,
            strategy_config=Cfg(),
        )
        return updated_meta

    def test_entry_day_does_not_increment_days_held(self):
        """days_held stays 0 when EOD carry runs on the same day the position was opened."""
        pos = self._make_pos("2026-04-17T09:20:00+05:30", days_held=0)
        meta = self._run_carry(pos, self._make_features(), trade_date="2026-04-17")
        assert meta["days_held"] == 0

    def test_subsequent_carry_increments_days_held(self):
        """days_held increments on carries after the entry day."""
        pos = self._make_pos("2026-04-16T09:20:00+05:30", days_held=0)
        meta = self._run_carry(pos, self._make_features(), trade_date="2026-04-17")
        assert meta["days_held"] == 1

    def test_time_exit_fires_at_3rd_carry_not_2nd(self):
        """TIME_EXIT for time_stop_days=3 must fire on the 3rd carry, not the 2nd."""
        from unittest.mock import MagicMock

        from nse_momentum_lab.services.paper.scripts.paper_eod_carry import (
            apply_eod_carry_decisions,
        )

        class Cfg:
            time_stop_days = 3
            h_carry_enabled = True
            h_filter_close_pos_threshold = 0.70

        # Position opened 2026-04-14 (3 trading days ago). days_held=2 → new_days_held=3 → exit.
        pos = self._make_pos("2026-04-14T09:20:00+05:30", days_held=2)

        paper_db = MagicMock()
        paper_db.list_open_positions.return_value = [pos]
        paper_db.insert_order.return_value = None

        result = apply_eod_carry_decisions(
            session_id="s1",
            trade_date="2026-04-17",
            paper_db=paper_db,
            daily_features=self._make_features(),
            strategy_config=Cfg(),
        )
        assert result["time_exit"] == 1
        assert result["carried"] == 0

    def test_time_exit_does_not_fire_at_2nd_carry(self):
        """With days_held=1 (2nd carry), should carry again, not exit."""
        from unittest.mock import MagicMock

        from nse_momentum_lab.services.paper.scripts.paper_eod_carry import (
            apply_eod_carry_decisions,
        )

        class Cfg:
            time_stop_days = 3
            h_carry_enabled = True
            h_filter_close_pos_threshold = 0.70

        pos = self._make_pos("2026-04-15T09:20:00+05:30", days_held=1)

        paper_db = MagicMock()
        paper_db.list_open_positions.return_value = [pos]
        updated_meta: dict = {}
        paper_db.update_position.side_effect = lambda _id, **kw: updated_meta.update(
            kw.get("metadata_json", {})
        )

        result = apply_eod_carry_decisions(
            session_id="s1",
            trade_date="2026-04-17",
            paper_db=paper_db,
            daily_features=self._make_features(),
            strategy_config=Cfg(),
        )
        assert result["carried"] == 1
        assert result["time_exit"] == 0
        assert updated_meta["days_held"] == 2
