from __future__ import annotations

from datetime import date

from nse_momentum_lab.services.backtest.walkforward import WalkForwardFramework


class TestWalkForwardFramework:
    def test_generate_rolling_windows_from_sessions_uses_trading_dates(self) -> None:
        framework = WalkForwardFramework()
        trading_sessions = [
            date(2026, 3, 2),
            date(2026, 3, 3),
            date(2026, 3, 4),
            date(2026, 3, 5),
            date(2026, 3, 6),
            date(2026, 3, 9),
            date(2026, 3, 10),
            date(2026, 3, 11),
        ]

        windows = list(
            framework.generate_rolling_windows_from_sessions(
                trading_sessions,
                train_sessions=3,
                test_sessions=2,
                roll_interval_sessions=2,
            )
        )

        assert len(windows) == 2
        assert windows[0].train_start == date(2026, 3, 2)
        assert windows[0].train_end == date(2026, 3, 4)
        assert windows[0].test_start == date(2026, 3, 5)
        assert windows[0].test_end == date(2026, 3, 6)
        assert windows[1].train_start == date(2026, 3, 4)
        assert windows[1].train_end == date(2026, 3, 6)
        assert windows[1].test_start == date(2026, 3, 9)
        assert windows[1].test_end == date(2026, 3, 10)

    def test_generate_rolling_windows_from_sessions_returns_empty_when_insufficient_data(
        self,
    ) -> None:
        framework = WalkForwardFramework()

        windows = list(
            framework.generate_rolling_windows_from_sessions(
                [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)],
                train_sessions=3,
                test_sessions=2,
                roll_interval_sessions=1,
            )
        )

        assert windows == []
