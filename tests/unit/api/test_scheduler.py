from nse_momentum_lab.api.scheduler import stop_scheduler


class TestScheduler:
    def test_stop_scheduler(self) -> None:
        stop_scheduler()
