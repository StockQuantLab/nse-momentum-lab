from nse_momentum_lab.services.rollup.worker import DailyRollupWorker


class TestDailyRollupWorkerInit:
    def test_init_without_scan_def_id(self) -> None:
        worker = DailyRollupWorker()
        assert worker._scan_def_id is None

    def test_init_with_scan_def_id(self) -> None:
        worker = DailyRollupWorker(scan_def_id=42)
        assert worker._scan_def_id == 42
