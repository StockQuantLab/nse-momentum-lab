from nse_momentum_lab.services.scan.rules import ScanConfig
from nse_momentum_lab.services.scan.worker import ScanWorker, ScanWorkerResult


class TestScanWorkerResult:
    def test_scan_worker_result_creation(self) -> None:
        result = ScanWorkerResult(
            scan_run_id=1,
            status="SUCCEEDED",
            candidates_found=10,
            total_universe=100,
        )
        assert result.scan_run_id == 1
        assert result.status == "SUCCEEDED"
        assert result.candidates_found == 10
        assert result.total_universe == 100


class TestScanWorker:
    def test_scan_worker_init_default(self) -> None:
        worker = ScanWorker()
        assert worker.config is not None
        assert worker.symbols is None
        assert worker.scan_def_id is None

    def test_scan_worker_init_with_params(self) -> None:
        config = ScanConfig(breakout_threshold=0.05)
        worker = ScanWorker(scan_def_id=1, config=config, symbols=["test1", "test2"])
        assert worker.scan_def_id == 1
        assert worker.config.breakout_threshold == 0.05
        assert worker.symbols == ["TEST1", "TEST2"]

    def test_scan_worker_symbols_uppercase(self) -> None:
        worker = ScanWorker(symbols=["abc", "def"])
        assert worker.symbols == ["ABC", "DEF"]
