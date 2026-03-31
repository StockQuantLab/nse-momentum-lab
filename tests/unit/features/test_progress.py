from __future__ import annotations

import json
from pathlib import Path
from types import MethodType

from nse_momentum_lab.features.materializer import (
    IncrementalFeatureMaterializer,
    MaterializationPlan,
    MaterializationResult,
)
from nse_momentum_lab.features.progress import FeatureBuildProgressReporter
from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureGranularity,
)


def test_materializer_writes_progress_events() -> None:
    progress_file = Path("tests/.feature_progress_test.ndjson")
    if progress_file.exists():
        progress_file.unlink()

    reporter = FeatureBuildProgressReporter(progress_file=progress_file, run_id="test-run")
    feat_def = FeatureDefinition(
        name="feat_test",
        version="feat_test_v1",
        description="Test feature",
        granularity=FeatureGranularity.DAILY,
        layer="core",
    )
    plan = MaterializationPlan(
        feature_sets=["feat_test"],
        build_order=[feat_def],
        force=True,
        stop_on_error=True,
        cascade=False,
    )

    materializer = IncrementalFeatureMaterializer()

    class FakeCon:
        def execute(self, _sql):
            return self

        def fetchone(self):
            return None

    def _fake_build_feature(self, con, feat_def, force, progress=None):
        return MaterializationResult(
            feature_name=feat_def.name,
            status="success",
            row_count=42,
            duration_seconds=0.25,
        )

    materializer._build_feature = MethodType(_fake_build_feature, materializer)

    try:
        summary = materializer.execute_plan(FakeCon(), plan, progress=reporter)

        assert summary.successful == 1
        assert progress_file.exists()

        events = [
            json.loads(line) for line in progress_file.read_text(encoding="utf-8").splitlines()
        ]
        assert events[0]["stage"] == "start"
        assert events[0]["run_id"] == "test-run"
        assert any(event["stage"] == "feature_start" for event in events)
        assert any(event["stage"] == "feature_success" for event in events)
        assert events[-1]["stage"] == "complete"
    finally:
        if progress_file.exists():
            progress_file.unlink()
