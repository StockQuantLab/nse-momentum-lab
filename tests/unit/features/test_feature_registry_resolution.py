from __future__ import annotations

from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureDependency,
    FeatureGranularity,
    FeatureRegistry,
)


def test_resolve_build_order_ignores_dataset_dependencies() -> None:
    registry = FeatureRegistry()
    registry.register(
        FeatureDefinition(
            name="feat_daily_core",
            version="v1",
            description="daily",
            granularity=FeatureGranularity.DAILY,
            layer="core",
            feature_dependencies=[
                FeatureDependency(name="v_daily", is_dataset=True, required_lookback_days=252),
            ],
        )
    )
    registry.register(
        FeatureDefinition(
            name="feat_2lynch_derived",
            version="v1",
            description="derived",
            granularity=FeatureGranularity.DAILY,
            layer="derived",
            feature_dependencies=[
                FeatureDependency(
                    name="feat_daily_core", is_dataset=False, required_lookback_days=252
                ),
            ],
        )
    )

    order = registry.resolve_build_order(["feat_daily_core", "feat_2lynch_derived"])

    assert [feat.name for feat in order] == ["feat_daily_core", "feat_2lynch_derived"]
