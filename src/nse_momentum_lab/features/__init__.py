"""
NSE Momentum Lab Feature Store.

This module provides a modular, strategy-agnostic feature store for
backtesting and research.

Feature Sets:
- feat_daily_core: Core daily features (returns, volatility, trend, liquidity)
- feat_intraday_core: Intraday features (opening ranges, breakout times, FEE windows)
- feat_event_core: Event features (earnings, corporate actions, post-event drift)
- feat_2lynch_derived: 2LYNCH strategy-specific filters and derived features

Usage:
    from nse_momentum_lab.features import (
        get_feature_registry,
        IncrementalFeatureMaterializer,
        build_feat_daily_core,
        build_feat_intraday_core,
    )

    # Build all feature sets
    materializer = IncrementalFeatureMaterializer()
    summary = materializer.build_all(con, force=True)

    # Build specific feature sets
    build_feat_daily_core(con)
    build_feat_intraday_core(con)
"""

from nse_momentum_lab.features.daily_core import (
    FEAT_DAILY_CORE_VERSION,
    build_feat_daily_core,
    register_feat_daily_core,
)
from nse_momentum_lab.features.event_core import (
    FEAT_EVENT_CORE_VERSION,
    build_feat_event_core,
    register_feat_event_core,
)
from nse_momentum_lab.features.intraday_core import (
    FEAT_INTRADAY_CORE_VERSION,
    build_feat_intraday_core,
    register_feat_intraday_core,
)
from nse_momentum_lab.features.materializer import (
    IncrementalFeatureMaterializer,
    MaterializationPlan,
    MaterializationResult,
    MaterializationSummary,
)
from nse_momentum_lab.features.progress import (
    FeatureBuildProgressEvent,
    FeatureBuildProgressReporter,
    default_progress_file,
)
from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureDependency,
    FeatureGranularity,
    FeatureRegistry,
    FeatureSetState,
    IncrementalPolicy,
    get_feature_registry,
)
from nse_momentum_lab.features.strategy_derived import (
    FEAT_2LYNCH_DERIVED_VERSION,
    build_2lynch_derived,
    create_legacy_feat_daily_view,
    register_2lynch_derived,
)

__all__ = [
    "FEAT_2LYNCH_DERIVED_VERSION",
    "FEAT_DAILY_CORE_VERSION",
    "FEAT_EVENT_CORE_VERSION",
    "FEAT_INTRADAY_CORE_VERSION",
    "FeatureBuildProgressEvent",
    "FeatureBuildProgressReporter",
    "FeatureDefinition",
    "FeatureDependency",
    "FeatureGranularity",
    "FeatureRegistry",
    "FeatureSetState",
    "IncrementalFeatureMaterializer",
    "IncrementalPolicy",
    "MaterializationPlan",
    "MaterializationResult",
    "MaterializationSummary",
    "build_2lynch_derived",
    "build_feat_daily_core",
    "build_feat_event_core",
    "build_feat_intraday_core",
    "create_legacy_feat_daily_view",
    "default_progress_file",
    "get_feature_registry",
    "register_2lynch_derived",
    "register_feat_daily_core",
    "register_feat_event_core",
    "register_feat_intraday_core",
]
