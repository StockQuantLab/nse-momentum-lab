from __future__ import annotations

from datetime import date, datetime
from typing import Annotated

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

numeric = Annotated[Numeric, 12, 4]


class Base(DeclarativeBase):
    pass


class RefExchangeCalendar(Base):
    __tablename__ = "ref_exchange_calendar"
    __table_args__ = {"schema": "nseml"}

    trading_date: Mapped[date] = mapped_column(Date, primary_key=True)
    is_trading_day: Mapped[bool] = mapped_column(Boolean, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text)


class RefSymbol(Base):
    __tablename__ = "ref_symbol"
    __table_args__ = (
        UniqueConstraint("symbol", "series", name="uq_ref_symbol_symbol_series"),
        {"schema": "nseml"},
    )

    symbol_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    series: Mapped[str] = mapped_column(Text, nullable=False)
    isin: Mapped[str | None] = mapped_column(Text)
    name: Mapped[str | None] = mapped_column(Text)
    listing_date: Mapped[date | None] = mapped_column(Date)
    delisting_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ACTIVE")


class RefSymbolAlias(Base):
    __tablename__ = "ref_symbol_alias"
    __table_args__ = (
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        UniqueConstraint(
            "symbol_id", "vendor", "vendor_symbol", "valid_from", name="uq_ref_symbol_alias"
        ),
        {"schema": "nseml"},
    )

    alias_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    vendor: Mapped[str] = mapped_column(Text, nullable=False)
    vendor_symbol: Mapped[str] = mapped_column(Text, nullable=False)
    valid_from: Mapped[date] = mapped_column(Date, nullable=False)
    valid_to: Mapped[date | None] = mapped_column(Date)


class MdOhlcvRaw(Base):
    __tablename__ = "md_ohlcv_raw"
    __table_args__ = (
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        PrimaryKeyConstraint("symbol_id", "trading_date"),
        Index("idx_md_ohlcv_raw_symbol_date", "symbol_id", "trading_date"),
        {"schema": "nseml"},
    )

    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    open_raw: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    high_raw: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    low_raw: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    close_raw: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)
    value_traded: Mapped[numeric | None] = mapped_column(Numeric(18, 4))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MdOhlcvAdj(Base):
    __tablename__ = "md_ohlcv_adj"
    __table_args__ = (
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        PrimaryKeyConstraint("symbol_id", "trading_date"),
        Index("idx_md_ohlcv_adj_symbol_date", "symbol_id", "trading_date"),
        {"schema": "nseml"},
    )

    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    trading_date: Mapped[date] = mapped_column(Date, nullable=False)
    open_adj: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    high_adj: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    low_adj: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    close_adj: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)
    value_traded: Mapped[numeric | None] = mapped_column(Numeric(18, 4))
    adj_factor: Mapped[numeric] = mapped_column(Numeric(12, 6), nullable=False, default=1.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CaEvent(Base):
    __tablename__ = "ca_event"
    __table_args__ = (
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        Index("idx_ca_event_symbol_ex_date", "symbol_id", "ex_date"),
        Index("idx_ca_event_type_ex_date", "action_type", "ex_date"),
        {"schema": "nseml"},
    )

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    ex_date: Mapped[date] = mapped_column(Date, nullable=False)
    record_date: Mapped[date | None] = mapped_column(Date)
    action_type: Mapped[str] = mapped_column(Text, nullable=False)
    ratio_num: Mapped[numeric | None] = mapped_column(Numeric(12, 6))
    ratio_den: Mapped[numeric | None] = mapped_column(Numeric(12, 6))
    cash_amount: Mapped[numeric | None] = mapped_column(Numeric(12, 4))
    currency: Mapped[str] = mapped_column(Text, nullable=False, default="INR")
    source_uri: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ScanDefinition(Base):
    __tablename__ = "scan_definition"
    __table_args__ = {"schema": "nseml"}

    scan_def_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    config_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    code_sha: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    scans: Mapped[list[ScanRun]] = relationship("ScanRun", back_populates="definition")


class ScanRun(Base):
    __tablename__ = "scan_run"
    __table_args__ = (
        ForeignKeyConstraint(
            ["scan_def_id"], ["nseml.scan_definition.scan_def_id"], ondelete="CASCADE"
        ),
        UniqueConstraint("scan_def_id", "asof_date", "dataset_hash", name="uq_scan_run"),
        {"schema": "nseml"},
    )

    scan_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scan_def_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.scan_definition.scan_def_id"), nullable=False
    )
    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    dataset_hash: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    logs_uri: Mapped[str | None] = mapped_column(Text)

    definition: Mapped[ScanDefinition] = relationship("ScanDefinition", back_populates="scans")
    results: Mapped[list[ScanResult]] = relationship("ScanResult", back_populates="run")


class ScanResult(Base):
    __tablename__ = "scan_result"
    __table_args__ = (
        ForeignKeyConstraint(["scan_run_id"], ["nseml.scan_run.scan_run_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        PrimaryKeyConstraint("scan_run_id", "symbol_id"),
        Index("idx_scan_result_asof", "asof_date"),
        {"schema": "nseml"},
    )

    scan_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.scan_run.scan_run_id"), nullable=False
    )
    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    score: Mapped[numeric | None] = mapped_column(Numeric(10, 4))
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")

    run: Mapped[ScanRun] = relationship("ScanRun", back_populates="results")


class DatasetManifest(Base):
    __tablename__ = "dataset_manifest"
    __table_args__ = (
        UniqueConstraint(
            "dataset_kind",
            "dataset_hash",
            "code_hash",
            "params_hash",
            name="uq_dataset_manifest",
        ),
        Index("idx_dataset_manifest_kind_created", "dataset_kind", "created_at"),
        {"schema": "nseml"},
    )

    dataset_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_kind: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_hash: Mapped[str] = mapped_column(Text, nullable=False)
    code_hash: Mapped[str | None] = mapped_column(Text)
    params_hash: Mapped[str | None] = mapped_column(Text)
    source_uri: Mapped[str | None] = mapped_column(Text)
    row_count: Mapped[int | None] = mapped_column(Integer)
    min_trading_date: Mapped[date | None] = mapped_column(Date)
    max_trading_date: Mapped[date | None] = mapped_column(Date)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ExpRun(Base):
    __tablename__ = "exp_run"
    __table_args__ = (
        Index("idx_exp_run_strategy_dataset", "strategy_hash", "dataset_hash"),
        {"schema": "nseml"},
    )

    exp_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exp_hash: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    strategy_name: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_hash: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_hash: Mapped[str] = mapped_column(Text, nullable=False)
    params_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    code_sha: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text, nullable=False)

    metrics: Mapped[list[ExpMetric]] = relationship("ExpMetric", back_populates="run")
    artifacts: Mapped[list[ExpArtifact]] = relationship("ExpArtifact", back_populates="run")
    trades: Mapped[list[BtTrade]] = relationship("BtTrade", back_populates="run")


class ExpMetric(Base):
    __tablename__ = "exp_metric"
    __table_args__ = (
        ForeignKeyConstraint(["exp_run_id"], ["nseml.exp_run.exp_run_id"], ondelete="CASCADE"),
        PrimaryKeyConstraint("exp_run_id", "metric_name"),
        {"schema": "nseml"},
    )

    exp_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.exp_run.exp_run_id"), nullable=False
    )
    metric_name: Mapped[str] = mapped_column(Text, nullable=False)
    metric_value: Mapped[numeric | None] = mapped_column(Numeric(18, 6))

    run: Mapped[ExpRun] = relationship("ExpRun", back_populates="metrics")


class ExpArtifact(Base):
    __tablename__ = "exp_artifact"
    __table_args__ = (
        ForeignKeyConstraint(["exp_run_id"], ["nseml.exp_run.exp_run_id"], ondelete="CASCADE"),
        PrimaryKeyConstraint("exp_run_id", "artifact_name"),
        {"schema": "nseml"},
    )

    exp_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.exp_run.exp_run_id"), nullable=False
    )
    artifact_name: Mapped[str] = mapped_column(Text, nullable=False)
    uri: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str | None] = mapped_column(Text)

    run: Mapped[ExpRun] = relationship("ExpRun", back_populates="artifacts")


class Signal(Base):
    __tablename__ = "signal"
    __table_args__ = (
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        Index("idx_signal_state_date", "state", "planned_entry_date"),
        {"schema": "nseml"},
    )

    signal_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    strategy_hash: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    entry_mode: Mapped[str] = mapped_column(Text, nullable=False)
    planned_entry_date: Mapped[date | None] = mapped_column(Date)
    initial_stop: Mapped[numeric | None] = mapped_column(Numeric(12, 4))
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    orders: Mapped[list[PaperOrder]] = relationship("PaperOrder", back_populates="signal")


class PaperOrder(Base):
    __tablename__ = "paper_order"
    __table_args__ = (
        ForeignKeyConstraint(["signal_id"], ["nseml.signal.signal_id"], ondelete="CASCADE"),
        {"schema": "nseml"},
    )

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.signal.signal_id"), nullable=False
    )
    side: Mapped[str] = mapped_column(Text, nullable=False)
    qty: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    order_type: Mapped[str] = mapped_column(Text, nullable=False)
    limit_price: Mapped[numeric | None] = mapped_column(Numeric(12, 4))
    status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    signal: Mapped[Signal] = relationship("Signal", back_populates="orders")
    fills: Mapped[list[PaperFill]] = relationship("PaperFill", back_populates="order")


class PaperFill(Base):
    __tablename__ = "paper_fill"
    __table_args__ = (
        ForeignKeyConstraint(["order_id"], ["nseml.paper_order.order_id"], ondelete="CASCADE"),
        {"schema": "nseml"},
    )

    fill_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.paper_order.order_id"), nullable=False
    )
    fill_time: Mapped[date] = mapped_column(DateTime(timezone=True), nullable=False)
    fill_price: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    qty: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    fees: Mapped[numeric | None] = mapped_column(Numeric(10, 4))
    slippage_bps: Mapped[numeric | None] = mapped_column(Numeric(8, 4))

    order: Mapped[PaperOrder] = relationship("PaperOrder", back_populates="fills")


class PaperPosition(Base):
    __tablename__ = "paper_position"
    __table_args__ = (
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        {"schema": "nseml"},
    )

    position_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    opened_at: Mapped[date] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    avg_entry: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    avg_exit: Mapped[numeric | None] = mapped_column(Numeric(12, 4))
    qty: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    pnl: Mapped[numeric | None] = mapped_column(Numeric(14, 4))
    state: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")


class JobRun(Base):
    __tablename__ = "job_run"
    __table_args__ = (
        Index("idx_job_run_name_date", "job_name", "asof_date"),
        Index("idx_job_run_status_date", "status", "asof_date"),
        Index("idx_job_run_idempotency", "idempotency_key", unique=True),
        {"schema": "nseml"},
    )

    job_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(Text, unique=True)
    dataset_hash: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[date] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    logs_uri: Mapped[str | None] = mapped_column(Text)
    metrics_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    error_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")


class BtTrade(Base):
    __tablename__ = "bt_trade"
    __table_args__ = (
        ForeignKeyConstraint(["exp_run_id"], ["nseml.exp_run.exp_run_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["symbol_id"], ["nseml.ref_symbol.symbol_id"], ondelete="CASCADE"),
        Index("idx_bt_trade_exp_entry", "exp_run_id", "entry_date"),
        Index("idx_bt_trade_exit_reason", "exit_reason"),
        {"schema": "nseml"},
    )

    trade_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exp_run_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.exp_run.exp_run_id"), nullable=False
    )
    symbol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.ref_symbol.symbol_id"), nullable=False
    )
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    entry_price: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    entry_mode: Mapped[str] = mapped_column(Text, nullable=False)
    qty: Mapped[numeric] = mapped_column(Numeric(12, 4), nullable=False)
    initial_stop: Mapped[numeric | None] = mapped_column(Numeric(12, 4))
    exit_date: Mapped[date | None] = mapped_column(Date)
    exit_price: Mapped[numeric | None] = mapped_column(Numeric(12, 4))
    pnl: Mapped[numeric | None] = mapped_column(Numeric(14, 4))
    pnl_r: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    fees: Mapped[numeric | None] = mapped_column(Numeric(10, 4))
    slippage_bps: Mapped[numeric | None] = mapped_column(Numeric(8, 4))
    mfe_r: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    mae_r: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    exit_reason: Mapped[str | None] = mapped_column(Text)
    exit_rule_version: Mapped[str] = mapped_column(Text, nullable=False)
    scan_run_id: Mapped[int | None] = mapped_column(Integer)
    reason_json: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    run: Mapped[ExpRun] = relationship("ExpRun", back_populates="trades")


class RptScanDaily(Base):
    __tablename__ = "rpt_scan_daily"
    __table_args__ = (
        ForeignKeyConstraint(
            ["scan_def_id"], ["nseml.scan_definition.scan_def_id"], ondelete="CASCADE"
        ),
        PrimaryKeyConstraint("asof_date", "scan_def_id", "dataset_hash"),
        {"schema": "nseml"},
    )

    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    scan_def_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("nseml.scan_definition.scan_def_id"), nullable=False
    )
    dataset_hash: Mapped[str] = mapped_column(Text, nullable=False)
    total_universe: Mapped[int] = mapped_column(Integer, nullable=False)
    passed_base_4p: Mapped[int] = mapped_column(Integer, nullable=False)
    passed_2lynch: Mapped[int] = mapped_column(Integer, nullable=False)
    passed_final: Mapped[int] = mapped_column(Integer, nullable=False)
    by_fail_reason: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    by_liquidity_bucket: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RptBtDaily(Base):
    __tablename__ = "rpt_bt_daily"
    __table_args__ = (
        PrimaryKeyConstraint("asof_date", "strategy_name", "dataset_hash", "entry_mode"),
        {"schema": "nseml"},
    )

    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    strategy_name: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_hash: Mapped[str] = mapped_column(Text, nullable=False)
    entry_mode: Mapped[str] = mapped_column(Text, nullable=False)
    signals: Mapped[int] = mapped_column(Integer, nullable=False)
    entries: Mapped[int] = mapped_column(Integer, nullable=False)
    exits: Mapped[int] = mapped_column(Integer, nullable=False)
    wins: Mapped[int] = mapped_column(Integer, nullable=False)
    losses: Mapped[int] = mapped_column(Integer, nullable=False)
    win_rate: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    avg_r: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    profit_factor: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    max_dd: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RptBtFailureDaily(Base):
    __tablename__ = "rpt_bt_failure_daily"
    __table_args__ = (
        PrimaryKeyConstraint(
            "asof_date", "strategy_name", "dataset_hash", "entry_mode", "exit_reason"
        ),
        {"schema": "nseml"},
    )

    asof_date: Mapped[date] = mapped_column(Date, nullable=False)
    strategy_name: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_hash: Mapped[str] = mapped_column(Text, nullable=False)
    entry_mode: Mapped[str] = mapped_column(Text, nullable=False)
    exit_reason: Mapped[str] = mapped_column(Text, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_r: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    median_r: Mapped[numeric | None] = mapped_column(Numeric(10, 6))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
