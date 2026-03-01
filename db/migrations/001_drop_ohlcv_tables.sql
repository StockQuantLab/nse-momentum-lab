-- Migration: Drop OHLCV tables from PostgreSQL
-- These are now stored in DuckDB + Parquet (see ADR-009)
--
-- Run this after verifying DuckDB/Parquet data is working correctly.

BEGIN;

-- Drop feature table (now in DuckDB)
DROP TABLE IF EXISTS nseml.feat_daily CASCADE;

-- Drop adjusted OHLCV table (now in DuckDB/Parquet)
DROP TABLE IF EXISTS nseml.md_ohlcv_adj CASCADE;

-- Drop raw OHLCV table (now in DuckDB/Parquet)
DROP TABLE IF EXISTS nseml.md_ohlcv_raw CASCADE;

COMMIT;

-- Verify remaining tables
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'nseml'
-- ORDER BY table_name;
