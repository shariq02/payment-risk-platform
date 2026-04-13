-- ============================================================================
-- Staging Schema DDL
-- Payment Risk and Order Analytics Platform
-- ============================================================================
-- File: sql/ddl/02_create_staging_schema.sql
-- Purpose: Creates staging schema and documents its role in the pipeline.
--          Staging tables are created and managed by dbt as views.
--          This file only ensures the schema exists so dbt can write to it.
-- Run: Once during Phase 3 setup via ingestion/setup_warehouse.py
-- Safe to re-run: yes (uses IF NOT EXISTS)
-- ============================================================================

-- Staging schema - dbt creates views here from bronze source tables
-- One view per bronze source table
-- Typing, renaming, light cleaning only - no business logic
CREATE SCHEMA IF NOT EXISTS staging;

-- Mart schema - dbt creates dimensional tables here
-- Dimensions, facts, and aggregate models
CREATE SCHEMA IF NOT EXISTS mart;

-- Snapshots schema - dbt creates SCD2 history tables here
-- snap_customer_profile and snap_seller_risk_profile
CREATE SCHEMA IF NOT EXISTS snapshots;

-- Bronze schema - already exists from Phase 2
-- Included here for completeness so setup is fully reproducible
CREATE SCHEMA IF NOT EXISTS bronze;
