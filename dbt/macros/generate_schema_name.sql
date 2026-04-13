-- macros/generate_schema_name.sql
-- ============================================================================
-- Custom schema name generation macro
-- ============================================================================
-- Purpose: Controls how dbt names schemas in PostgreSQL.
--          By default dbt appends custom schema to target schema producing
--          names like mart_staging instead of staging.
--          This macro overrides that behaviour so:
--            staging models   -> staging schema
--            intermediate     -> staging schema (ephemeral, no schema needed)
--            marts models     -> mart schema
--            snapshots        -> snapshots schema
--          If no custom schema defined, uses target schema (public).
-- ============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
