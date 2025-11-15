-- ===============================================================================
-- CONFIGURATION FILE FOR NESTED MANAGER ACCOUNTS ETL PIPELINE
-- ===============================================================================
-- Purpose: Centralized configuration settings and parameters
-- Usage: Include at the beginning of ETL execution
-- Version: 2.0
-- ===============================================================================

-- ===============================================================================
-- GLOBAL SETTINGS
-- ===============================================================================

-- Maximum hierarchy depth to prevent infinite recursion
SET max_hierarchy_levels = 12;

-- Date range for processing (adjust as needed)
SET processing_start_date = '2020-01-01';
SET processing_end_date = '2099-12-31';

-- Performance settings
SET enable_hashjoin = true;
SET enable_mergejoin = true;
SET work_mem = '2GB';
SET maintenance_work_mem = '1GB';

-- ===============================================================================
-- SCHEMA CONFIGURATION
-- ===============================================================================

-- Source schema settings
SET source_schema_gap = 'andes."adam-manager-account-prod"';
SET source_schema_identity = 'andes.identity_hub';
SET source_schema_aapn = 'andes.aapn_dw';
SET source_schema_barnegat = 'barnegat_share.aapt_dw';

-- Target schema settings
SET target_schema_aapt = 'aapt_dw';

-- ===============================================================================
-- TABLE NAMING CONVENTIONS
-- ===============================================================================

-- Temporary table prefix
SET temp_table_prefix = 'tmp_nma_';

-- Final output table names
SET final_hierarchy_table = 'd_mngr_accnt_nma_hierarchy';
SET final_mapping_table = 'dim_prtnr_adv_entt_mpng_nma';
SET final_consolidated_table = 'test_nma_changes';

-- ===============================================================================
-- BUSINESS RULE PARAMETERS
-- ===============================================================================

-- Source priority for deduplication (1 = highest priority)
-- GSO-DSP-SS: 1, PN: 2, API: 3, RODEO: 4, MANUAL: 5, LCARS: 6, GSO-DSP-MS: 7

-- Entity types for perpetual relationships
SET agency_entity_types = ('Agency', 'AdTechAgency', 'MediaAgency');
SET partner_entity_types = ('Partner', 'TechPartner', 'DataPartner');

-- PN registration flags
SET approved_registration_flag = 'Y';
SET disabled_account_flag = 'N';
SET external_partner_flag = 'Y';

-- Migration flag for PNAG2 NMA
SET pnag2_nma_migration_flag = 'Y';

-- ===============================================================================
-- DATA QUALITY THRESHOLDS
-- ===============================================================================

-- Maximum allowed segments per manager account (for anomaly detection)
SET max_segments_per_account = 50;

-- Minimum data quality threshold (percentage)
SET min_data_quality_threshold = 95.0;

-- Maximum processing time threshold (hours)
SET max_processing_time_hours = 3.0;

-- ===============================================================================
-- MONITORING PARAMETERS
-- ===============================================================================

-- Key metrics to track
SET track_hierarchy_depth = true;
SET track_pn_manager_ratio = true;
SET track_temporal_segments = true;
SET track_deduplication_ratio = true;

-- Alert thresholds
SET alert_hierarchy_depth_threshold = 10;
SET alert_pn_ratio_min = 0.1;  -- Minimum 10% PN managers expected
SET alert_pn_ratio_max = 0.9;  -- Maximum 90% PN managers expected
SET alert_segment_count_threshold = 1000000;  -- Alert if over 1M segments

-- ===============================================================================
-- PERFORMANCE OPTIMIZATION SETTINGS
-- ===============================================================================

-- Distribution keys for parallel processing
SET dist_key_manager_account = 'manager_account_id';
SET dist_key_entity_id = 'entity_id';

-- Sort keys for query optimization
SET sort_key_dates = 'effective_start_date, effective_end_date';
SET sort_key_hierarchy = 'root_manager_account_id, level__, chld_manager_account_id';

-- ===============================================================================
-- VALIDATION RULES
-- ===============================================================================

-- Cycle prevention rules
SET enable_cycle_detection = true;
SET max_path_length = 1000;

-- Date validation rules
SET validate_date_consistency = true;
SET allow_future_dates = true;
SET future_date_limit = '2050-12-31';

-- ===============================================================================
-- DEBUG AND LOGGING SETTINGS
-- ===============================================================================

-- Enable detailed logging
SET enable_debug_logging = false;
SET log_level = 'INFO';  -- Options: DEBUG, INFO, WARN, ERROR

-- Save intermediate results for debugging
SET save_intermediate_tables = false;
SET intermediate_table_retention_days = 7;

-- ===============================================================================
-- SAMPLE CONFIGURATION USAGE
-- ===============================================================================

/*
-- Example of using configuration in ETL:

-- 1. Load configuration
\i config.sql

-- 2. Use variables in queries
SELECT COUNT(*) 
FROM ${source_schema_identity}.dim_partner_adv_entity_mapping
WHERE effective_start_date >= '${processing_start_date}'
  AND effective_end_date <= '${processing_end_date}';

-- 3. Apply business rules
WITH filtered_accounts AS (
    SELECT partner_account_id
    FROM ${source_schema_aapn}.dim_partner_accounts
    WHERE UPPER(is_registration_approved) = '${approved_registration_flag}'
      AND UPPER(is_disabled) = '${disabled_account_flag}'
      AND UPPER(is_external_partner_account) = '${external_partner_flag}'
)
SELECT * FROM filtered_accounts;
*/

-- ===============================================================================
-- ENVIRONMENT-SPECIFIC OVERRIDES
-- ===============================================================================

-- Production environment settings
-- Uncomment for production deployment
-- SET work_mem = '4GB';
-- SET maintenance_work_mem = '2GB';
-- SET enable_debug_logging = false;
-- SET save_intermediate_tables = false;

-- Development environment settings  
-- Uncomment for development/testing
-- SET max_hierarchy_levels = 5;
-- SET enable_debug_logging = true;
-- SET save_intermediate_tables = true;
-- SET processing_start_date = '2024-01-01';
-- SET processing_end_date = '2024-12-31';

-- ===============================================================================
-- END OF CONFIGURATION
-- ===============================================================================
