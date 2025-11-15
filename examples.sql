-- ===============================================================================
-- EXAMPLE QUERIES FOR NESTED MANAGER ACCOUNTS ETL PIPELINE
-- ===============================================================================
-- Purpose: Practical examples and test queries for validation
-- Usage: Run after ETL completion for validation and learning
-- Version: 2.0
-- ===============================================================================

-- ===============================================================================
-- EXAMPLE 1: BASIC HIERARCHY VALIDATION
-- ===============================================================================
-- Purpose: Validate hierarchy structure and relationships

-- Query 1.1: Show complete hierarchy for a specific root manager
SELECT 
    root_manager_account_id,
    chld_manager_account_id,
    level__,
    path_,
    hier_start_date,
    hier_end_date,
    root_pn_mngr_flg
FROM aapt_dw.d_mngr_accnt_nma_hierarchy 
WHERE root_manager_account_id = 'amzn1.ads1.ma.example.root'
ORDER BY level__, chld_manager_account_id;

-- Query 1.2: Find deepest hierarchies in the system
SELECT 
    root_manager_account_id,
    MAX(level__) as max_depth,
    COUNT(DISTINCT chld_manager_account_id) as total_children
FROM aapt_dw.d_mngr_accnt_nma_hierarchy
GROUP BY root_manager_account_id
ORDER BY max_depth DESC, total_children DESC
LIMIT 10;

-- ===============================================================================
-- EXAMPLE 2: TEMPORAL INHERITANCE VALIDATION  
-- ===============================================================================
-- Purpose: Validate temporal inheritance logic for post-hierarchy transitions

-- Query 2.1: Show temporal inheritance for a specific account
WITH account_timeline AS (
    SELECT 
        manager_account_id,
        effective_start_date,
        effective_end_date,
        root_pn_mngr_flg,
        source,
        'Final Output' as data_source
    FROM test_nma_changes
    WHERE manager_account_id = 'amzn1.ads1.ma.example.child'
    
    UNION ALL
    
    SELECT 
        chld_manager_account_id as manager_account_id,
        hier_start_date as effective_start_date,
        hier_end_date as effective_end_date,
        root_pn_mngr_flg,
        'Hierarchy' as source,
        'Hierarchy Table' as data_source
    FROM aapt_dw.d_mngr_accnt_nma_hierarchy
    WHERE chld_manager_account_id = 'amzn1.ads1.ma.example.child'
)
SELECT *
FROM account_timeline
ORDER BY effective_start_date, data_source;

-- Query 2.2: Find accounts with temporal inheritance (become root PN managers after hierarchy ends)
SELECT DISTINCT
    t1.manager_account_id,
    t1.effective_start_date as inheritance_start_date,
    t1.root_pn_mngr_flg as inherited_root_flag,
    h.hier_end_date as hierarchy_end_date
FROM test_nma_changes t1
INNER JOIN aapt_dw.d_mngr_accnt_nma_hierarchy h
    ON t1.manager_account_id = h.chld_manager_account_id
WHERE t1.root_pn_mngr_flg = 1  -- Account becomes root PN manager
  AND h.root_pn_mngr_flg = 0   -- Was not root PN manager in hierarchy
  AND t1.effective_start_date = h.hier_end_date + INTERVAL '1 day'  -- Inheritance starts day after hierarchy ends
ORDER BY inheritance_start_date DESC;

-- ===============================================================================
-- EXAMPLE 3: DATA QUALITY VALIDATION
-- ===============================================================================
-- Purpose: Validate data quality and identify potential issues

-- Query 3.1: Check for overlapping date ranges (should be empty)
WITH overlapping_segments AS (
    SELECT 
        t1.manager_account_id,
        t1.entity_id,
        t1.effective_start_date as start1,
        t1.effective_end_date as end1,
        t2.effective_start_date as start2, 
        t2.effective_end_date as end2,
        t1.source as source1,
        t2.source as source2
    FROM test_nma_changes t1
    JOIN test_nma_changes t2 
        ON t1.manager_account_id = t2.manager_account_id
        AND t1.entity_id = t2.entity_id
        AND t1.effective_start_date != t2.effective_start_date
    WHERE t1.effective_start_date < t2.effective_end_date
      AND t1.effective_end_date > t2.effective_start_date
)
SELECT COUNT(*) as overlapping_count,
       'Expected: 0 overlapping segments' as validation_note
FROM overlapping_segments;

-- Query 3.2: Validate cycle prevention (should be empty)
WITH potential_cycles AS (
    SELECT 
        h1.chld_manager_account_id,
        h1.prnt_manager_account_id,
        h1.path_,
        h2.chld_manager_account_id as potential_cycle_child
    FROM aapt_dw.d_mngr_accnt_nma_hierarchy h1
    JOIN aapt_dw.d_mngr_accnt_nma_hierarchy h2
        ON h1.chld_manager_account_id = h2.prnt_manager_account_id
        AND h2.chld_manager_account_id = h1.root_manager_account_id
)
SELECT COUNT(*) as cycle_count,
       'Expected: 0 cycles detected' as validation_note
FROM potential_cycles;

-- Query 3.3: Data quality summary report
SELECT 
    'Total Records' as metric,
    COUNT(*)::VARCHAR as value
FROM test_nma_changes

UNION ALL

SELECT 
    'Unique Manager Accounts' as metric,
    COUNT(DISTINCT manager_account_id)::VARCHAR as value
FROM test_nma_changes

UNION ALL

SELECT 
    'Unique Entities' as metric,
    COUNT(DISTINCT entity_id)::VARCHAR as value
FROM test_nma_changes

UNION ALL

SELECT 
    'Root PN Managers' as metric,
    COUNT(DISTINCT CASE WHEN root_pn_mngr_flg = 1 THEN manager_account_id END)::VARCHAR as value
FROM test_nma_changes

UNION ALL

SELECT 
    'Max Hierarchy Depth' as metric,
    MAX(level__)::VARCHAR as value
FROM aapt_dw.d_mngr_accnt_nma_hierarchy

UNION ALL

SELECT 
    'Avg Segments per Account' as metric,
    ROUND(COUNT(*)::DECIMAL / COUNT(DISTINCT manager_account_id), 2)::VARCHAR as value
FROM test_nma_changes;

-- ===============================================================================
-- EXAMPLE 4: BUSINESS INSIGHTS QUERIES
-- ===============================================================================
-- Purpose: Generate business insights from processed data

-- Query 4.1: Top 10 root managers by entity count
SELECT 
    manager_account_id,
    COUNT(DISTINCT entity_id) as unique_entities,
    COUNT(DISTINCT advertiser_id) as unique_advertisers,
    MIN(effective_start_date) as earliest_relationship,
    MAX(effective_end_date) as latest_relationship
FROM test_nma_changes
WHERE root_pn_mngr_flg = 1  -- Only root PN managers
GROUP BY manager_account_id
ORDER BY unique_entities DESC
LIMIT 10;

-- Query 4.2: Source distribution analysis
SELECT 
    source,
    COUNT(*) as record_count,
    COUNT(DISTINCT manager_account_id) as unique_managers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM test_nma_changes
GROUP BY source
ORDER BY record_count DESC;

-- Query 4.3: Temporal analysis - accounts by relationship duration
WITH duration_analysis AS (
    SELECT 
        manager_account_id,
        entity_id,
        effective_start_date,
        effective_end_date,
        CASE 
            WHEN effective_end_date = '2099-12-31' THEN 'Ongoing'
            WHEN effective_end_date - effective_start_date <= 30 THEN 'Short-term (≤30 days)'
            WHEN effective_end_date - effective_start_date <= 365 THEN 'Medium-term (≤1 year)'
            ELSE 'Long-term (>1 year)'
        END as duration_category
    FROM test_nma_changes
)
SELECT 
    duration_category,
    COUNT(*) as relationship_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM duration_analysis
GROUP BY duration_category
ORDER BY 
    CASE duration_category
        WHEN 'Short-term (≤30 days)' THEN 1
        WHEN 'Medium-term (≤1 year)' THEN 2  
        WHEN 'Long-term (>1 year)' THEN 3
        WHEN 'Ongoing' THEN 4
    END;

-- ===============================================================================
-- EXAMPLE 5: PERFORMANCE ANALYSIS QUERIES
-- ===============================================================================
-- Purpose: Analyze processing performance and optimization opportunities

-- Query 5.1: Table size analysis
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size
FROM pg_tables 
WHERE tablename IN (
    'test_nma_changes',
    'd_mngr_accnt_nma_hierarchy', 
    'dim_prtnr_adv_entt_mpng_nma__'
)
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Query 5.2: Distribution key effectiveness (for Redshift)
-- Note: This query works on Amazon Redshift to analyze data distribution
/*
SELECT 
    slice,
    COUNT(*) as row_count,
    MIN(manager_account_id) as min_key,
    MAX(manager_account_id) as max_key
FROM test_nma_changes
GROUP BY slice
ORDER BY slice;
*/

-- ===============================================================================
-- EXAMPLE 6: TROUBLESHOOTING QUERIES  
-- ===============================================================================
-- Purpose: Debug common issues and anomalies

-- Query 6.1: Find accounts with excessive segments (potential data quality issues)
SELECT 
    manager_account_id,
    COUNT(*) as segment_count,
    COUNT(DISTINCT entity_id) as unique_entities,
    MIN(effective_start_date) as earliest_date,
    MAX(effective_end_date) as latest_date,
    LISTAGG(DISTINCT source, ',') WITHIN GROUP (ORDER BY source) as sources
FROM test_nma_changes
GROUP BY manager_account_id
HAVING COUNT(*) > 20  -- Threshold for investigation
ORDER BY segment_count DESC
LIMIT 20;

-- Query 6.2: Find orphaned hierarchy records (child accounts without parents in current data)
SELECT DISTINCT
    h.chld_manager_account_id,
    h.prnt_manager_account_id,
    h.root_manager_account_id,
    h.level__
FROM aapt_dw.d_mngr_accnt_nma_hierarchy h
LEFT JOIN test_nma_changes t ON h.chld_manager_account_id = t.manager_account_id
WHERE t.manager_account_id IS NULL
  AND h.level__ > 1  -- Root managers are expected to potentially not be in final output
ORDER BY h.level__ DESC, h.chld_manager_account_id;

-- Query 6.3: Compare record counts between processing stages
SELECT 
    'Raw Relationships' as stage,
    COUNT(*) as record_count
FROM resolved_relationships

UNION ALL

SELECT 
    'Hierarchy Table' as stage,
    COUNT(*) as record_count
FROM aapt_dw.d_mngr_accnt_nma_hierarchy

UNION ALL

SELECT 
    'Three-Union Output' as stage,
    COUNT(*) as record_count  
FROM dim_prtnr_adv_entt_mpng_nma__

UNION ALL

SELECT 
    'Final Consolidated' as stage,
    COUNT(*) as record_count
FROM test_nma_changes

ORDER BY 
    CASE stage
        WHEN 'Raw Relationships' THEN 1
        WHEN 'Hierarchy Table' THEN 2
        WHEN 'Three-Union Output' THEN 3
        WHEN 'Final Consolidated' THEN 4
    END;

-- ===============================================================================
-- EXAMPLE 7: SAMPLE TEST DATA GENERATION (FOR DEVELOPMENT)
-- ===============================================================================
-- Purpose: Generate sample data for testing (use in development environment only)

/*
-- Create sample hierarchy for testing
INSERT INTO src_global_entity_relationship VALUES
('amzn1.ads1.ma.root.001', 'amzn1.ads1.ma.child.001', '2024-01-01'::timestamp, '2024-01-01'::timestamp, 'N'),
('amzn1.ads1.ma.root.001', 'amzn1.ads1.ma.child.002', '2024-01-01'::timestamp, '2024-01-01'::timestamp, 'N'),
('amzn1.ads1.ma.child.001', 'amzn1.ads1.ma.grandchild.001', '2024-02-01'::timestamp, '2024-02-01'::timestamp, 'N');

-- Create sample partner accounts
INSERT INTO dim_partner_accounts VALUES
('amzn1.ads1.ma.root.001', 'amzn1.ads1.ma.group.001', 'Y', 'Y', 'N'),
('amzn1.ads1.ma.child.001', NULL, 'Y', 'Y', 'N'),
('amzn1.ads1.ma.child.002', NULL, 'Y', 'Y', 'N');

-- Create sample entity mappings
INSERT INTO dim_partner_adv_entity_mapping VALUES
('amzn1.ads1.ma.root.001', 'entity.001', 0, '2024-01-01', '2099-12-31', 'PN', 'advertiser.001', 'Y'),
('amzn1.ads1.ma.child.001', 'entity.002', 0, '2024-01-01', '2024-06-30', 'PN', 'advertiser.002', 'Y'),
('amzn1.ads1.ma.child.001', 'entity.002', 0, '2024-07-01', '2099-12-31', 'PN', 'advertiser.002', 'Y');
*/

-- ===============================================================================
-- END OF EXAMPLES
-- ===============================================================================
