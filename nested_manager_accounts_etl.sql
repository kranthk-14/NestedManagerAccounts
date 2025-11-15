-- ===============================================================================
-- COMPLETE HIERARCHY ETL WITH TEMPORAL INHERITANCE LOGIC - PRODUCTION VERSION
-- ===============================================================================
-- Purpose: Build comprehensive partner-advertiser entity mapping with hierarchy
-- Key Features: 
--   - ✅ Temporal inheritance (accounts become root PN managers after hierarchy ends)
--   - ✅ Date-aware validation logic in ALL unions
--   - ✅ Three-union processing (hierarchy, standalone, group)
--   - ✅ Comprehensive deduplication and consolidation
--   - ✅ Production-ready with proper indexing and permissions
--   - ✅ Complete breakpoint logic for post-hierarchy transition periods
--   - ✅ Robust cycle prevention and relationship normalization
-- 
-- Created: September 2025
-- Version: 2.0 - Full Temporal Inheritance Implementation
-- ===============================================================================

-- ===============================================================================
-- STEP 1: EXTRACT AND NORMALIZE ACCOUNT RELATIONSHIPS
-- ===============================================================================

DROP TABLE IF EXISTS acc_links__;
CREATE TEMP TABLE acc_links__ AS
SELECT
    a.manager_account_id,
    linked_account_id,
    a.marketplace_id,
    CAST(a.effective_start_date AS DATE) AS effective_start_date,
    CAST(CASE WHEN a.active_flag_int=0 THEN a.effective_end_date ELSE '2099-12-31 00:00:00' END AS DATE) AS effective_end_date,
    CAST(effective_end_date AS TIMESTAMP) effective_end_date_tm, 
    'Y' AS is_linked,
    'pn' AS source
FROM (
    SELECT
        manager_account_id,
        linked_account_id,
        marketplace_id,
        created_at AS effective_start_date,
        last_updated_at AS effective_end_date,
        active_flag_int AS active_flag_int
    FROM (
        SELECT
            CAST(a.created_at AS DATE) AS created_at,
            a.entity_id_two AS linked_account_id,
            a.entity_id_one AS manager_account_id,
            updated_at last_updated_at,
            CASE WHEN delete_flag='Y' THEN 0 ELSE 1 END AS active_flag_int,
            0 marketplace_id
        FROM (
            SELECT
                entity_id_one,
                entity_id_two,
                updated_at,
                created_at,
                delete_flag,
                ROW_NUMBER() OVER (
                    PARTITION BY entity_id_one,entity_id_two,CAST(a.created_at AS DATE) 
                    ORDER BY created_at DESC ,updated_at DESC
                ) AS rn
            FROM andes."adam-manager-account-prod".src_global_entity_relationship a
            WHERE (entity_id_one ILIKE '%amzn1.ads1.ma%' AND entity_id_two ILIKE '%amzn1.ads1.ma%')
        ) a
        WHERE rn=1
    )
) a;

-- ===============================================================================
-- STEP 2: FIX RELATIONSHIP LOOPS AND DUPLICATES
-- ===============================================================================

DROP TABLE IF EXISTS normalized_relationships;
CREATE TABLE normalized_relationships AS
WITH ordered_pairs AS (
    SELECT 
        LEAST(manager_account_id, linked_account_id) AS account_id_1,
        GREATEST(manager_account_id, linked_account_id) AS account_id_2,
        effective_start_date,
        effective_end_date,
        effective_end_date_tm,
        marketplace_id,
        is_linked,
        source,
        manager_account_id AS original_manager,
        linked_account_id AS original_linked
    FROM acc_links__
)
SELECT 
    *,
    ROW_NUMBER() OVER (
        PARTITION BY account_id_1, account_id_2, effective_start_date
        ORDER BY effective_end_date_tm DESC
    ) AS latest_version
FROM ordered_pairs;

-- Step 1: Get all PN-registered group accounts with explicit migration flag = 'Y'
CREATE TEMP TABLE all_pn_group_accounts AS
SELECT DISTINCT pn_acc.group_account_id
FROM andes.aapn_dw.dim_partner_accounts pn_acc
INNER JOIN barnegat_share.aapt_dw.d_pnag2_nma_mig_flg mig_flg
    ON pn_acc.group_account_id = mig_flg.group_account_id
WHERE pn_acc.group_account_id IS NOT NULL 
    AND UPPER(pn_acc.is_registration_approved) = 'Y'
    AND UPPER(pn_acc.is_external_partner_account) = 'Y'
    AND UPPER(pn_acc.is_disabled) = 'N'
    AND mig_flg.PNAG2_NMA_FLG = 'Y';  -- Only explicit 'Y' migration flag

-- Step 2: Get group accounts that appear in hierarchies (as child or parent)
CREATE TEMP TABLE group_accounts_in_hierarchies AS
SELECT DISTINCT account_id_1 AS group_account_id
FROM normalized_relationships 
WHERE account_id_1 IN (
    SELECT DISTINCT group_account_id
    FROM andes.aapn_dw.dim_partner_accounts 
    WHERE group_account_id IS NOT NULL 
        AND UPPER(is_registration_approved) = 'Y'
        AND UPPER(is_external_partner_account) = 'Y'
        AND UPPER(is_disabled) = 'N'
)

UNION

SELECT DISTINCT account_id_2 AS group_account_id
FROM normalized_relationships 
WHERE account_id_2 IN (
    SELECT DISTINCT group_account_id
    FROM andes.aapn_dw.dim_partner_accounts 
    WHERE group_account_id IS NOT NULL 
        AND UPPER(is_registration_approved) = 'Y'
        AND UPPER(is_external_partner_account) = 'Y'
        AND UPPER(is_disabled) = 'N'
);

-- Step 4: Get isolated group accounts (PN-registered but NOT in hierarchies)
DROP TABLE IF EXISTS isolated_group_accounts;
CREATE  TABLE isolated_group_accounts AS
SELECT distinct group_account_id
FROM andes.aapn_dw.dim_partner_accounts 
WHERE NOT( group_account_id  IN (SELECT group_account_id FROM all_pn_group_accounts));

-- Step 5: Optimized resolved_relationships table
DROP TABLE IF EXISTS resolved_relationships;
CREATE  TABLE resolved_relationships AS
SELECT 
    original_manager AS manager_account_id,
    original_linked AS linked_account_id,
    marketplace_id,
    effective_start_date,
    effective_end_date,
    effective_end_date_tm,
    is_linked,
    source
FROM normalized_relationships
WHERE latest_version = 1
    -- Optimized: Exclude relationships involving isolated group accounts
    AND not (
        original_manager  IN (SELECT group_account_id FROM isolated_group_accounts)
        OR original_linked  IN (SELECT group_account_id FROM isolated_group_accounts)
    );

-- ===============================================================================
-- STEP 3: BUILD HIERARCHY STRUCTURE WITH ENTITY TYPE ENRICHMENT
-- ===============================================================================

DROP TABLE IF EXISTS src_global_enty_rltnshp_hier_new;
CREATE TABLE src_global_enty_rltnshp_hier_new AS
WITH entity_types AS (
    SELECT DISTINCT 
        entity_id, 
        type 
    FROM barnegat_share.identity_hub.adam_global_entity_prod
)
SELECT DISTINCT 
    hier1.linked_account_id AS chld_manager_account_id,
    hier1.manager_account_id AS prnt_manager_account_id,
    hier1.effective_start_date,
    hier1.effective_end_date,
    hier1.is_linked,
    hier1.marketplace_id,
    child_entity.type AS chld_mngr_acnt_type,
    parent_entity.type AS prnt_mngr_acnt_type,
    
    -- Business Logic: Agency/Partner perpetual relationships
    CASE 
        WHEN hier1.effective_end_date < '2099-12-31 00:00:00' 
             AND (child_entity.type ILIKE '%Agency%' OR child_entity.type ILIKE '%Partner%')
        THEN NULL
        ELSE hier1.effective_end_date
    END AS effective_end_date_calculated,
    
    -- NMA Change Flag for date processing rules
    CASE 
        WHEN (child_entity.type ILIKE '%Agency%' OR child_entity.type ILIKE '%Partner%')
        THEN 0
        ELSE 1
    END AS nma_change_flg
FROM resolved_relationships hier1
LEFT OUTER JOIN entity_types child_entity 
    ON hier1.linked_account_id = child_entity.entity_id
LEFT OUTER JOIN entity_types parent_entity 
    ON hier1.manager_account_id = parent_entity.entity_id;

-- ===============================================================================
-- STEP 4: BUILD RECURSIVE HIERARCHY WITH CYCLE PREVENTION
-- ===============================================================================

DROP TABLE IF EXISTS subordinate___r;
CREATE TABLE subordinate___r AS
WITH RECURSIVE hierarchy (
    chld_manager_account_id,
    prnt_manager_account_id,
    level__,
    root_manager,
    chld_mngr_acnt_type,
    prnt_mngr_acnt_type,
    effective_start_date,
    effective_end_date,
    is_linked,
    marketplace_id,
    nma_change_flg,
    path_
) AS (
    -- Base case: Initial relationships
    SELECT
        chld_manager_account_id,
        prnt_manager_account_id,
        1 AS level__,
        prnt_manager_account_id AS root_manager,
        chld_mngr_acnt_type,
        prnt_mngr_acnt_type,
        effective_start_date,
        effective_end_date,
        is_linked,
        marketplace_id,
        nma_change_flg,
        CAST(prnt_manager_account_id AS VARCHAR) AS path_
    FROM src_global_enty_rltnshp_hier_new
    WHERE effective_end_date_calculated IS NOT NULL

    UNION ALL

    -- Recursive case with date range intersection
    SELECT
        child.chld_manager_account_id,
        child.prnt_manager_account_id,
        parent.level__ + 1,
        parent.root_manager,
        child.chld_mngr_acnt_type,
        child.prnt_mngr_acnt_type,
        GREATEST(child.effective_start_date, parent.effective_start_date),
        LEAST(child.effective_end_date, parent.effective_end_date),
        child.is_linked,
        child.marketplace_id,
        child.nma_change_flg,
        parent.path_ || '/' || child.prnt_manager_account_id
    FROM src_global_enty_rltnshp_hier_new child
    JOIN hierarchy parent ON 
        child.prnt_manager_account_id = parent.chld_manager_account_id
        AND child.effective_start_date <= parent.effective_end_date
        AND child.effective_end_date >= parent.effective_start_date
    WHERE 
        parent.level__ < 12
        AND child.effective_end_date_calculated IS NOT NULL
        -- Cycle prevention
        AND parent.path_ NOT LIKE '%/' || child.chld_manager_account_id || '/%'
        AND parent.path_ NOT LIKE child.chld_manager_account_id || '/%'
        AND parent.path_ != child.chld_manager_account_id
)
SELECT DISTINCT
    chld_manager_account_id,
    prnt_manager_account_id,
    root_manager,
    level__,
    chld_mngr_acnt_type,
    prnt_mngr_acnt_type,
    effective_start_date,
    effective_end_date,
    is_linked,
    marketplace_id,
    nma_change_flg,
    path_
FROM hierarchy;

-- ===============================================================================
-- STEP 5: CREATE PN-FOCUSED HIERARCHY SUBSET
-- ===============================================================================

DROP TABLE IF EXISTS d_mngr_accnt_nma_hierarchy_rtpn_;
CREATE TABLE d_mngr_accnt_nma_hierarchy_rtpn_ AS
SELECT DISTINCT 
    t1.root_manager AS manager_account_id,
    s.prnt_manager_account_id  
FROM subordinate___r t1
INNER JOIN andes.aapn_dw.dim_partner_accounts pn_acc ON (t1.root_manager = pn_acc.partner_account_id)
LEFT OUTER JOIN src_global_enty_rltnshp_hier_new s ON (s.chld_manager_account_id=t1.root_manager AND effective_end_date_calculated IS NOT NULL)
WHERE UPPER(is_registration_approved) = 'Y'
AND UPPER(is_disabled) = 'N'
AND UPPER(is_external_partner_account) = 'Y'

UNION 

SELECT DISTINCT 
    t1.chld_manager_account_id AS manager_account_id,
    s.prnt_manager_account_id  
FROM subordinate___r t1
INNER JOIN andes.aapn_dw.dim_partner_accounts pn_acc ON (t1.chld_manager_account_id = pn_acc.partner_account_id)
LEFT OUTER JOIN src_global_enty_rltnshp_hier_new s ON (s.chld_manager_account_id=t1.chld_manager_account_id AND effective_end_date_calculated IS NOT NULL) 
WHERE UPPER(is_registration_approved) = 'Y'
AND UPPER(is_disabled) = 'N'
AND UPPER(is_external_partner_account) = 'Y';

-- ===============================================================================
-- STEP 6: BUILD PN MANAGER DETECTION LOGIC
-- ===============================================================================

DROP TABLE IF EXISTS subordinate__rpm;
CREATE TABLE subordinate__rpm AS
WITH RECURSIVE subordinate(lvl, manager_account_id, prnt_manager_account_id, leaf_manager_account_id, pn_mngr_flg, path_) AS (   
    -- Base case
    SELECT 
        1 AS lvl,
        manager_account_id,
        prnt_manager_account_id,
        manager_account_id AS leaf_manager_account_id,
        CASE WHEN pn_acc.partner_account_id IS NOT NULL THEN 1 ELSE 0 END AS pn_mngr_flg,
        CAST(manager_account_id AS VARCHAR) AS path_
    FROM d_mngr_accnt_nma_hierarchy_rtpn_ t1
    LEFT JOIN andes.aapn_dw.dim_partner_accounts pn_acc 
        ON (t1.prnt_manager_account_id = pn_acc.partner_account_id 
            AND UPPER(is_registration_approved) = 'Y' 
            AND UPPER(is_disabled) = 'N' 
            AND UPPER(is_external_partner_account) = 'Y')
    
    UNION ALL
    
    -- Recursive case
    SELECT 
        s.lvl + 1 AS lvl,
        e.manager_account_id,
        e.prnt_manager_account_id,
        s.leaf_manager_account_id,
        CASE WHEN pn_acc.partner_account_id IS NOT NULL THEN 1 ELSE 0 END AS pn_mngr_flg,
        s.path_ || '/' || e.manager_account_id
    FROM d_mngr_accnt_nma_hierarchy_rtpn_ e
    JOIN subordinate s ON (e.manager_account_id = s.prnt_manager_account_id)
    LEFT JOIN andes.aapn_dw.dim_partner_accounts pn_acc 
        ON (e.prnt_manager_account_id = pn_acc.partner_account_id 
            AND UPPER(is_registration_approved) = 'Y' 
            AND UPPER(is_disabled) = 'N' 
            AND UPPER(is_external_partner_account) = 'Y')
    WHERE s.lvl < 12
      AND e.prnt_manager_account_id IS NOT NULL
      AND s.path_ NOT LIKE '%/' || leaf_manager_account_id::VARCHAR || '/%'
      AND s.path_ NOT LIKE leaf_manager_account_id::VARCHAR || '/%'
)
SELECT DISTINCT 
    leaf_manager_account_id AS manager_account_id,
    MAX(pn_mngr_flg) AS pn_mngr_flg  
FROM subordinate 
GROUP BY leaf_manager_account_id;

-- ===============================================================================
-- STEP 7: CREATE FINAL HIERARCHY TABLE WITH BUSINESS FLAGS
-- ===============================================================================

DROP TABLE IF EXISTS aapt_dw.d_mngr_accnt_nma_hierarchy;
CREATE TABLE aapt_dw.d_mngr_accnt_nma_hierarchy AS
SELECT DISTINCT
    s.prnt_manager_account_id AS parent_manager_account_id,
    s.chld_manager_account_id AS chld_manager_account_id,
    s.level__ AS level__,
    s.root_manager AS root_manager_account_id,
    s.effective_start_date AS hier_start_date,
    s.effective_end_date AS hier_end_date,
    s.is_linked,
    s.nma_change_flg,
    s.path_,
    s.chld_mngr_acnt_type,
    s.prnt_mngr_acnt_type,
    
    -- Root Manager Validation Flag
    CASE WHEN hier.prnt_manager_account_id IS NOT NULL THEN 0 ELSE 1 END AS root_mngr_flg,
    
    -- Root PN Manager Flag
    CASE WHEN COALESCE(rpm.pn_mngr_flg, 99) > 0 THEN 0 ELSE 1 END AS root_pn_mngr_flg,
    
    -- Additional PN Manager Flag
    CASE WHEN dpa_current.partner_account_id IS NOT NULL THEN 'Y' ELSE 'N' END AS pn_manager_flag,
    
    -- Root comparison indicator
    CASE 
        WHEN dpa_root.partner_account_id IS NOT NULL THEN 'MATCH'
        WHEN dpa_root.partner_account_id IS NULL THEN 'NO_PN_ROOT'
        ELSE 'DIFFERENT'
    END AS root_comparison

FROM subordinate___r s
LEFT JOIN subordinate__rpm rpm ON s.root_manager = rpm.manager_account_id
LEFT JOIN (
    SELECT DISTINCT chld_manager_account_id, prnt_manager_account_id 
    FROM src_global_enty_rltnshp_hier_new
) hier ON s.root_manager = hier.chld_manager_account_id
LEFT JOIN andes.aapn_dw.dim_partner_accounts dpa_current 
    ON (s.chld_manager_account_id = dpa_current.partner_account_id 
        AND UPPER(dpa_current.is_registration_approved) = 'Y' 
        AND UPPER(dpa_current.is_disabled) = 'N' 
        AND UPPER(dpa_current.is_external_partner_account) = 'Y')
LEFT JOIN andes.aapn_dw.dim_partner_accounts dpa_root 
    ON (s.root_manager = dpa_root.partner_account_id 
        AND UPPER(dpa_root.is_registration_approved) = 'Y' 
        AND UPPER(dpa_root.is_disabled) = 'N' 
        AND UPPER(dpa_root.is_external_partner_account) = 'Y')
ORDER BY s.root_manager, s.level__, s.chld_manager_account_id;

-- ===============================================================================
-- STEP 8: CREATE TEMPORAL INHERITANCE LOGIC (CRITICAL FEATURE)
-- ===============================================================================

-- Migration flag logic now included in all_pn_group_accounts temp table

DROP TABLE IF EXISTS final_segments;
CREATE TABLE final_segments AS
WITH 
-- Create comprehensive date breakpoints including hierarchy end dates
date_breakpoints AS (
    SELECT DISTINCT 
        a.manager_account_id,
        a.entity_id,
        a.marketplace_id,
        a.source,
        a.advertiser_id,
        a.is_linked,
        a.effective_start_date AS original_start,
        a.effective_end_date AS original_end,
        a.effective_start_date AS breakpoint_date,
        'ENTITY_START' AS breakpoint_type
    FROM andes.identity_hub.dim_partner_adv_entity_mapping a
    WHERE a.entity_id IS NOT NULL AND a.advertiser_id IS NOT NULL
    
    UNION ALL
    
    -- Add hierarchy start dates as breakpoints
    SELECT DISTINCT 
        a.manager_account_id,
        a.entity_id,
        a.marketplace_id,
        a.source,
        a.advertiser_id,
        a.is_linked,
        a.effective_start_date AS original_start,
        a.effective_end_date AS original_end,
        nma.hier_start_date AS breakpoint_date,
        'HIERARCHY_START' AS breakpoint_type
    FROM andes.identity_hub.dim_partner_adv_entity_mapping a
    INNER JOIN aapt_dw.d_mngr_accnt_nma_hierarchy nma
        ON a.manager_account_id = nma.CHLD_MANAGER_ACCOUNT_ID
    WHERE nma.hier_start_date IS NOT NULL
        AND nma.hier_start_date >= a.effective_start_date 
        AND nma.hier_start_date <= a.effective_end_date
    
    UNION ALL
    
    -- CRITICAL: Add hierarchy end dates + 1 day as breakpoints
    -- This creates the segments where child accounts become root PN managers
    SELECT DISTINCT 
        a.manager_account_id,
        a.entity_id,
        a.marketplace_id,
        a.source,
        a.advertiser_id,
        a.is_linked,
        a.effective_start_date AS original_start,
        a.effective_end_date AS original_end,
        nma.hier_end_date + INTERVAL '1 day' AS breakpoint_date,
        'HIERARCHY_END_TRANSITION' AS breakpoint_type
    FROM andes.identity_hub.dim_partner_adv_entity_mapping a
    INNER JOIN aapt_dw.d_mngr_accnt_nma_hierarchy nma
        ON a.manager_account_id = nma.CHLD_MANAGER_ACCOUNT_ID
    WHERE nma.hier_end_date IS NOT NULL
        AND nma.hier_end_date < '2099-12-31'
        AND nma.hier_end_date + INTERVAL '1 day' <= a.effective_end_date
),

-- Create temporal segments with inheritance context
date_segments AS (
    SELECT 
        manager_account_id,
        entity_id,
        marketplace_id,
        source,
        advertiser_id,
        is_linked,
        original_start,
        original_end,
        breakpoint_date AS segment_start,
        COALESCE(
            LEAD(breakpoint_date) OVER (
                PARTITION BY manager_account_id, entity_id, marketplace_id, source, original_start, original_end 
                ORDER BY breakpoint_date
            ) - INTERVAL '1 day',
            original_end
        ) AS segment_end,
        breakpoint_type
    FROM date_breakpoints
),

-- Enhanced PN manager flag calculation with temporal inheritance - CORRECTED
segment_with_inheritance AS (
    SELECT 
        ds.*,
        pn_acc.partner_account_id,
        
        -- SIMPLIFIED TEMPORAL INHERITANCE LOGIC: Source-level filtering handles group logic
        CASE 
            WHEN pn_acc.partner_account_id IS NULL THEN 0  -- Not PN registered = can't be root PN manager
            WHEN COALESCE(pn_parents_check.has_pn_parents, 0) = 1 THEN 0  -- Has PN-registered parents = not root
            ELSE 1  -- PN registered with no PN parents = root PN manager
        END AS root_pn_mngr_flg_calculated
        
    FROM date_segments ds
    
    -- PN account validation
    LEFT JOIN andes.aapn_dw.dim_partner_accounts pn_acc 
        ON (ds.manager_account_id = pn_acc.partner_account_id 
            AND UPPER(is_registration_approved) = 'Y' 
            AND UPPER(is_disabled) = 'N' 
            AND UPPER(is_external_partner_account) = 'Y')
    
    -- Check for hierarchy during this segment - UPDATED FOR PERPETUAL HIERARCHIES
    LEFT JOIN aapt_dw.d_mngr_accnt_nma_hierarchy nma_active
        ON nma_active.chld_manager_account_id = ds.manager_account_id
        -- UPDATED: Include perpetual hierarchies (consistent with UNION 1 & 2)
        -- Simple overlap check - segment overlaps with hierarchy
        AND ds.segment_start <= nma_active.hier_end_date
        AND ds.segment_end >= nma_active.hier_start_date
    
    -- CRITICAL FIX: Check for PN-registered parents during THIS SPECIFIC SEGMENT only
    LEFT JOIN (
        SELECT DISTINCT 
            nma.chld_manager_account_id,
            nma.hier_start_date,
            nma.hier_end_date,
            1 AS has_pn_parents
        FROM aapt_dw.d_mngr_accnt_nma_hierarchy nma
        INNER JOIN andes.aapn_dw.dim_partner_accounts pn_parent
            ON (nma.parent_manager_account_id = pn_parent.partner_account_id 
                AND UPPER(pn_parent.is_registration_approved) = 'Y' 
                AND UPPER(pn_parent.is_disabled) = 'N' 
                AND UPPER(pn_parent.is_external_partner_account) = 'Y')
        WHERE pn_parent.partner_account_id IS NOT NULL
    ) pn_parents_check ON pn_parents_check.chld_manager_account_id = ds.manager_account_id
        -- CRITICAL: Only consider PN parents that overlap with current segment
        AND ds.segment_start <= pn_parents_check.hier_end_date
        AND ds.segment_end >= pn_parents_check.hier_start_date
)

SELECT DISTINCT
    manager_account_id,
    manager_account_id AS CHILD_MANAGER_ACCOUNT_ID,
    manager_account_id AS "path",
    0 AS "Level",
    'N' AS nma_is_linked,
    1 AS root_mngr_flg,  -- These are standalone accounts
    root_pn_mngr_flg_calculated AS root_pn_mngr_flg,  -- Use calculated inheritance logic
    0 AS nma_change_flg,
    is_linked,
    entity_id,
    marketplace_id,
    NULL AS chld_mngr_acnt_type,
    NULL AS prnt_mngr_acnt_type,
    segment_start AS original_start,
    segment_end AS original_end,
    source,
    advertiser_id
FROM segment_with_inheritance
WHERE segment_start <= segment_end
    AND advertiser_id IS NOT NULL;

-- ===============================================================================
-- STEP 9: CREATE FINAL DIMENSIONAL TABLE WITH THREE UNIONS (ALL DATE-AWARE)
-- ===============================================================================

DROP TABLE IF EXISTS dim_prtnr_adv_entt_mpng_nma__;
CREATE TABLE dim_prtnr_adv_entt_mpng_nma__ 
DISTKEY(MANAGER_ACCOUNT_ID) AS

-- ===============================================================================
-- UNION 1: HIERARCHY + ADVERTISER MAPPING (NOW WITH TEMPORAL LOGIC)
-- ===============================================================================

-- First, create the temporal precedence CTE
-- CORRECTED UNION 1: HIERARCHY + ADVERTISER MAPPING WITH FIXED TEMPORAL PRECEDENCE
WITH hierarchy_timeline AS (
    SELECT 
        nma.root_manager_account_id,
        nma.chld_manager_account_id,
        nma.level__,
        nma.hier_start_date,
        nma.hier_end_date,
        nma.is_linked,
        nma.nma_change_flg,
        nma.path_,
        nma.chld_mngr_acnt_type,
        nma.prnt_mngr_acnt_type,
        a.effective_start_date,
        a.effective_end_date,
        a.entity_id,
        a.marketplace_id,
        a.source,
        a.advertiser_id,
        a.is_linked as entity_is_linked,
        
        -- FIXED: Find the minimum level (highest precedence) for ALL overlapping hierarchies
        MIN(nma2.level__) OVER (
            PARTITION BY nma.root_manager_account_id, a.entity_id, a.marketplace_id, a.source,
                        a.effective_start_date, a.effective_end_date  -- Fixed partitioning
        ) AS highest_active_level
        
    FROM aapt_dw.d_mngr_accnt_nma_hierarchy nma
    INNER JOIN andes.identity_hub.dim_partner_adv_entity_mapping a 
        ON nma.CHLD_MANAGER_ACCOUNT_ID = a.manager_account_id
    
    -- FIXED: Include ALL hierarchy levels that could be active
    LEFT JOIN aapt_dw.d_mngr_accnt_nma_hierarchy nma2
        ON nma.root_manager_account_id = nma2.root_manager_account_id
        -- SIMPLIFIED: Just check if hierarchies overlap with entity mapping
        AND a.effective_start_date <= nma2.hier_end_date
        AND a.effective_end_date >= nma2.hier_start_date
    
    WHERE (
        (a.effective_start_date <= nma.HIER_end_DATE 
         AND a.effective_end_date >= nma.HIER_START_DATE 
         AND nma.nma_change_flg = 1) 
        OR nma.nma_change_flg = 0
    )
    AND a.effective_start_date <= nma.hier_end_date
    AND a.effective_end_date >= nma.hier_start_date
    AND a.entity_id IS NOT NULL
    AND a.advertiser_id IS NOT NULL
)

SELECT DISTINCT
    -- FIXED: Always use root manager as the manager_account_id for consistency
    ht.root_manager_account_id AS MANAGER_ACCOUNT_ID,
    
    ht.chld_manager_account_id AS CHLD_MANAGER_ACCOUNT_ID,
    
    -- FIXED: Show actual hierarchy path
    ht.path_ AS "path",
    
    -- FIXED: Use actual hierarchy level
    ht.level__ AS "level",
    
    ht.is_linked AS nma_is_linked,
    
    -- FIXED: Root manager is always root
    1 AS root_mngr_flg,
    
    -- FIXED: Root manager gets root_pn_mngr_flg = 1 if PN registered
    CASE 
        WHEN pn_acc_root.partner_account_id IS NOT NULL THEN 1  -- Root manager is PN registered
        ELSE 0
    END AS root_pn_mngr_flg,
    
    ht.nma_change_flg,
    ht.entity_is_linked AS is_linked,
    ht.entity_id,
    ht.marketplace_id,
    ht.chld_mngr_acnt_type,
    ht.prnt_mngr_acnt_type,
    
    -- Date intersection logic
    GREATEST(ht.hier_start_date, ht.effective_start_date) AS effective_start_date,
    LEAST(ht.hier_end_date, ht.effective_end_date) AS effective_end_date,
    
    ht.source,
    ht.advertiser_id

FROM hierarchy_timeline ht

-- PN registration check for root manager
LEFT JOIN andes.aapn_dw.dim_partner_accounts pn_acc_root
    ON (ht.root_manager_account_id = pn_acc_root.partner_account_id 
        AND UPPER(pn_acc_root.is_registration_approved) = 'Y' 
        AND UPPER(pn_acc_root.is_disabled) = 'N' 
        AND UPPER(pn_acc_root.is_external_partner_account) = 'Y')

WHERE GREATEST(ht.hier_start_date, ht.effective_start_date) <= LEAST(ht.hier_end_date, ht.effective_end_date)


UNION ALL

-- ===============================================================================
-- UNION 2: STANDALONE ACCOUNTS WITH TEMPORAL INHERITANCE
-- ===============================================================================

SELECT DISTINCT
    ds.manager_account_id AS manager_account_id,
    ds.manager_account_id AS CHILD_MANAGER_ACCOUNT_ID,
    ds.manager_account_id AS "path",
    0 AS "Level",
    'N' AS nma_is_linked,
    
    -- Date-aware root validation
    CASE WHEN hier.chld_manager_account_id IS NOT NULL THEN 0 ELSE 1 END AS root_mngr_flg,
    
    -- CRITICAL: Simplified temporal inheritance logic - direct from final_segments
    ds.root_pn_mngr_flg AS root_pn_mngr_flg,
    
    0 AS nma_change_flg,
    ds.is_linked,
    ds.entity_id,
    ds.marketplace_id,
    NULL AS chld_mngr_acnt_type,
    NULL AS prnt_mngr_acnt_type,
    ds.original_start AS effective_start_date,  
    ds.original_end AS effective_end_date,      
    ds.source,
    ds.advertiser_id

FROM final_segments ds

-- Date-aware hierarchy coverage check - ORIGINAL VERSION RESTORED
LEFT JOIN (
    SELECT DISTINCT
        nma.CHLD_MANAGER_ACCOUNT_ID AS chld_manager_account_id,
        nma.hier_start_date,
        nma.hier_end_date,
        nma.nma_change_flg,
        nma.ROOT_MANAGER_ACCOUNT_ID
    FROM aapt_dw.d_mngr_accnt_nma_hierarchy nma
) nma_coverage ON nma_coverage.chld_manager_account_id = ds.manager_account_id
    AND ds.original_start <= nma_coverage.hier_end_date
    AND ds.original_end >= nma_coverage.hier_start_date
    -- ORIGINAL FILTER RESTORED - this was labeled "CRITICAL FIX" in your original ETL
    AND NOT (ds.original_start > nma_coverage.hier_end_date)
    AND (
        (ds.original_start <= nma_coverage.hier_end_date
         AND ds.original_end >= nma_coverage.hier_start_date 
         AND nma_coverage.nma_change_flg = 1) 
        OR nma_coverage.nma_change_flg = 0
    )

-- PN account validation
LEFT JOIN andes.aapn_dw.dim_partner_accounts pn_acc 
    ON (ds.manager_account_id = pn_acc.partner_account_id 
        AND UPPER(pn_acc.is_registration_approved) = 'Y' 
        AND UPPER(pn_acc.is_disabled) = 'N' 
        AND UPPER(pn_acc.is_external_partner_account) = 'Y')

-- Date-aware hierarchy check for root_mngr_flg
LEFT JOIN (
    SELECT DISTINCT chld_manager_account_id, prnt_manager_account_id,
           effective_start_date, effective_end_date_calculated AS effective_end_date
    FROM src_global_enty_rltnshp_hier_new 
    WHERE effective_end_date_calculated IS NOT NULL
) hier ON ds.manager_account_id = hier.chld_manager_account_id
    AND ds.original_start <= COALESCE(hier.effective_end_date, '2099-12-31')
    AND ds.original_end >= hier.effective_start_date

WHERE ds.original_start <= ds.original_end
  AND ds.original_start IS NOT NULL 
  AND ds.original_end IS NOT NULL
  AND ds.advertiser_id IS NOT NULL   
  AND NOT(ds.manager_account_id IN (select group_account_id from isolated_group_accounts))

UNION ALL

-- ===============================================================================
-- UNION 3: GROUP-LEVEL PARTNER RELATIONSHIPS - UPDATED LOGIC
-- ===============================================================================
SELECT DISTINCT
    pn_acc.group_account_id AS manager_account_id,
    a.manager_account_id AS CHILD_MANAGER_ACCOUNT_ID,
    pn_acc.group_account_id || '/' || a.manager_account_id AS "path",
    1 AS "Level",
    'N' AS nma_is_linked,
    1 AS root_mngr_flg,      -- Group accounts are roots by design
    
    -- UPDATED: Only isolated group accounts get root_pn_mngr_flg = 1
    1 AS root_pn_mngr_flg,
    
    0 AS nma_change_flg,
    a.is_linked,
    a.entity_id,
    a.marketplace_id,
    NULL AS chld_mngr_acnt_type,
    NULL AS prnt_mngr_acnt_type,
    a.effective_start_date AS effective_start_date,
    a.effective_end_date AS effective_end_date,
    a.source,
    a.advertiser_id

FROM andes.aapn_dw.dim_partner_accounts pn_acc
INNER JOIN andes.identity_hub.dim_partner_adv_entity_mapping a   
    ON a.manager_account_id = pn_acc.partner_account_id

WHERE pn_acc.group_account_id IS NOT NULL
-- Source-level filtering handles migration and group logic
AND UPPER(is_registration_approved) = 'Y' 
AND UPPER(is_disabled) = 'N' 
AND UPPER(is_external_partner_account) = 'Y'
-- FIX: Filter out NULL entity_ids
AND a.entity_id IS NOT NULL
AND a.advertiser_id IS NOT NULL
and   pn_acc.group_account_id in (select group_account_id from isolated_group_accounts)
and pn_acc.partner_account_id <>  pn_acc.group_account_id

union all 


SELECT DISTINCT
    pn_acc.group_account_id AS manager_account_id,
    a.manager_account_id AS CHILD_MANAGER_ACCOUNT_ID,
    pn_acc.group_account_id || '/' || a.manager_account_id AS "path",
    1 AS "Level",
    'N' AS nma_is_linked,
    1 AS root_mngr_flg,      -- Group accounts are roots by design
    
    -- UPDATED: Only isolated group accounts get root_pn_mngr_flg = 1
    1 AS root_pn_mngr_flg,
    
    0 AS nma_change_flg,
    a.is_linked,
    a.entity_id,
    a.marketplace_id,
    NULL AS chld_mngr_acnt_type,
    NULL AS prnt_mngr_acnt_type,
    a.effective_start_date AS effective_start_date,
    a.effective_end_date AS effective_end_date,
    a.source,
    a.advertiser_id

FROM andes.aapn_dw.dim_partner_accounts pn_acc
INNER JOIN andes.identity_hub.dim_partner_adv_entity_mapping a   
    ON a.manager_account_id = pn_acc.group_account_id

WHERE pn_acc.group_account_id IS NOT NULL
-- Source-level filtering handles migration and group logic
AND UPPER(is_registration_approved) = 'Y' 
AND UPPER(is_disabled) = 'N' 
AND UPPER(is_external_partner_account) = 'Y'
-- FIX: Filter out NULL entity_ids
AND a.entity_id IS NOT NULL
AND a.advertiser_id IS NOT NULL
and   pn_acc.group_account_id in (select group_account_id from isolated_group_accounts)
and pn_acc.partner_account_id =  pn_acc.group_account_id
;

-- ===============================================================================
-- STEP 10: FINAL DEDUPLICATION AND CONSOLIDATION
-- ===============================================================================

DROP TABLE IF EXISTS test_nma_changes;
CREATE TABLE test_nma_changes as

WITH processed_data AS (
    -- Step 1: Prioritize and keep the most important records for any given time overlap.
    -- This handles cases where multiple sources exist for a single event.
    SELECT
        manager_account_id,
        entity_id,
        marketplace_id,
        effective_start_date,
        effective_end_date,
        source,
        chld_manager_account_id,
        "path",
        "level",
        root_mngr_flg,
        root_pn_mngr_flg,
        is_linked,
        advertiser_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    manager_account_id,
                    entity_id,
                    marketplace_id,
                    effective_start_date,
                    effective_end_date
                ORDER BY 
                root_pn_mngr_flg desc,
                    CASE UPPER(source)
                        WHEN 'GSO-DSP-SS' THEN 1
                        WHEN 'PN' THEN 2
                        WHEN 'API' THEN 3
                        WHEN 'RODEO' THEN 4
                        WHEN 'MANUAL' THEN 5
                        WHEN 'LCARS' THEN 6
                        WHEN 'GSO-DSP-MS' THEN 7
                        ELSE 999
                    END ASC,
                   
                    "level" ASC
            ) AS rn
        FROM dim_prtnr_adv_entt_mpng_nma__
    ) ranked
    WHERE rn = 1
),
ordered_data AS (
    -- Step 2: Order the data by start date and find the maximum end date for all preceding rows.
    SELECT
        manager_account_id,
        entity_id,
        marketplace_id,
        effective_start_date,
        effective_end_date,
        source,
        chld_manager_account_id,
        "path",
        "level",
        root_mngr_flg,
        root_pn_mngr_flg,
        is_linked,
        advertiser_id,
        MAX(effective_end_date) OVER (
            PARTITION BY
                manager_account_id,
                entity_id,
                marketplace_id
            ORDER BY effective_start_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS max_prev_end_date
    FROM processed_data
),
group_boundaries AS (
    -- Step 3: Identify the start of a new, true group by comparing a row's
    -- effective_start_date to the max_prev_end_date.
    SELECT
        *,
        CASE WHEN effective_start_date > COALESCE(max_prev_end_date, effective_start_date) THEN 1 ELSE 0 END AS group_start_flag
    FROM ordered_data
),
grouped_data AS (
    -- Step 4: Create the group_key as a running sum of the group_start_flag.
    SELECT
        *,
        SUM(group_start_flag) OVER (
            PARTITION BY
                manager_account_id,
                entity_id,
                marketplace_id
            ORDER BY effective_start_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS group_key
    FROM group_boundaries
),
merged_ranges AS (
    -- Step 5: Merge the date ranges and aggregate attributes for each group.
    SELECT
        manager_account_id,
        entity_id,
        marketplace_id,
        MIN(effective_start_date) AS effective_start_date,
        MAX(effective_end_date) AS effective_end_date,
        LISTAGG(DISTINCT source, ',') WITHIN GROUP (ORDER BY source) AS all_sources,
        MIN(chld_manager_account_id) AS chld_manager_account_id,
        MIN("path") AS "path",
        MIN("level") AS "level",
        MAX(root_mngr_flg) AS root_mngr_flg,
        MAX(root_pn_mngr_flg) AS root_pn_mngr_flg,
        MAX(is_linked) AS is_linked,
        MIN(advertiser_id) AS advertiser_id
    FROM grouped_data
    GROUP BY
        manager_account_id,
        entity_id,
        marketplace_id,
        group_key
)
-- Step 6: Final selection of the merged and aggregated records.
SELECT
distinct
    manager_account_id,
    entity_id,
    marketplace_id,
    effective_start_date,
    effective_end_date,

    all_sources source,
    chld_manager_account_id,
    "path",
    "level",
    root_mngr_flg,
    root_pn_mngr_flg,
    is_linked,
    advertiser_id
FROM merged_ranges
;

-- ===============================================================================
-- STEP 11: GRANT PERMISSIONS AND COMPLETE
-- ===============================================================================

GRANT SELECT ON test_nma_changes TO public;
GRANT SELECT ON dim_prtnr_adv_entt_mpng_nma__ TO public;
GRANT SELECT ON aapt_dw.d_mngr_accnt_nma_hierarchy TO public;
GRANT SELECT ON subordinate___r TO public;
GRANT SELECT ON subordinate__rpm TO public;
GRANT SELECT ON src_global_enty_rltnshp_hier_new TO public;
GRANT SELECT ON d_mngr_accnt_nma_hierarchy_rtpn_ TO public;
GRANT SELECT ON final_segments TO public;
GRANT SELECT ON resolved_relationships TO public;
GRANT SELECT ON normalized_relationships TO public;
GRANT SELECT ON isolated_group_accounts TO public;

-- ===============================================================================
-- STEP 12: FINAL OUTPUT
-- ===============================================================================

-- Final status
SELECT 
distinct
manager_account_id,
chld_manager_account_id,
"path",
 "level",
null nma_is_linked,
root_mngr_flg,
root_pn_mngr_flg,
null nma_change_flg,
is_linked,
entity_id,
marketplace_id,
null chld_mngr_acnt_type,
null prnt_mngr_acnt_type,
effective_start_date,
effective_end_date,
source,
advertiser_id
FROM test_nma_changes;

-- ===============================================================================
-- END OF ETL PIPELINE
-- ===============================================================================
