/*
================================================================================
HEALTHCARE CLAIMS DATA INTEGRATION PIPELINE
================================================================================

Purpose: Integrate treatment and procedure claims data from multiple data sources
         (Komodo and Symphony) using sophisticated matching algorithms.

Author: Data Engineering Team
Date: 2024
Version: 1.0

Overview:
1. BASE TABLES: Process raw treatment and procedure data
2. SCENARIO 1: Match anchor claims across data sources with date tolerance
3. SCENARIO 3: Match non-anchor claims with exact criteria
4. INTEGRATION: Combine all scenarios into final integrated table

Data Sources:
- Komodo: Primary healthcare claims database
- Symphony: Secondary healthcare claims database

Key Business Rules:
- Date matching tolerance: ±5 days
- Only process PAID claims
- Priority given to non-null provider/cost information
- Patient matching via token bridge system
================================================================================
*/

-- ============================================================================
-- SECTION 1: BASE TABLE PREPARATION
-- ============================================================================

/*
PURPOSE: Create standardized treatment claims table from source data
INPUTS: JNJ_CDW_DEV.CDW_CURATED.treatment, TBLPATIENTTOKENS_BRIDGE
OUTPUT: tbl_gj_treatment_temp
*/
CREATE OR REPLACE TABLE tbl_gj_treatment_temp AS 
WITH treatment_temp AS (
    SELECT
        -- Data source identification
        t.DATASOURCE,
        SOURCE,
        
        -- Patient identification (handle special null case)
        CASE 
            WHEN token2 = 'XXX - F00000' THEN NULL 
            ELSE token1 
        END AS PATIENT_TOKEN_1,
        token2 AS PATIENT_TOKEN_2,
        CASE 
            WHEN t.DATASOURCE = 'Komodo' THEN PATIENT_ID 
        END AS PATIENT_ID,
        
        -- Claim details
        TREATMENT_CLAIM_ID AS claim_id,
        PROVIDER_ID,
        TREATMENT_DOS AS DOS,
        DAYS_SUPPLY,
        
        -- Financial information
        OUT_OF_POCKET,
        PATIENT_PAY,
        
        -- Claim status and processing
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        TREATMENT_CODE AS CODE,
        
        -- Trinity group classifications
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        
        -- Business rule: Flag for paid claims only
        CASE 
            WHEN TREATMENT_CLAIM_STATUS IN ('1', 'PAID') THEN 1 
            ELSE 0 
        END AS PAID_FLAG
        
    FROM JNJ_CDW_DEV.CDW_CURATED.treatment t
        LEFT JOIN JNJ_CDW_DEV.CDW_CURATED.TBLPATIENTTOKENS_BRIDGE token 
            ON token.original_token_1 = t.patient_token_1 
            AND token.original_token_2 = t.PATIENT_TOKEN_2
        LEFT JOIN JNJ_CDW_DEV.CDW_RAW.IST_TRN_HEALTHCARE_CODE_REFERENCE_RAW r 
            ON t.TREATMENT_CODE = r.code
)      
SELECT * FROM treatment_temp;

/*
PURPOSE: Create standardized procedure claims table from source data
INPUTS: JNJ_CDW_DEV.CDW_CURATED.procedure, TBLPATIENTTOKENS_BRIDGE
OUTPUT: tbl_gj_procedure_temp
NOTE: Procedures are treated as always PAID with status '1'
*/
CREATE OR REPLACE TABLE tbl_gj_procedure_temp AS 
WITH procedure_temp AS (
    SELECT
        -- Data source identification
        a.DATASOURCE,
        SOURCE,
        
        -- Patient identification (handle special null case)
        CASE 
            WHEN token2 = 'XXX - F00000' THEN NULL 
            ELSE token1 
        END AS PATIENT_TOKEN_1,
        token2 AS PATIENT_TOKEN_2,
        CASE 
            WHEN a.DATASOURCE = 'Komodo' THEN PATIENT_ID 
        END AS PATIENT_ID,
        
        -- Claim details
        PROCEDURE_CLAIM_ID AS claim_id,
        PROVIDER_ID,
        PROCEDURE_DOS AS DOS,
        NULL AS DAYS_SUPPLY,  -- Procedures don't have supply days
        
        -- Financial information
        OUT_OF_POCKET,
        PATIENT_PAY,
        
        -- Business rule: All procedures considered paid
        '1' AS TREATMENT_CLAIM_STATUS,
        'PAID' AS TREATMENT_FINAL_CLAIM_INDICATOR,
        PROCEDURE_CODE AS CODE,
        
        -- Trinity group classifications
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        
        -- Business rule: All procedures flagged as paid
        1 AS PAID_FLAG
        
    FROM JNJ_CDW_DEV.CDW_CURATED.procedure A
        LEFT JOIN JNJ_CDW_DEV.CDW_CURATED.TBLPATIENTTOKENS_BRIDGE token 
            ON token.original_token_1 = A.patient_token_1 
            AND token.original_token_2 = A.PATIENT_TOKEN_2
        LEFT JOIN JNJ_CDW_DEV.CDW_RAW.IST_TRN_HEALTHCARE_CODE_REFERENCE_RAW C 
            ON A.PROCEDURE_CODE = C.CODE
) 
SELECT * FROM procedure_temp;

-- ============================================================================
-- SECTION 2: DATA SEGMENTATION BY SOURCE AND ANCHOR STATUS
-- ============================================================================

/*
PURPOSE: Separate data into anchor vs non-anchor segments for different matching strategies
BUSINESS RULE: Anchor claims have TRINITY_GROUP1 classification
*/

-- Komodo anchor claims (have Trinity classification)
CREATE OR REPLACE TABLE tbl_gj_komodo_anchor AS
WITH komodo_anchor AS (
    SELECT * FROM tbl_gj_treatment_temp 
    WHERE TRINITY_GROUP1 IS NOT NULL AND datasource = 'Komodo'
    
    UNION ALL
    
    SELECT * FROM tbl_gj_procedure_temp 
    WHERE TRINITY_GROUP1 IS NOT NULL AND datasource = 'Komodo'    
) 
SELECT * FROM komodo_anchor;

-- Symphony anchor claims (have Trinity classification)
CREATE OR REPLACE TABLE tbl_gj_sha_anchor AS
WITH sha_anchor AS (
    SELECT * FROM tbl_gj_treatment_temp 
    WHERE TRINITY_GROUP1 IS NOT NULL AND datasource = 'Symphony'
    
    UNION ALL
    
    SELECT * FROM tbl_gj_procedure_temp 
    WHERE TRINITY_GROUP1 IS NOT NULL AND datasource = 'Symphony'        
)
SELECT * FROM sha_anchor;

-- Komodo non-anchor claims (no Trinity classification)
CREATE OR REPLACE TABLE tbl_gj_komodo_non_anchor AS
WITH komodo_non_anchor AS (
    SELECT * FROM tbl_gj_treatment_temp 
    WHERE TRINITY_GROUP1 IS NULL AND datasource = 'Komodo'
    
    UNION ALL
    
    SELECT * FROM tbl_gj_procedure_temp 
    WHERE TRINITY_GROUP1 IS NULL AND datasource = 'Komodo'    
) 
SELECT * FROM komodo_non_anchor;

-- Symphony non-anchor claims (no Trinity classification)
CREATE OR REPLACE TABLE tbl_gj_sha_non_anchor AS
WITH sha_non_anchor AS (
    SELECT * FROM tbl_gj_treatment_temp 
    WHERE TRINITY_GROUP1 IS NULL AND datasource = 'Symphony'
    
    UNION ALL
    
    SELECT * FROM tbl_gj_procedure_temp 
    WHERE TRINITY_GROUP1 IS NULL AND datasource = 'Symphony'      
) 
SELECT * FROM sha_non_anchor;

-- ============================================================================
-- SECTION 3: SCENARIO 1 - ANCHOR CLAIMS MATCHING
-- ============================================================================

/*
PURPOSE: Match anchor claims between Komodo and Symphony with flexible date matching
MATCHING CRITERIA:
- Same patient (token1 & token2)
- Same treatment/procedure code
- Date of service within ±5 days
- Both claims must be PAID
PRIORITY: Closest date match, then non-null supplementary data
*/

-- Step 3A: Komodo-primary matching (Komodo claims matched to Symphony)
CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_1_paid AS
WITH anchor_linked AS (
    SELECT DISTINCT 
        'Both' AS data_source,
        
        -- Primary Komodo claim data
        a.*,
        
        -- Matched Symphony claim data
        b.claim_id AS sha_claim_id,
        b.DOS AS sha_dos,
        b.PROVIDER_ID AS sha_provider_id,
        b.OUT_OF_POCKET AS sha_out_of_pocket,
        b.PATIENT_PAY AS sha_patient_pay,
        b.days_supply AS sha_days_supply,
        
        -- Flags for non-null Symphony data (for prioritization)
        CASE WHEN b.PROVIDER_ID IS NOT NULL THEN 1 ELSE 0 END AS non_null_sha_provider_id,
        CASE WHEN b.OUT_OF_POCKET IS NOT NULL THEN 1 ELSE 0 END AS non_null_sha_out_of_pocket,
        CASE WHEN b.PATIENT_PAY IS NOT NULL THEN 1 ELSE 0 END AS non_null_sha_patient_pay,
        CASE WHEN b.days_supply IS NOT NULL THEN 1 ELSE 0 END AS non_null_sha_days_supply,
        
        -- Date difference for ranking
        ABS(DATEDIFF(day, a.dos, b.dos)) AS abs_date_diff
        
    FROM tbl_gj_komodo_anchor a
        INNER JOIN tbl_gj_sha_anchor b
            ON a.patient_token_1 = b.patient_token_1 
            AND a.patient_token_2 = b.patient_token_2 
            AND a.code = b.code
            AND ABS(DATEDIFF(day, a.dos, b.dos)) <= 5  -- ±5 day tolerance
            AND a.PAID_FLAG = 1 
            AND b.paid_flag = 1
),
rank_anchor_linked AS (
    SELECT *,
        -- Primary ranking: closest date match
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY ABS_DATE_DIFF
        ) AS RN,
        
        -- Specialized rankings for data augmentation (prefer non-null + closest date)
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_sha_provider_id DESC, ABS_DATE_DIFF
        ) AS RN_sha_provider_id,
        
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_sha_out_of_pocket DESC, ABS_DATE_DIFF
        ) AS RN_sha_out_of_pocket,
        
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_sha_patient_pay DESC, ABS_DATE_DIFF
        ) AS RN_sha_patient_pay,
        
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_sha_days_supply DESC, ABS_DATE_DIFF
        ) AS RN_sha_days_supply
        
    FROM anchor_linked
)
SELECT * FROM rank_anchor_linked;

-- Step 3B: Symphony-primary matching (Symphony claims not already matched)
CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_2_paid AS
WITH anchor_linked AS (
    SELECT DISTINCT 
        'Both' AS data_source,
        
        -- Primary Symphony claim data
        a.*,
        
        -- Matched Komodo claim data
        b.claim_id AS komodo_claim_id,
        b.DOS AS komodo_dos,  -- Note: This should probably be komodo_dos
        b.PROVIDER_ID AS komodo_provider_id,
        b.OUT_OF_POCKET AS komodo_out_of_pocket,
        b.PATIENT_PAY AS komodo_patient_pay,
        b.days_supply AS komodo_days_supply,
        
        -- Flags for non-null Komodo data (for prioritization)
        CASE WHEN b.PROVIDER_ID IS NOT NULL THEN 1 ELSE 0 END AS non_null_komodo_provider_id,
        CASE WHEN b.OUT_OF_POCKET IS NOT NULL THEN 1 ELSE 0 END AS non_null_komodo_out_of_pocket,
        CASE WHEN b.PATIENT_PAY IS NOT NULL THEN 1 ELSE 0 END AS non_null_komodo_patient_pay,
        CASE WHEN b.days_supply IS NOT NULL THEN 1 ELSE 0 END AS non_null_komodo_days_supply,
        
        -- Date difference for ranking
        ABS(DATEDIFF(day, a.dos, b.dos)) AS abs_date_diff
        
    FROM tbl_gj_sha_anchor a
        INNER JOIN tbl_gj_komodo_anchor b
            ON a.patient_token_1 = b.patient_token_1 
            AND a.patient_token_2 = b.patient_token_2 
            AND a.code = b.code
            AND ABS(DATEDIFF(day, a.dos, b.dos)) <= 5  -- ±5 day tolerance
            -- Exclude Symphony claims already matched in step 3A
            AND a.claim_id || a.code NOT IN (
                SELECT DISTINCT sha_claim_id || code 
                FROM JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_1_paid
            )
            AND a.PAID_FLAG = 1 
            AND b.paid_flag = 1
),
rank_anchor_linked AS (
    SELECT *,
        -- Primary ranking: closest date match
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY ABS_DATE_DIFF
        ) AS RN,
        
        -- Specialized rankings for data augmentation
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_komodo_provider_id DESC, ABS_DATE_DIFF
        ) AS RN_komodo_provider_id,
        
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_komodo_out_of_pocket DESC, ABS_DATE_DIFF
        ) AS RN_komodo_out_of_pocket,
        
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_komodo_patient_pay DESC, ABS_DATE_DIFF
        ) AS RN_komodo_patient_pay,
        
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_komodo_days_supply DESC, ABS_DATE_DIFF
        ) AS RN_komodo_days_supply
        
    FROM anchor_linked
)
SELECT * FROM rank_anchor_linked;

-- ============================================================================
-- SECTION 4: SCENARIO 1 - CONSOLIDATION OF BEST MATCHES
-- ============================================================================

/*
PURPOSE: Select the best match for each claim and prepare for data augmentation
RULE: Take RN=1 (closest date match) as primary choice
*/

-- Step 4A: Best Komodo-primary matches
CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.tbl_gj_consolidated_anchor_1_paid AS
WITH consolidated_anchor AS (
    SELECT * FROM tbl_gj_rank_anchor_linked_1_paid
    WHERE RN = 1  -- Best match only
)
SELECT * FROM consolidated_anchor;

-- Step 4B: Best Symphony-primary matches  
CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.tbl_gj_consolidated_anchor_2_paid AS
WITH consolidated_anchor AS (
    SELECT * FROM tbl_gj_rank_anchor_linked_2_paid
    WHERE RN = 1  -- Best match only
)
SELECT * FROM consolidated_anchor;

-- ============================================================================
-- SECTION 5: SCENARIO 1 - DATA AUGMENTATION AND FINALIZATION
-- ============================================================================

/*
PURPOSE: Create final Scenario 1 table with augmented data from best available sources
STRATEGY: Use COALESCE to fill missing data from alternative matches
PRIORITY: Primary claim → Matched claim → Best alternative match
*/

CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_1_paid AS
WITH augment_consolidated_anchor_1 AS (
    SELECT DISTINCT 
        a.*,
        
        -- Additional augmentation data from specialized rankings
        b.sha_PROVIDER_ID AS augment_sha_provider_id,
        c.sha_OUT_OF_POCKET AS augment_sha_out_of_pocket,
        d.sha_PATIENT_PAY AS augment_sha_patient_pay,
        e.sha_days_supply AS augment_sha_days_supply,

        -- Final consolidated fields using priority hierarchy
        COALESCE(a.PROVIDER_ID, a.sha_provider_id, b.sha_PROVIDER_ID) AS final_provider_id,
        COALESCE(a.OUT_OF_POCKET, a.sha_OUT_OF_POCKET, c.sha_OUT_OF_POCKET) AS final_OUT_OF_POCKET,
        COALESCE(a.PATIENT_PAY, a.sha_PATIENT_PAY, d.sha_PATIENT_PAY) AS final_PATIENT_PAY,
        COALESCE(a.DAYS_SUPPLY, a.sha_days_supply, e.sha_days_supply) AS final_days_supply

    FROM tbl_gj_consolidated_anchor_1_paid A
        -- Best provider ID augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_1_paid
            WHERE RN_sha_provider_id = 1 AND non_null_sha_provider_id = 1
        ) B ON a.patient_token_1 = b.patient_token_1 
            AND a.patient_token_2 = b.patient_token_2 
            AND A.CODE = B.CODE 
            AND A.DOS = B.DOS
            
        -- Best out-of-pocket augmentation  
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_1_paid 
            WHERE RN_sha_out_of_pocket = 1 AND non_null_sha_out_of_pocket = 1
        ) C ON a.patient_token_1 = c.patient_token_1 
            AND a.patient_token_2 = c.patient_token_2 
            AND A.CODE = C.CODE 
            AND A.DOS = C.DOS
            
        -- Best patient pay augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_1_paid 
            WHERE RN_sha_patient_pay = 1 AND non_null_sha_patient_pay = 1
        ) D ON a.patient_token_1 = d.patient_token_1 
            AND a.patient_token_2 = d.patient_token_2 
            AND A.CODE = D.CODE 
            AND A.DOS = D.DOS
            
        -- Best days supply augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_1_paid 
            WHERE RN_sha_days_supply = 1 AND non_null_sha_days_supply = 1
        ) E ON a.patient_token_1 = e.patient_token_1 
            AND a.patient_token_2 = e.patient_token_2 
            AND A.CODE = E.CODE 
            AND A.DOS = E.DOS
),
augment_consolidated_anchor_2 AS (
    SELECT DISTINCT 
        a.*,
        
        -- Additional augmentation data from specialized rankings
        b.komodo_PROVIDER_ID AS augment_komodo_provider_id,
        c.komodo_OUT_OF_POCKET AS augment_komodo_out_of_pocket,
        d.komodo_PATIENT_PAY AS augment_komodo_patient_pay,
        e.komodo_days_supply AS augment_komodo_days_supply,

        -- Final consolidated fields using priority hierarchy
        COALESCE(a.PROVIDER_ID, a.komodo_provider_id, b.komodo_PROVIDER_ID) AS final_provider_id,
        COALESCE(a.OUT_OF_POCKET, a.komodo_OUT_OF_POCKET, c.komodo_OUT_OF_POCKET) AS final_OUT_OF_POCKET,
        COALESCE(a.PATIENT_PAY, a.komodo_PATIENT_PAY, d.komodo_PATIENT_PAY) AS final_PATIENT_PAY,
        COALESCE(a.DAYS_SUPPLY, a.komodo_days_supply, e.komodo_days_supply) AS final_days_supply

    FROM tbl_gj_consolidated_anchor_2_paid A
        -- Best provider ID augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_2_paid
            WHERE RN_komodo_provider_id = 1 AND non_null_komodo_provider_id = 1
        ) B ON a.patient_token_1 = b.patient_token_1 
            AND a.patient_token_2 = b.patient_token_2 
            AND A.CODE = B.CODE 
            AND A.DOS = B.DOS
            
        -- Best out-of-pocket augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_2_paid 
            WHERE RN_komodo_out_of_pocket = 1 AND non_null_komodo_out_of_pocket = 1
        ) C ON a.patient_token_1 = c.patient_token_1 
            AND a.patient_token_2 = c.patient_token_2 
            AND A.CODE = C.CODE 
            AND A.DOS = C.DOS
            
        -- Best patient pay augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_2_paid 
            WHERE RN_komodo_patient_pay = 1 AND non_null_komodo_patient_pay = 1
        ) D ON a.patient_token_1 = d.patient_token_1 
            AND a.patient_token_2 = d.patient_token_2 
            AND A.CODE = D.CODE 
            AND A.DOS = D.DOS
            
        -- Best days supply augmentation
        LEFT JOIN (
            SELECT * FROM tbl_gj_rank_anchor_linked_2_paid 
            WHERE RN_komodo_days_supply = 1 AND non_null_komodo_days_supply = 1
        ) E ON a.patient_token_1 = e.patient_token_1 
            AND a.patient_token_2 = e.patient_token_2 
            AND A.CODE = E.CODE 
            AND A.DOS = E.DOS
)
-- Combine both augmented datasets
SELECT * FROM augment_consolidated_anchor_1
UNION 
SELECT * FROM augment_consolidated_anchor_2;

-- ============================================================================
-- SECTION 6: SCENARIO 3 - NON-ANCHOR CLAIMS EXACT MATCHING
-- ============================================================================

/*
PURPOSE: Match non-anchor claims using exact criteria for high-confidence matches
MATCHING CRITERIA (ALL MUST MATCH):
- Same patient (token1 & token2)
- Same treatment/procedure code  
- Same provider ID
- Same out-of-pocket cost
- Same patient pay amount
- Date of service within ±5 days
- Both claims must be PAID
*/

CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid AS
SELECT 
    A.*,
    
    -- Flag for non-null Symphony days supply (for augmentation ranking)
    CASE WHEN B.days_supply IS NOT NULL THEN 1 ELSE 0 END AS non_null_sha_days_supply,
    
    -- Matched Symphony claim details
    B.CLAIM_ID AS SHA_CLAIM_ID,
    B.dos AS SHA_DOS,
    B.provider_id AS SHA_PROVIDER_ID,
    B.OUT_OF_POCKET AS SHA_OUT_OF_POCKET,
    B.patient_pay AS SHA_PATIENT_PAY,
    B.days_supply AS SHA_DAYS_SUPPLY,
    
    -- Date difference for ranking
    ABS(DATEDIFF(day, A.dos, B.dos)) AS abs_date_diff
    
FROM tbl_gj_komodo_non_anchor AS A
    INNER JOIN tbl_gj_sha_non_anchor AS B
        ON A.patient_token_1 = B.patient_token_1 
        AND A.patient_token_2 = B.patient_token_2 
        AND A.CODE = B.CODE
        AND ABS(DATEDIFF(day, A.DOS, B.DOS)) <= 5  -- ±5 day tolerance
        -- Exact matching criteria for high confidence
        AND A.PROVIDER_ID = B.PROVIDER_ID 
        AND A.OUT_OF_POCKET = B.OUT_OF_POCKET 
        AND A.PATIENT_PAY = B.PATIENT_PAY
        AND A.PAID_FLAG = 1 
        AND B.PAID_FLAG = 1;

-- Scenario 3 Final: Process and augment non-anchor matches
CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_3_paid AS
WITH rank_anchor_linked AS (
    SELECT *,
        -- Primary ranking by date proximity
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY ABS_DATE_DIFF
        ) AS RN,
        
        -- Specialized ranking for days supply augmentation
        ROW_NUMBER() OVER (
            PARTITION BY PATIENT_TOKEN_2, CODE, DOS 
            ORDER BY non_null_sha_days_supply DESC, ABS_DATE_DIFF
        ) AS RN_sha_days_supply
        
    FROM JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid
),
top_claims AS (
    -- Select best match for each claim
    SELECT DISTINCT * FROM rank_anchor_linked 
    WHERE RN = 1
),
augment_consolidated_anchor AS (
    SELECT DISTINCT 
        a.*,
        
        -- Augmented days supply using best available source
        COALESCE(a.DAYS_SUPPLY, a.sha_days_supply, e.sha_days_supply) AS final_days_supply

    FROM top_claims A
        -- Best days supply augmentation
        LEFT JOIN (
            SELECT * FROM rank_anchor_linked 
            WHERE RN_sha_days_supply = 1 AND non_null_sha_days_supply = 1
        ) E ON a.patient_token_1 = e.patient_token_1 
            AND a.patient_token_2 = e.patient_token_2 
            AND A.CODE = E.CODE 
            AND A.DOS = E.DOS
)
SELECT DISTINCT * FROM augment_consolidated_anchor;

-- ============================================================================
-- SECTION 7: FINAL INTEGRATION - COMBINE ALL SCENARIOS
-- ============================================================================

/*
PURPOSE: Create final integrated table combining all matching scenarios
HIERARCHY: Scenario 1 (anchor) → Scenario 3 (non-anchor) → Remaining unmatched
DEDUPLICATION: Prevent double-counting of claims across scenarios
*/

CREATE OR REPLACE TABLE JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_PAID AS
WITH 
-- Track claims used in Scenario 1 (anchor matching)
scenario_1_claims AS (
    SELECT claim_id || code AS claim_code_key
    FROM tbl_gj_rank_anchor_linked_1_paid
    
    UNION
    
    SELECT claim_id || code
    FROM tbl_gj_rank_anchor_linked_2_paid
    
    UNION
    
    SELECT sha_claim_id || code
    FROM tbl_gj_rank_anchor_linked_1_paid
    
    UNION
    
    SELECT komodo_claim_id || code
    FROM tbl_gj_rank_anchor_linked_2_paid
),

-- Track claims used in Scenario 3 (non-anchor matching)
scenario_3_claims AS (
    SELECT claim_id || code AS claim_code_key
    FROM JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid
    
    UNION
    
    SELECT SHA_CLAIM_ID || code
    FROM JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid
),

-- Final integrated result
integrated_data AS (
    /*
    SCENARIO 1: Anchor claims with cross-datasource matches
    - Highest priority due to Trinity classification and flexible matching
    */
    SELECT DISTINCT 
        DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS ENCOUNTER_KEY,
        FINAL_PROVIDER_ID AS PROVIDER_ID,
        DOS AS SVC_DATE,
        FINAL_DAYS_SUPPLY AS DAYS_SUPPLY,
        FINAL_OUT_OF_POCKET AS OUT_OF_POCKET,
        FINAL_PATIENT_PAY AS PATIENT_PAY,
        CODE AS CODES,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
    FROM tbl_gj_scenario_1_paid

    UNION

    /*
    SCENARIO 3: Non-anchor claims with exact matches
    - Second priority, excludes claims already in Scenario 1
    */
    SELECT DISTINCT 
        'Both' AS DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS ENCOUNTER_KEY,
        PROVIDER_ID,
        DOS AS SVC_DATE,
        FINAL_DAYS_SUPPLY AS DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        CODE AS CODES,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
    FROM tbl_gj_scenario_3_paid
    WHERE claim_id || code NOT IN (
        SELECT claim_code_key FROM scenario_1_claims
    )

    UNION

    /*
    REMAINING KOMODO ANCHOR CLAIMS: Unmatched anchor claims from Komodo
    - Third priority, preserves unmatched high-value claims
    */
    SELECT 
        'Komodo' AS DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS ENCOUNTER_KEY,
        PROVIDER_ID,
        DOS AS SVC_DATE,
        DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        CODE AS CODES,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
    FROM tbl_gj_komodo_anchor
    WHERE claim_id || code NOT IN (
        SELECT claim_code_key FROM scenario_1_claims
        UNION
        SELECT claim_code_key FROM scenario_3_claims
    )

    UNION

    /*
    REMAINING SYMPHONY ANCHOR CLAIMS: Unmatched anchor claims from Symphony
    - Fourth priority, completes anchor claim coverage
    */
    SELECT 
        'Symphony' AS DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS ENCOUNTER_KEY,
        PROVIDER_ID,
        DOS AS SVC_DATE,
        DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        CODE AS CODES,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
    FROM tbl_gj_sha_anchor
    WHERE claim_id || code NOT IN (
        SELECT claim_code_key FROM scenario_1_claims
        UNION
        SELECT claim_code_key FROM scenario_3_claims
    )

    UNION

    /*
    REMAINING KOMODO NON-ANCHOR CLAIMS: All other unmatched Komodo claims
    - Fifth priority, ensures comprehensive Komodo coverage
    */
    SELECT 
        'Komodo' AS DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS ENCOUNTER_KEY,
        PROVIDER_ID,
        DOS AS SVC_DATE,
        DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        CODE AS CODES,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
    FROM tbl_gj_komodo_non_anchor
    WHERE claim_id || code NOT IN (
        SELECT claim_code_key FROM scenario_1_claims
        UNION
        SELECT claim_code_key FROM scenario_3_claims
    )

    UNION

    /*
    REMAINING SYMPHONY NON-ANCHOR CLAIMS: All other unmatched Symphony claims  
    - Final priority, ensures comprehensive Symphony coverage
    */
    SELECT 
        'Symphony' AS DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS ENCOUNTER_KEY,
        PROVIDER_ID,
        DOS AS SVC_DATE,
        DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        CODE AS CODES,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
    FROM tbl_gj_sha_non_anchor
    WHERE claim_id || code NOT IN (
        SELECT claim_code_key FROM scenario_1_claims
        UNION
        SELECT claim_code_key FROM scenario_3_claims
    )
)
SELECT * FROM integrated_data;

-- ============================================================================
-- END OF HEALTHCARE CLAIMS INTEGRATION PIPELINE
-- ============================================================================

/*
VALIDATION QUERIES (OPTIONAL - UNCOMMENT FOR DATA QUALITY CHECKS):

-- Check record counts by scenario
SELECT 
    DATA_SOURCE,
    COUNT(*) AS record_count,
    COUNT(DISTINCT PATIENT_TOKEN_2) AS unique_patients,
    COUNT(DISTINCT ENCOUNTER_KEY) AS unique_encounters
FROM JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_PAID
GROUP BY DATA_SOURCE;

-- Validate no duplicates exist
SELECT 
    ENCOUNTER_KEY, 
    CODES, 
    COUNT(*) 
FROM JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_PAID
GROUP BY ENCOUNTER_KEY, CODES
HAVING COUNT(*) > 1;

-- Check data augmentation effectiveness
SELECT 
    CASE WHEN PROVIDER_ID IS NOT NULL THEN 'Provider_Complete' ELSE 'Provider_Missing' END AS provider_status,
    CASE WHEN OUT_OF_POCKET IS NOT NULL THEN 'Cost_Complete' ELSE 'Cost_Missing' END AS cost_status,
    COUNT(*) AS record_count
FROM JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_PAID
GROUP BY provider_status, cost_status;
*/