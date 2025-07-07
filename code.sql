
--------------------------------------------------------------
-- BASE TABLES
--------------------------------------------------------------
create or replace table tbl_gj_treatment_temp as 
----------------------------- step 2 -----------------------------------
with treatment_temp as (
    Select
        t.DATASOURCE,
        SOURCE,
        CASE WHEN token2 = 'XXX - F00000' THEN NULL ELSE token1 END AS PATIENT_TOKEN_1,
        token2 AS PATIENT_TOKEN_2,
        CASE WHEN t.DATASOURCE = 'Komodo' THEN PATIENT_ID END AS PATIENT_ID ,
        TREATMENT_CLAIM_ID as claim_id,
        PROVIDER_ID,
        TREATMENT_DOS as DOS,
        DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        TREATMENT_CODE as CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        CASE WHEN TREATMENT_CLAIM_STATUS IN ('1', 'PAID')
        THEN 1 ELSE 0 END AS PAID_FLAG
    from
        JNJ_CDW_DEV.CDW_CURATED.treatment t
        left join JNJ_CDW_DEV.CDW_CURATED.TBLPATIENTTOKENS_BRIDGE token 
        on token.original_token_1 = t.patient_token_1 and token.original_token_2 = t.PATIENT_TOKEN_2
        left join JNJ_CDW_DEV.CDW_RAW.IST_TRN_HEALTHCARE_CODE_REFERENCE_RAW r 
        on t.TREATMENT_CODE = r.code
         --WHERE (TREATMENT_FINAL_CLAIM_INDICATOR IN ('PAID') AND T.DATASOURCE IN ('Komodo')) OR (TREATMENT_CLAIM_STATUS IN ('1') AND T.DATASOURCE IN ('Symphony'))
    )      
select * from treatment_temp;

select count(distinct PATIENT_TOKEN_2 || CLAIM_ID), COUNT(DISTINCT PATIENT_TOKEN_1 || PATIENT_TOKEN_2 || CLAIM_ID) 
from tbl_gj_treatment_temp;
--256,848,244	261,844,748
--256,848,244	256,848,244
;

select count(distinct CLAIM_ID)
from tbl_gj_treatment_temp
;
--367,502,693
select count(distinct treatment_claim_id)
from JNJ_CDW_DEV.CDW_CURATED.treatment;
--367,502,693

SELECT * FROM tbl_gj_treatment_temp
WHERE CLAIM_ID ='ab99af9b94b1e904fd8a15b043257c37b0a78c9f8995fd78e3ccff0f7bb624be';

create or replace table tbl_gj_procedure_temp as 
----------------------------- step 2 -----------------------------------
with procedure_temp as (
    SELECT
        a.DATASOURCE,
        SOURCE,
        CASE WHEN token2 = 'XXX - F00000' THEN NULL ELSE token1 END AS PATIENT_TOKEN_1,
        token2 AS PATIENT_TOKEN_2,
        CASE WHEN a.DATASOURCE = 'Komodo' THEN PATIENT_ID END AS PATIENT_ID,
        PROCEDURE_CLAIM_ID as claim_id,
        PROVIDER_ID,
        PROCEDURE_DOS as DOS,
        NULL as DAYS_SUPPLY,
        OUT_OF_POCKET,
        PATIENT_PAY,
        '1' as TREATMENT_CLAIM_STATUS,
        'PAID' as TREATMENT_FINAL_CLAIM_INDICATOR,
        PROCEDURE_CODE as CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        1 AS PAID_FLAG
    FROM
        JNJ_CDW_DEV.CDW_CURATED.procedure A
        left join JNJ_CDW_DEV.CDW_CURATED.TBLPATIENTTOKENS_BRIDGE token 
        on token.original_token_1 = A.patient_token_1 and token.original_token_2 = A.PATIENT_TOKEN_2
        LEFT JOIN JNJ_CDW_DEV.CDW_RAW.IST_TRN_HEALTHCARE_CODE_REFERENCE_RAW C 
        on A.PROCEDURE_CODE = C.CODE
) select * from procedure_temp;

SELECT DISTINCT CLAIM_ID FROM tbl_gj_procedure_temp
UNION
SELECT DISTINCT CLAIM_ID FROM tbl_gj_treatment_temp;

-------------final treatment tables

create or replace table tbl_gj_komodo_anchor as
with komodo_anchor as (
    select *
    from tbl_gj_treatment_temp --created in step 2
    where TRINITY_GROUP1 is not null and datasource='Komodo'
    union all
    select *
    from tbl_gj_procedure_temp --created in step 2
    where TRINITY_GROUP1 is not null and datasource='Komodo'    
) 
select * from komodo_anchor;

create or replace table tbl_gj_sha_anchor as
with sha_anchor as (
    select *
    from tbl_gj_treatment_temp --created in step 2
    where TRINITY_GROUP1 is not null and datasource='Symphony'
    union all
    select *
    from tbl_gj_procedure_temp --created in step 2
    where TRINITY_GROUP1 is not null and datasource='Symphony'        
)
select * from sha_anchor;

create or replace table tbl_gj_komodo_non_anchor as
with komodo_non_anchor as (
    select *
    from tbl_gj_treatment_temp --created in step 2
    where TRINITY_GROUP1 is null and datasource='Komodo'
    union all
    select *
    from tbl_gj_procedure_temp --created in step 2
    where TRINITY_GROUP1 is null and datasource='Komodo'    
) select * from komodo_non_anchor;

create or replace table tbl_gj_sha_non_anchor as
with sha_non_anchor as (
    select *
    from tbl_gj_treatment_temp --created in step 2
    where TRINITY_GROUP1 is null and datasource='Symphony'
    union all
    select *
    from tbl_gj_procedure_temp --created in step 2
    where TRINITY_GROUP1 is null and datasource='Symphony'      
) select * from sha_non_anchor;


SELECT * FROM tbl_gj_komodo_anchor WHERE CLAIM_ID ='ab99af9b94b1e904fd8a15b043257c37b0a78c9f8995fd78e3ccff0f7bb624be';

--------------------------------------------------------------
-- Scenario 1
--------------------------------------------------------------
-- intermediate table 1
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_1_paid as
----------------------------- step 3 -----------------------------------
with anchor_linked as (
    select distinct 
          'Both' as data_source, 
           a.*,
           b.claim_id as sha_claim_id,
           b.DOS as sha_dos,
           b.PROVIDER_ID as sha_provider_id,
           b.OUT_OF_POCKET as sha_out_of_pocket,
           b.PATIENT_PAY as sha_patient_pay,
           b.days_supply as sha_days_supply,
           case when b.PROVIDER_ID is not null then 1 else 0 end as non_null_sha_provider_id,
           case when b.OUT_OF_POCKET is not null then 1 else 0 end as non_null_sha_out_of_pocket,
           case when b.PATIENT_PAY is not null then 1 else 0 end as non_null_sha_patient_pay,
           case when b.days_supply is not null then 1 else 0 end as non_null_sha_days_supply,
           abs(datediff(day, a.dos, b.dos)) as abs_date_diff
    from tbl_gj_komodo_anchor a
    join tbl_gj_sha_anchor b
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
    and a.code=b.code
    and abs(datediff(day, a.dos, b.dos))<=5
    and a.PAID_FLAG = 1 AND b.paid_flag = 1
),
rank_anchor_linked as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY ABS_DATE_DIFF) AS RN,
           
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_provider_id desc, ABS_DATE_DIFF) AS RN_sha_provider_id,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_out_of_pocket desc, ABS_DATE_DIFF) AS RN_sha_out_of_pocket,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_patient_pay desc, ABS_DATE_DIFF) AS RN_sha_patient_pay,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_days_supply desc, ABS_DATE_DIFF) AS RN_sha_days_supply
    from anchor_linked
)
select * from rank_anchor_linked;


---For sha
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_2_paid as
----------------------------- step 3 -----------------------------------
with anchor_linked as (
    select distinct 
          'Both' as data_source, 
           a.*,
           b.claim_id as komodo_claim_id,
           b.DOS as sha_dos,
           b.PROVIDER_ID as komodo_provider_id,
           b.OUT_OF_POCKET as komodo_out_of_pocket,
           b.PATIENT_PAY as komodo_patient_pay,
           b.days_supply as komodo_days_supply,
           case when b.PROVIDER_ID is not null then 1 else 0 end as non_null_komodo_provider_id,
           case when b.OUT_OF_POCKET is not null then 1 else 0 end as non_null_komodo_out_of_pocket,
           case when b.PATIENT_PAY is not null then 1 else 0 end as non_null_komodo_patient_pay,
           case when b.days_supply is not null then 1 else 0 end as non_null_komodo_days_supply,
           abs(datediff(day, a.dos, b.dos)) as abs_date_diff
    from tbl_gj_sha_anchor a
    join tbl_gj_komodo_anchor b
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
    and a.code=b.code
    and abs(datediff(day, a.dos, b.dos))<=5
    and a.claim_id || a.code not in (select distinct sha_claim_id || code from JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_1)
    and a.PAID_FLAG = 1 AND b.paid_flag = 1
),
rank_anchor_linked as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY ABS_DATE_DIFF) AS RN,
           
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_provider_id desc, ABS_DATE_DIFF) AS RN_komodo_provider_id,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_out_of_pocket desc, ABS_DATE_DIFF) AS RN_komodo_out_of_pocket,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_patient_pay desc, ABS_DATE_DIFF) AS RN_komodo_patient_pay,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_days_supply desc, ABS_DATE_DIFF) AS RN_komodo_days_supply
    from anchor_linked
)
select * from rank_anchor_linked;



-- intermediate table 2
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_consolidated_anchor_1_paid as
----------------------------- step 4 -----------------------------------
with consolidated_anchor as (
    select *
    from tbl_gj_rank_anchor_linked_1_paid
    where RN = 1
)
select * from consolidated_anchor;

SELECT * FROM tbl_gj_consolidated_anchor_1 WHERE CLAIM_ID ='ab99af9b94b1e904fd8a15b043257c37b0a78c9f8995fd78e3ccff0f7bb624be';


create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_consolidated_anchor_2_paid as
----------------------------- step 4 -----------------------------------
with consolidated_anchor as (
    select *
    from tbl_gj_rank_anchor_linked_2_paid
    where RN = 1
)
select * from consolidated_anchor;


-- scenario 1 final table 
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_1_paid as
----------------------------- step 5 -----------------------------------
with augment_consolidated_anchor_1 as (
    select distinct a.*,
            b.sha_PROVIDER_ID as augment_sha_provider_id,
            c.sha_OUT_OF_POCKET as augment_sha_out_of_pocket,
            d.sha_PATIENT_PAY as augment_sha_patient_pay,
           e.sha_days_supply as augment_sha_days_supply,

            coalesce(a.PROVIDER_ID, a.sha_provider_id, b.sha_PROVIDER_ID) as final_provider_id,
            coalesce(a.OUT_OF_POCKET, a.sha_OUT_OF_POCKET, c.sha_OUT_OF_POCKET) as final_OUT_OF_POCKET,
            coalesce(a.PATIENT_PAY, a.sha_PATIENT_PAY, d.sha_PATIENT_PAY) as final_PATIENT_PAY,
           coalesce(a.DAYS_SUPPLY, a.sha_days_supply, e.sha_days_supply) as final_days_supply

    from tbl_gj_consolidated_anchor_1_paid A
     left join (select * from tbl_gj_rank_anchor_linked_1_paid
                 where RN_sha_provider_id=1 and non_null_sha_provider_id=1) B
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
     AND A.CODE = B.CODE 
     AND A.DOS = B.DOS
     left join (select * from tbl_gj_rank_anchor_linked_1_paid 
                 where RN_sha_out_of_pocket=1 and non_null_sha_out_of_pocket=1) C
    on a.patient_token_1 = c.patient_token_1 and a.patient_token_2=c.patient_token_2 
     AND A.CODE = C.CODE 
     AND A.DOS = C.DOS
     left join (select * from tbl_gj_rank_anchor_linked_1_paid 
                 where RN_sha_patient_pay=1 and non_null_sha_patient_pay=1) D
    on a.patient_token_1 = d.patient_token_1 and a.patient_token_2=d.patient_token_2 
     AND A.CODE = D.CODE 
     AND A.DOS = D.DOS
    left join (select * from tbl_gj_rank_anchor_linked_1_paid 
                where RN_sha_days_supply=1 and non_null_sha_days_supply=1) E
    on a.patient_token_1 = e.patient_token_1 and a.patient_token_2=e.patient_token_2 
    AND A.CODE = E.CODE 
    AND A.DOS = E.DOS
)
, augment_consolidated_anchor_2 as (
    select distinct a.*,
            b.komodo_PROVIDER_ID as augment_komodo_provider_id,
            c.komodo_OUT_OF_POCKET as augment_komodo_out_of_pocket,
            d.komodo_PATIENT_PAY as augment_komodo_patient_pay,
           e.komodo_days_supply as augment_komodo_days_supply,

            coalesce(a.PROVIDER_ID, a.komodo_provider_id, b.komodo_PROVIDER_ID) as final_provider_id,
            coalesce(a.OUT_OF_POCKET, a.komodo_OUT_OF_POCKET, c.komodo_OUT_OF_POCKET) as final_OUT_OF_POCKET,
            coalesce(a.PATIENT_PAY, a.komodo_PATIENT_PAY, d.komodo_PATIENT_PAY) as final_PATIENT_PAY,
           coalesce(a.DAYS_SUPPLY, a.komodo_days_supply, e.komodo_days_supply) as final_days_supply

    from tbl_gj_consolidated_anchor_2_paid A
     left join (select * from tbl_gj_rank_anchor_linked_2_paid
                 where RN_komodo_provider_id=1 and non_null_komodo_provider_id=1) B
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
     AND A.CODE = B.CODE 
     AND A.DOS = B.DOS
     left join (select * from tbl_gj_rank_anchor_linked_2_paid 
                 where RN_komodo_out_of_pocket=1 and non_null_komodo_out_of_pocket=1) C
    on a.patient_token_1 = c.patient_token_1 and a.patient_token_2=c.patient_token_2 
     AND A.CODE = C.CODE 
     AND A.DOS = C.DOS
     left join (select * from tbl_gj_rank_anchor_linked_2_paid 
                 where RN_komodo_patient_pay=1 and non_null_komodo_patient_pay=1) D
    on a.patient_token_1 = d.patient_token_1 and a.patient_token_2=d.patient_token_2 
     AND A.CODE = D.CODE 
     AND A.DOS = D.DOS
    left join (select * from tbl_gj_rank_anchor_linked_2_paid 
                where RN_komodo_days_supply=1 and non_null_komodo_days_supply=1) E
    on a.patient_token_1 = e.patient_token_1 and a.patient_token_2=e.patient_token_2 
    AND A.CODE = E.CODE 
    AND A.DOS = E.DOS
)

select * from augment_consolidated_anchor_1
UNION 
select * from augment_consolidated_anchor_2
;


SELECT * FROM JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_1_paid
WHERE CLAIM_ID ='ab99af9b94b1e904fd8a15b043257c37b0a78c9f8995fd78e3ccff0f7bb624be';



-- scenario 3


----------------------------- step 8 -----------------------------------
create or replace table JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid AS

SELECT 
    A.*,
    case when b.days_supply is not null then 1 else 0 end as non_null_sha_days_supply,
    B.CLAIM_ID AS SHA_CLAIM_ID,
    B.dos AS    SHA_DOS,
    B.provider_id AS   SHA_PROVIDER_ID,
    B.OUT_OF_POCKET AS   SHA_OUT_OF_POCKET,
    B.patient_pay AS    SHA_PATIENT_PAY,
    B.days_supply   SHA_DAYS_SUPPLY,
    abs(datediff(day, a.dos, b.dos)) as abs_date_diff
FROM tbl_gj_komodo_non_anchor AS A
INNER JOIN tbl_gj_sha_non_anchor AS B
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
    AND A.CODE = B.CODE
    AND ABS(DATEDIFF(day, A.DOS, B.DOS)) <= 5
    AND A.PROVIDER_ID = B.PROVIDER_ID 
    AND A.OUT_OF_POCKET= B.OUT_OF_POCKET AND A.PATIENT_PAY = B.PATIENT_PAY
    AND A.PAID_FLAG = 1 AND B.PAID_FLAG = 1
;

SELECT DISTINCT CLAIM_ID FROM non_anchor_linked;



-------sc 3 final

create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_3_paid as

With rank_anchor_linked as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY ABS_DATE_DIFF) AS RN,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_days_supply desc, ABS_DATE_DIFF) AS RN_sha_days_supply
    from JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid
)
, top_claims AS(
SELECT DISTINCT * FROM rank_anchor_linked WHERE RN = 1
)
,augment_consolidated_anchor as (
    select distinct a.*,
           coalesce(a.DAYS_SUPPLY, a.sha_days_supply, e.sha_days_supply) as final_days_supply

    from top_claims A
    left join (select * from rank_anchor_linked 
                where RN_sha_days_supply=1 and non_null_sha_days_supply=1) E
    on a.patient_token_1 = e.patient_token_1 and a.patient_token_2=e.patient_token_2 
    AND A.CODE = E.CODE 
    AND A.DOS = E.DOS
)
SELECT DISTINCT * FROM augment_consolidated_anchor;



----------------------------- step 9 -----------------------------------
-- integrated table
create or replace table JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_PAID as

WITH T1 AS(
    select claim_id || code as sc1
    from tbl_gj_rank_anchor_linked_1_paid
    UNION
    select claim_id || code
    from tbl_gj_rank_anchor_linked_2_paid
    UNION
    select sha_claim_id || code
    from tbl_gj_rank_anchor_linked_1_paid
    UNION
    select komodo_claim_id || code
    from tbl_gj_rank_anchor_linked_2_paid
)
, t2 AS(
    select claim_id || code as sc2
    from JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_2
)
, t3 AS(
    select claim_id || code as sc3
    from JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid
    UNION
    SELECT SHA_CLAIM_ID || code
    FROM JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_paid
)
, T AS(
select DISTINCT 
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
union
-----
--S2 (not doing as we wont get any rows from this)
-----
--S3
SELECT DISTINCT 'Both' DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS KOMODO_CLAIM_ID,
        PROVIDER_ID AS KOMODO_PROVIDER_ID,
        DOS AS KOMODO_DOS,
        FINAL_DAYS_SUPPLY AS KOMODO_DAYS_SUPPLY,
        OUT_OF_POCKET AS KOMODO_OUT_OF_POCKET,
        PATIENT_PAY AS KOMODO_PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
FROM tbl_gj_scenario_3_paid
where claim_id || code not in (
    select sc1
    from t1
)

UNION
-----any remaining claims
select 'Komodo' DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS KOMODO_CLAIM_ID,
        PROVIDER_ID AS KOMODO_PROVIDER_ID,
        DOS AS KOMODO_DOS,
        DAYS_SUPPLY AS KOMODO_DAYS_SUPPLY,
        OUT_OF_POCKET AS KOMODO_OUT_OF_POCKET,
        PATIENT_PAY AS KOMODO_PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_komodo_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
union
select 'Symphony' as DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS CLAIM_ID,
        PROVIDER_ID AS PROVIDER_ID,
        DOS AS DOS,
        DAYS_SUPPLY AS DAYS_SUPPLY,
        OUT_OF_POCKET AS OUT_OF_POCKET,
        PATIENT_PAY AS PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_sha_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
union
select 'Komodo' DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS KOMODO_CLAIM_ID,
        PROVIDER_ID AS KOMODO_PROVIDER_ID,
        DOS AS KOMODO_DOS,
        DAYS_SUPPLY AS KOMODO_DAYS_SUPPLY,
        OUT_OF_POCKET AS KOMODO_OUT_OF_POCKET,
        PATIENT_PAY AS KOMODO_PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_komodo_non_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
union
select 'Symphony' as DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS CLAIM_ID,
        PROVIDER_ID AS PROVIDER_ID,
        DOS AS DOS,
        DAYS_SUPPLY AS DAYS_SUPPLY,
        OUT_OF_POCKET AS OUT_OF_POCKET,
        PATIENT_PAY AS PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_sha_non_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
)
SELECT DISTINCT * FROM T
MINUS
SELECT DISTINCT * FROM T
WHERE DATA_SOURCE = 'Symphony' AND PATIENT_TOKEN_1 IS NULL AND PATIENT_TOKEN_2 IS NULL AND PATIENT_ID IS NULL
;





--------------------------------------------------------------
-- Scenario 1
--------------------------------------------------------------
-- intermediate table 1
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_1_non_paid as
----------------------------- step 3 -----------------------------------
with anchor_linked as (
    select distinct 
          'Both' as data_source, 
           a.*,
           b.claim_id as sha_claim_id,
           b.DOS as sha_dos,
           b.PROVIDER_ID as sha_provider_id,
           b.OUT_OF_POCKET as sha_out_of_pocket,
           b.PATIENT_PAY as sha_patient_pay,
           b.days_supply as sha_days_supply,
           case when b.PROVIDER_ID is not null then 1 else 0 end as non_null_sha_provider_id,
           case when b.OUT_OF_POCKET is not null then 1 else 0 end as non_null_sha_out_of_pocket,
           case when b.PATIENT_PAY is not null then 1 else 0 end as non_null_sha_patient_pay,
           case when b.days_supply is not null then 1 else 0 end as non_null_sha_days_supply,
           abs(datediff(day, a.dos, b.dos)) as abs_date_diff
    from tbl_gj_komodo_anchor a
    join tbl_gj_sha_anchor b
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
    and a.code=b.code
    and abs(datediff(day, a.dos, b.dos))<=5
    AND A.PAID_FLAG = 0 AND B.PAID_FLAG = 0
),
rank_anchor_linked as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY ABS_DATE_DIFF) AS RN,
           
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_provider_id desc, ABS_DATE_DIFF) AS RN_sha_provider_id,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_out_of_pocket desc, ABS_DATE_DIFF) AS RN_sha_out_of_pocket,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_patient_pay desc, ABS_DATE_DIFF) AS RN_sha_patient_pay,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_days_supply desc, ABS_DATE_DIFF) AS RN_sha_days_supply
    from anchor_linked
)
select * from rank_anchor_linked;

---For sha
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_2_non_paid as
----------------------------- step 3 -----------------------------------
with anchor_linked as (
    select distinct 
          'Both' as data_source, 
           a.*,
           b.claim_id as komodo_claim_id,
           b.DOS as sha_dos,
           b.PROVIDER_ID as komodo_provider_id,
           b.OUT_OF_POCKET as komodo_out_of_pocket,
           b.PATIENT_PAY as komodo_patient_pay,
           b.days_supply as komodo_days_supply,
           case when b.PROVIDER_ID is not null then 1 else 0 end as non_null_komodo_provider_id,
           case when b.OUT_OF_POCKET is not null then 1 else 0 end as non_null_komodo_out_of_pocket,
           case when b.PATIENT_PAY is not null then 1 else 0 end as non_null_komodo_patient_pay,
           case when b.days_supply is not null then 1 else 0 end as non_null_komodo_days_supply,
           abs(datediff(day, a.dos, b.dos)) as abs_date_diff
    from tbl_gj_sha_anchor a
    join tbl_gj_komodo_anchor b
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
    and a.code=b.code
    and abs(datediff(day, a.dos, b.dos))<=5
    and a.claim_id || a.code not in (select distinct sha_claim_id || code from JNJ_CDW_DEV.CDW_CURATED.tbl_gj_rank_anchor_linked_1)
    and a.paid_flag = 0 and b.paid_flag = 0
),
rank_anchor_linked as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY ABS_DATE_DIFF) AS RN,
           
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_provider_id desc, ABS_DATE_DIFF) AS RN_komodo_provider_id,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_out_of_pocket desc, ABS_DATE_DIFF) AS RN_komodo_out_of_pocket,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_patient_pay desc, ABS_DATE_DIFF) AS RN_komodo_patient_pay,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_komodo_days_supply desc, ABS_DATE_DIFF) AS RN_komodo_days_supply
    from anchor_linked
)
select * from rank_anchor_linked;



-- intermediate table 2
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_consolidated_anchor_1_non_paid as
----------------------------- step 4 -----------------------------------
with consolidated_anchor as (
    select *
    from tbl_gj_rank_anchor_linked_1_non_paid
    where RN = 1
)
select * from consolidated_anchor;



create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_consolidated_anchor_2_non_paid as
----------------------------- step 4 -----------------------------------
with consolidated_anchor as (
    select *
    from tbl_gj_rank_anchor_linked_2_non_paid
    where RN = 1
)
select * from consolidated_anchor;


-- scenario 1 final table 
create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_1_non_paid as
----------------------------- step 5 -----------------------------------
with augment_consolidated_anchor_1 as (
    select distinct a.*,
            b.sha_PROVIDER_ID as augment_sha_provider_id,
            c.sha_OUT_OF_POCKET as augment_sha_out_of_pocket,
            d.sha_PATIENT_PAY as augment_sha_patient_pay,
           e.sha_days_supply as augment_sha_days_supply,

            coalesce(a.PROVIDER_ID, a.sha_provider_id, b.sha_PROVIDER_ID) as final_provider_id,
            coalesce(a.OUT_OF_POCKET, a.sha_OUT_OF_POCKET, c.sha_OUT_OF_POCKET) as final_OUT_OF_POCKET,
            coalesce(a.PATIENT_PAY, a.sha_PATIENT_PAY, d.sha_PATIENT_PAY) as final_PATIENT_PAY,
           coalesce(a.DAYS_SUPPLY, a.sha_days_supply, e.sha_days_supply) as final_days_supply

    from tbl_gj_consolidated_anchor_1_non_paid A
     left join (select * from tbl_gj_rank_anchor_linked_1_non_paid
                 where RN_sha_provider_id=1 and non_null_sha_provider_id=1) B
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
     AND A.CODE = B.CODE 
     AND A.DOS = B.DOS
     left join (select * from tbl_gj_rank_anchor_linked_1_non_paid 
                 where RN_sha_out_of_pocket=1 and non_null_sha_out_of_pocket=1) C
    on a.patient_token_1 = c.patient_token_1 and a.patient_token_2=c.patient_token_2 
     AND A.CODE = C.CODE 
     AND A.DOS = C.DOS
     left join (select * from tbl_gj_rank_anchor_linked_1_non_paid 
                 where RN_sha_patient_pay=1 and non_null_sha_patient_pay=1) D
    on a.patient_token_1 = d.patient_token_1 and a.patient_token_2=d.patient_token_2 
     AND A.CODE = D.CODE 
     AND A.DOS = D.DOS
    left join (select * from tbl_gj_rank_anchor_linked_1_non_paid 
                where RN_sha_days_supply=1 and non_null_sha_days_supply=1) E
    on a.patient_token_1 = e.patient_token_1 and a.patient_token_2=e.patient_token_2 
    AND A.CODE = E.CODE 
    AND A.DOS = E.DOS
)
, augment_consolidated_anchor_2 as (
    select distinct a.*,
            b.komodo_PROVIDER_ID as augment_komodo_provider_id,
            c.komodo_OUT_OF_POCKET as augment_komodo_out_of_pocket,
            d.komodo_PATIENT_PAY as augment_komodo_patient_pay,
           e.komodo_days_supply as augment_komodo_days_supply,

            coalesce(a.PROVIDER_ID, a.komodo_provider_id, b.komodo_PROVIDER_ID) as final_provider_id,
            coalesce(a.OUT_OF_POCKET, a.komodo_OUT_OF_POCKET, c.komodo_OUT_OF_POCKET) as final_OUT_OF_POCKET,
            coalesce(a.PATIENT_PAY, a.komodo_PATIENT_PAY, d.komodo_PATIENT_PAY) as final_PATIENT_PAY,
           coalesce(a.DAYS_SUPPLY, a.komodo_days_supply, e.komodo_days_supply) as final_days_supply

    from tbl_gj_consolidated_anchor_2_non_paid A
     left join (select * from tbl_gj_rank_anchor_linked_2_non_paid
                 where RN_komodo_provider_id=1 and non_null_komodo_provider_id=1) B
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
     AND A.CODE = B.CODE 
     AND A.DOS = B.DOS
     left join (select * from tbl_gj_rank_anchor_linked_2_non_paid 
                 where RN_komodo_out_of_pocket=1 and non_null_komodo_out_of_pocket=1) C
    on a.patient_token_1 = c.patient_token_1 and a.patient_token_2=c.patient_token_2 
     AND A.CODE = C.CODE 
     AND A.DOS = C.DOS
     left join (select * from tbl_gj_rank_anchor_linked_2_non_paid 
                 where RN_komodo_patient_pay=1 and non_null_komodo_patient_pay=1) D
    on a.patient_token_1 = d.patient_token_1 and a.patient_token_2=d.patient_token_2 
     AND A.CODE = D.CODE 
     AND A.DOS = D.DOS
    left join (select * from tbl_gj_rank_anchor_linked_2_non_paid 
                where RN_komodo_days_supply=1 and non_null_komodo_days_supply=1) E
    on a.patient_token_1 = e.patient_token_1 and a.patient_token_2=e.patient_token_2 
    AND A.CODE = E.CODE 
    AND A.DOS = E.DOS
)

select * from augment_consolidated_anchor_1
UNION 
select * from augment_consolidated_anchor_2
;




-- scenario 3


----------------------------- step 8 -----------------------------------
create or replace table JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_non_paid AS

SELECT 
    A.*,
    case when b.days_supply is not null then 1 else 0 end as non_null_sha_days_supply,
    B.CLAIM_ID AS SHA_CLAIM_ID,
    B.dos AS    SHA_DOS,
    B.provider_id AS   SHA_PROVIDER_ID,
    B.OUT_OF_POCKET AS   SHA_OUT_OF_POCKET,
    B.patient_pay AS    SHA_PATIENT_PAY,
    B.days_supply   SHA_DAYS_SUPPLY,
    abs(datediff(day, a.dos, b.dos)) as abs_date_diff
FROM tbl_gj_komodo_non_anchor AS A
INNER JOIN tbl_gj_sha_non_anchor AS B
    on a.patient_token_1 = b.patient_token_1 and a.patient_token_2=b.patient_token_2 
    AND A.CODE = B.CODE
    AND ABS(DATEDIFF(day, A.DOS, B.DOS)) <= 5
    AND A.PROVIDER_ID = B.PROVIDER_ID 
    AND A.OUT_OF_POCKET= B.OUT_OF_POCKET AND A.PATIENT_PAY = B.PATIENT_PAY
    AND A.PAID_FLAG = 0 AND B.PAID_FLAG = 0
;

SELECT DISTINCT CLAIM_ID FROM non_anchor_linked;



-------sc 3 final

create or replace table JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_3_non_paid as

With rank_anchor_linked as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY ABS_DATE_DIFF) AS RN,
           ROW_NUMBER() OVER (PARTITION BY PATIENT_TOKEN_2, CODE, DOS ORDER BY non_null_sha_days_supply desc, ABS_DATE_DIFF) AS RN_sha_days_supply
    from JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked_non_paid
)
, top_claims AS(
SELECT DISTINCT * FROM rank_anchor_linked WHERE RN = 1
)
,augment_consolidated_anchor as (
    select distinct a.*,
           coalesce(a.DAYS_SUPPLY, a.sha_days_supply, e.sha_days_supply) as final_days_supply

    from top_claims A
    left join (select * from rank_anchor_linked 
                where RN_sha_days_supply=1 and non_null_sha_days_supply=1) E
    on a.patient_token_1 = e.patient_token_1 and a.patient_token_2=e.patient_token_2 
    AND A.CODE = E.CODE 
    AND A.DOS = E.DOS
)
SELECT DISTINCT * FROM augment_consolidated_anchor;


SELECT DISTINCT CLAIM_ID FROM tbl_gj_scenario_3 
WHERE CLAIM_ID ='ab99af9b94b1e904fd8a15b043257c37b0a78c9f8995fd78e3ccff0f7bb624be';

----------------------------- step 9 -----------------------------------
-- integrated table
create or replace table JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_NON_PAID as

--create temporary table JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_FINAL_JUNE as


WITH T1 AS(
    select claim_id || code as sc1
    from tbl_gj_rank_anchor_linked_1
    UNION
    select claim_id || code
    from tbl_gj_rank_anchor_linked_2
    UNION
    select sha_claim_id || code
    from tbl_gj_rank_anchor_linked_1
    UNION
    select komodo_claim_id || code
    from tbl_gj_rank_anchor_linked_2
)
, t2 AS(
    select claim_id || code as sc2
    from JNJ_CDW_DEV.CDW_CURATED.tbl_gj_scenario_2
)
, t3 AS(
    select claim_id || code as sc3
    from JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked
    UNION
    SELECT SHA_CLAIM_ID || code
    FROM JNJ_CDW_DEV.CDW_CURATED.non_anchor_linked
)
, T AS(
select DISTINCT 
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
FROM tbl_gj_scenario_1
union
-----
--S2 (not doing as we wont get any rows from this)
-----
--S3
SELECT DISTINCT 'Both' DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS KOMODO_CLAIM_ID,
        PROVIDER_ID AS KOMODO_PROVIDER_ID,
        DOS AS KOMODO_DOS,
        FINAL_DAYS_SUPPLY AS KOMODO_DAYS_SUPPLY,
        OUT_OF_POCKET AS KOMODO_OUT_OF_POCKET,
        PATIENT_PAY AS KOMODO_PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
FROM tbl_gj_scenario_3
where claim_id || code not in (
    select sc1
    from t1
)

UNION
-----any remaining claims
select 'Komodo' DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS KOMODO_CLAIM_ID,
        PROVIDER_ID AS KOMODO_PROVIDER_ID,
        DOS AS KOMODO_DOS,
        DAYS_SUPPLY AS KOMODO_DAYS_SUPPLY,
        OUT_OF_POCKET AS KOMODO_OUT_OF_POCKET,
        PATIENT_PAY AS KOMODO_PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_komodo_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
union
select 'Symphony' as DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS CLAIM_ID,
        PROVIDER_ID AS PROVIDER_ID,
        DOS AS DOS,
        DAYS_SUPPLY AS DAYS_SUPPLY,
        OUT_OF_POCKET AS OUT_OF_POCKET,
        PATIENT_PAY AS PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_sha_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
union
select 'Komodo' DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS KOMODO_CLAIM_ID,
        PROVIDER_ID AS KOMODO_PROVIDER_ID,
        DOS AS KOMODO_DOS,
        DAYS_SUPPLY AS KOMODO_DAYS_SUPPLY,
        OUT_OF_POCKET AS KOMODO_OUT_OF_POCKET,
        PATIENT_PAY AS KOMODO_PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_komodo_non_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
union
select 'Symphony' as DATA_SOURCE,
        DATASOURCE,
        SOURCE,
        PATIENT_TOKEN_1,
        PATIENT_TOKEN_2,
        PATIENT_ID,
        claim_id AS CLAIM_ID,
        PROVIDER_ID AS PROVIDER_ID,
        DOS AS DOS,
        DAYS_SUPPLY AS DAYS_SUPPLY,
        OUT_OF_POCKET AS OUT_OF_POCKET,
        PATIENT_PAY AS PATIENT_PAY,
        CODE,
        TRINITY_GROUP1,
        TRINITY_GROUP2,
        TRINITY_GROUP3,
        TRINITY_GROUP4,
        TREATMENT_CLAIM_STATUS,
        TREATMENT_FINAL_CLAIM_INDICATOR,
        PAID_FLAG
from tbl_gj_sha_non_anchor
where claim_id || code not in (
    select sc1
    from t1
    UNION
    select sc3
    from t3
)
)
SELECT DISTINCT * FROM T
MINUS
SELECT DISTINCT * FROM T
WHERE DATA_SOURCE = 'Symphony' AND PATIENT_TOKEN_1 IS NULL AND PATIENT_TOKEN_2 IS NULL AND PATIENT_ID IS NULL
;




-----FINAL 

create or replace table JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_FINAL_TEMP as
SELECT * FROM JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_PAID
UNION 
SELECT * FROM JNJ_CDW_DEV.CDW_CURATED.TBL_TREATMENT_INTEGRATED_NON_PAID
;
