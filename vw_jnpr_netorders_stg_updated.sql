--------------------------------------------------------------
CREATE OR REPLACE VIEW bu_nnbu.sch_bu_nnbu.vw_jnpr_netorders_stg AS

WITH

-- =============================================================================
-- CTE 1: SOURCE
-- =============================================================================
src AS (
    SELECT *
    FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_landing
    WHERE CAST(updt_dtm AS DATE) = '2026-03-05'
    -- Dynamic alternative (uncomment when control table is ready):
    -- WHERE updt_dtm = (
    --     SELECT last_processed_timestamp
    --     FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_source_control_table
    --     WHERE table_name = 'vw_jnpr_netorders_stg'
    -- )
),

-- =============================================================================
-- CTE 2: BUSINESS AREA DIMENSION
-- Table   : nnbu.sch_nnbu_rptng.vw_nnbu_business_area_d
-- Columns : business_area_cd, business_name, cp_business_level00,
--           cp_business_level1, cp_business_level2, cp_offering_category
-- Join    : LEFT(business_area_cd, 2) = HPE_PRODUCT_LINE
-- Exclude : business_name IN ('Fin_PnL','Other')
-- =============================================================================
biz_area AS (
    SELECT
        LEFT(business_area_cd, 2)   AS pl,
        business_area_cd            AS business_area_code,
        cp_business_level00         AS cp_business_level_00,
        cp_business_level1          AS cp_business_level_1,
        cp_business_level2          AS cp_business_level_2,
        cp_offering_category        AS cp_offering_category
    FROM nnbu.sch_nnbu_rptng.vw_nnbu_business_area_d
    WHERE business_name NOT IN ('Fin_PnL', 'Other')
),

-- =============================================================================
-- CTE 3: PROFIT CENTER HIERARCHY
-- Table   : nnbu.sch_nnbu_rptng.vw_ea_fin_vrt_pft_cntr_std_hrchy
-- Columns : pft_cntr_ky, pch_level_1..8 + desc, prd_ln,
--           ins_ts, ins_dtm, updt_dtm, source_file_name
-- Join    : pft_cntr_ky = src.PROFIT_CENTER
-- Note    : No direct profit_center_cd column — joining on pft_cntr_ky
--           Using prd_ln as the product line / profit center reference
-- =============================================================================
pft_cntr AS (
    SELECT
        pft_cntr_ky,
        prd_ln,
        pch_level_1,        pch_level_1_desc,
        pch_level_2,        pch_level_2_desc,
        pch_level_3,        pch_level_3_desc,
        pch_level_4,        pch_level_4_desc,
        pch_level_5,        pch_level_5_desc,
        pch_level_6,        pch_level_6_desc,
        pch_level_7,        pch_level_7_desc,
        pch_level_8,        pch_level_8_desc
    FROM nnbu.sch_nnbu_rptng.vw_ea_fin_vrt_pft_cntr_std_hrchy
),

-- =============================================================================
-- CTE 4: CALENDAR DIMENSION
-- Table   : nnbu.sch_nnbu_rptng.vw_nnbu_clndr_dmnsn
-- Columns used:
--   cldr_trns_dt          → join key (calendar date)
--   nnbu_fscl_yr_qtr_nr   → fiscal quarter  (e.g. 2026Q1)
--   fisc_yr_nm            → reporting fiscal year (e.g. FY2026)
--   nnbu_week_nr          → reporting week number
-- Joined 3x aliased: clndr_create, clndr_cancel, clndr_fin
-- =============================================================================
clndr AS (
    SELECT
        cldr_trns_dt,
        nnbu_fscl_yr_qtr_nr     AS fiscal_quarter,
        fisc_yr_nm              AS reporting_fiscal_year,
        nnbu_week_nr            AS reporting_week_number
    FROM nnbu.sch_nnbu_rptng.vw_nnbu_clndr_dmnsn
),

-- =============================================================================
-- CTE 5: ACCOUNT PARTY DIMENSION
-- Table   : nnbu.sch_nnbu_rptng.vw_ea_common_account_party_current_start_dmnsn
-- Columns used:
--   party_id                          → join key
--   party_name                        → party name
--   party_country_code                → country code
--   country_entity_id                 → country entity id
--   country_entity_name               → country entity name
--   industry_vertical_segment_name    → market segment
--   country_entity_region_name        → aruba coverage tier (closest available)
--   global_entity_id                  → global entity id
--   global_entity_name                → global entity name
--   global_entity_region_name         → global entity market segment
-- Joined 6x aliased: sold_to, ship_to, end_cust, reseller, bill_to, payer
-- =============================================================================
acct_party AS (
    SELECT
        party_id,
        party_name,
        party_country_code                  AS country_code,
        country_entity_id,
        country_entity_name,
        industry_vertical_segment_name      AS market_segment,
        country_entity_region_name          AS aruba_coverage_tier_name,
        global_entity_id,
        global_entity_name,
        global_entity_region_name           AS global_entity_market_segment,
        prty_typ_nm                         AS reseller_code
    FROM nnbu.sch_nnbu_rptng.vw_ea_common_account_party_current_start_dmnsn
),

-- =============================================================================
-- CTE 6: PARTNER XREF
-- Table   : nnbu.sch_nnbu_rptng.vw_nnbu_hpen_partner_xref
-- Columns : juniper_sap_id, party_id, partner_name, partner_type,
--           create_dt, ins_dtm, updt_dtm, source_file_name
-- Join    : juniper_sap_id = src.DISTRIBUTOR / src.RESELLER
-- =============================================================================
partner_xref AS (
    SELECT
        juniper_sap_id,
        party_id            AS hpe_party_id,
        partner_name,
        partner_type
    FROM nnbu.sch_nnbu_rptng.vw_nnbu_hpen_partner_xref
),

-- =============================================================================
-- CTE 7: RESOLVED RESELLER PARTY ID
-- Fix: Databricks does not support correlated scalar subqueries in JOIN conditions.
-- Solution: Pre-resolve each src.RESELLER → HPE party_id here in a CTE,
--           then JOIN the result in the main query on a simple equality.
-- Logic: Use xref HPE party_id if match found, else fall back to HPE_RESELLER_PARTY_ID
-- =============================================================================
src_reseller_resolved AS (
    SELECT
        s.SALES_ORDER_NO,
        s.SALES_ORDER_LINE,
        COALESCE(px.hpe_party_id, s.HPE_RESELLER_PARTY_ID) AS resolved_reseller_party_id
    FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_landing s
    LEFT JOIN partner_xref px
        ON px.juniper_sap_id = s.RESELLER
    WHERE CAST(s.updt_dtm AS DATE) = '2026-03-05'
)

-- =============================================================================
-- MAIN SELECT
-- =============================================================================
SELECT

    -- =========================================================
    -- DEFAULT VALUES
    -- =========================================================
    '500'                                           AS client_id,
    5677                                            AS source_system_ky,
    'JUNIPER'                                       AS source_system_descr,
    'Juniper Orders'                                AS source_type_descr,
    'JUNIPER'                                       AS source_system_code,
    'Order-Adj'                                     AS internal_process_code,

    -- =========================================================
    -- DIRECT MAPPINGS - ORDER HEADER
    -- =========================================================
    src.SALES_ORDER_NO                              AS order_number,
    src.SALES_ORDER_LINE                            AS order_line_item,
    src.HIGHER_LEVEL_LINE_ITEM                      AS higher_level_line_item,
    src.ORDER_TYPE                                  AS order_type_code,
    src.ORDER_TYPE_DESC                             AS order_type_descr,
    src.HEADER_BOOKED_DATE                          AS order_create_date,
    src.LINE_BOOKED_DATE                            AS order_item_create_date,
    src.SALES_ORGANIZATION                          AS sales_organization_code,
    src.SALES_ORGANIZATION_NAME                     AS sales_organization_descr,

    -- =========================================================
    -- DIRECT MAPPINGS - DATES
    -- =========================================================
    src.CALCULATED_REQUESTED_SHIP_DATE              AS calculated_requested_ship_date,
    src.CUSTOMER_REQUESTED_DELIVERY_DATE            AS customer_requested_delivery_date,
    src.REQUESTED_DELIVERY_DATE                     AS requested_delivery_date,
    src.COMMITTED_SHIP_DATE                         AS committed_ship_date,
    src.COMMITTED_DELIVERY_DATE                     AS committed_delivery_date,

    -- =========================================================
    -- DIRECT MAPPINGS - ORDER ATTRIBUTES
    -- =========================================================
    src.CUSTOMER_PO_NO_HEADER                       AS customer_purchase_order_id,
    src.INCOTERMS_1                                 AS incoterms_1_code,
    src.INCOTERMS_2                                 AS incoterms_2_code,
    src.PAYMENT_TERMS_NAME                          AS payment_terms_descr,
    src.PAYMENT_TERMS_ID                            AS payment_terms_code,
    src.CUSTOMER_GROUP_DESC                         AS business_relationship_type_descr,
    src.CUSTOMER_GROUP                              AS business_relationship_type_cd,
    src.ORDER_CURRENCY                              AS header_document_currency_code,
    src.ORDER_CURRENCY                              AS item_document_currency_code,
    src.EXCHANGE_RATE                               AS exchange_rate,
    src.EXCHANGE_RATE_TYPE                          AS exchange_rate_type_code,
    src.COMPLETE_DELIVERY                           AS complete_delivery_group_flag,
    src.ORDER_REASON                                AS order_reason_code,
    src.ORDER_REASON_TEXT                           AS order_reason_descr,
    src.REASON_FOR_REJECTION                        AS rejection_reason_code,
    src.REASON_FOR_REJECTION_DESC                   AS rejection_reason_descr,
    src.HEADER_DELIVERY_BLOCK                       AS header_delivery_block_code,
    src.HEADER_DELIVERY_BLOCK_TEXT                  AS header_delivery_block_descr,
    src.LINE_DELIVERY_BLOCK                         AS item_delivery_block_code,
    src.LINE_DELIVERY_BLOCK_TEXT                    AS item_delivery_block_descr,
    src.CUSTOMER_PO_RECEIPT_DATE                    AS hpe_received_date,
    src.QUOTE_ID                                    AS quote_id,
    src.QUOTE_LINE                                  AS quote_line_item,
    src.DEAL_NUMBER                                 AS deal_number,
    src.DEAL_NAME                                   AS deal_name,
    src.OPPORTUNITY_NUMBER                          AS opportunity_id,
    src.OPPORTUNITY_NAME                            AS opportunity_name,
    src.HOLD_FLAG                                   AS hold_flag,
    src.CHANGED_ON                                  AS order_item_change_date,

    -- =========================================================
    -- DERIVED - DROP SHIP CODE
    -- =========================================================
    CASE WHEN src.PURCHASE_ORDER_TYPE = 'DRSP' THEN 'DROP'
         ELSE src.PURCHASE_ORDER_TYPE
    END                                             AS drop_ship_code,

    -- =========================================================
    -- DERIVED - ROUTE TO MARKET (UPPER case)
    -- =========================================================
    UPPER(src.DISTRIBUTION_CHANNEL_TEXT)            AS route_to_market,
    src.DISTRIBUTION_CHANNEL                        AS route_to_market_id,

    -- =========================================================
    -- DIRECT MAPPINGS - INCOMPLETION / PROCESSING STATUS
    -- =========================================================
    src.SOH_ALL_LINES_INCOMPLETION_STATUS           AS header_all_lines_incompletion_status_code,
    src.SOH_INCOMPLETION_STATUS                     AS header_incompletion_status_code,
    src.SOL_GENERAL_INCOMPLETION_STATUS             AS item_incompletion_status_code,
    src.SOH_OVERALL_PROCESSING_STATUS               AS header_overall_processing_status_code,
    src.SOL_OVERALL_PROCESSING_STATUS               AS item_overall_processing_status_code,

    -- =========================================================
    -- NULL - ORDER ITEM CHANGE TYPE (Derive TBD: Cancelled/Rebook/Other)
    -- =========================================================
    CAST(NULL AS STRING)                                            AS order_item_change_type,

    -- =========================================================
    -- HPE PARTY IDs + NAME / COUNTRY
    -- Table: vw_ea_common_account_party_current_start_dmnsn
    -- Join key: party_id
    -- =========================================================
    src.HPE_SOLD_TO_PARTY_ID                        AS sold_to_party_id,
    sold_to.party_name                              AS sold_to_party_name,
    sold_to.country_code                            AS sold_to_country_code,

    src.HPE_SHIP_TO_PARTY_ID                        AS ship_to_prty_id,
    ship_to.party_name                              AS ship_to_party_name,
    ship_to.country_code                            AS ship_to_country_code,

    src.HPE_END_CUSTOMER_PARTY_ID                   AS end_customer_party_id,
    end_cust.party_name                             AS end_customer_party_name,
    end_cust.country_code                           AS end_customer_country_code,
    end_cust.country_entity_id                      AS end_customer_country_entity_id,
    end_cust.country_entity_name                    AS end_customer_country_entity_name,
    end_cust.market_segment                         AS end_customer_country_entity_market_segment,
    end_cust.aruba_coverage_tier_name               AS end_customer_country_entity_aruba_coverage_tier_name,
    end_cust.global_entity_id                       AS end_customer_global_entity_id,
    end_cust.global_entity_name                     AS end_customer_global_entity_name,
    end_cust.global_entity_market_segment           AS end_customer_global_entity_market_segment,

    -- Distributor: Juniper DISTRIBUTOR → HPE party_id via partner_xref (juniper_sap_id)
    COALESCE(dist_xref.hpe_party_id, src.DISTRIBUTOR) AS distributor_party_id,
    src.DISTRIBUTOR_NAME                            AS distributor_party_name,
    src.DISTRIBUTOR_COUNTRY                         AS distributor_party_country_code,

    -- Reseller: resolved via partner_xref (juniper_sap_id) first, else CDO HPE_RESELLER_PARTY_ID
    COALESCE(res_xref.hpe_party_id, src.HPE_RESELLER_PARTY_ID) AS reseller_party_id,
    COALESCE(reseller_xref.party_name,    reseller_hpe.party_name)    AS reseller_party_name,
    COALESCE(reseller_xref.reseller_code, reseller_hpe.reseller_code) AS reseller_party_code,

    src.HPE_BILL_TO_PARTY_ID                        AS bill_to_party_id,
    bill_to.party_name                              AS bill_to_party_name,
    bill_to.country_code                            AS bill_to_country_code,

    src.HPE_PAYER_PARTY_ID                          AS payer_party_id,
    payer.party_name                                AS payer_party_name,
    payer.country_code                              AS payer_country_code,

    -- =========================================================
    -- DIRECT MAPPINGS - PRODUCT / LINE ITEM
    -- =========================================================
    src.UNIT_OF_MEASURE                             AS base_unit_of_measure,
    src.ORDER_LINE_CATEGORY                         AS item_category_code,
    src.ORDER_LINE_CATEGORY_DESC                    AS item_category_descr,
    src.PLANT                                       AS plant_code,
    src.PLANT_DESC                                  AS plant_descr,
    src.DELIVERY_PRIORITY                           AS delivery_priority_code,
    src.DELIVERY_PRIORITY_DESC                      AS delivery_priority_descr,
    src.SCHEDULE_LINE_DATE                          AS lead_time_delivery_date,

    -- =========================================================
    -- BUSINESS HIERARCHY
    -- Table : vw_nnbu_business_area_d → join on LEFT(business_area_cd,2) = HPE_PRODUCT_LINE
    -- Table : vw_ea_fin_vrt_pft_cntr_std_hrchy → join on pft_cntr_ky = PROFIT_CENTER
    -- NOTE  : profit_center_code mapped to pft_cntr_ky (no separate profit_center_cd col)
    -- =========================================================
    pft_cntr.pft_cntr_ky                            AS profit_center_code,
    biz_area.business_area_code                     AS business_area_code,
    biz_area.cp_business_level_00                   AS cp_business_level_00,
    biz_area.cp_business_level_1                    AS cp_business_level_1,
    biz_area.cp_business_level_2                    AS cp_business_level_2,
    biz_area.cp_offering_category                   AS cp_offering_category,

    -- =========================================================
    -- DIRECT MAPPINGS - SCHEDULE LINE
    -- =========================================================
    src.ACCOUNT_ASSIGNMENT_GROUP                    AS account_assignment_group_code,
    src.SCHEDULE_LINE_CATEGORY                      AS schedule_line_category_code,
    src.SCHEDULE_LINE_CATEGORY_DESC                 AS schedule_line_category_descr,
    src.SCHEDULE_LINE_DATE                          AS schedule_line_date,
    src.SCHEDULE_LINE_NO                            AS schedule_line_number,

    -- =========================================================
    -- DIRECT MAPPINGS - PRODUCT
    -- =========================================================
    src.HPE_PRODUCT_LINE                            AS product_line,
    src.PRODUCT_NUMBER_DESCRIPTION                  AS product_descr,
    CAST(NULL AS STRING)                                            AS base_product_id,
    src.PRODUCT_NUMBER                              AS product_id,
    src.PARENT_FLAG                                 AS parent_product_flag,
    src.PARENT_PRODUCT_NO_LONG                      AS parent_product_long_id,
    src.PARENT_PRODUCT_NO                           AS parent_product_id,

    -- =========================================================
    -- DIRECT MAPPINGS - QUANTITY
    -- =========================================================
    src.ORDER_QUANTITY                              AS order_base_quantity,
    0                                               AS order_option_quantity,
    src.ORDER_QUANTITY                              AS order_quantity,

    -- =========================================================
    -- DIRECT MAPPINGS - BILLING BLOCK
    -- =========================================================
    src.HEADER_BILLING_BLOCK                        AS header_billing_block_code,
    src.HEADER_BILLING_BLOCK_TEXT                   AS header_billing_block_descr,
    src.LINE_BILLING_BLOCK                          AS item_billing_block_code,
    src.LINE_BILLING_BLOCK_TEXT                     AS item_billing_block_descr,

    -- =========================================================
    -- DIRECT MAPPINGS - PRICING / AMOUNTS
    -- =========================================================
    src.PRICE_DATE                                  AS pricing_date,
    src.LIST_VALUE                                  AS gross_dmct_amount,
    src.LIST_VALUE_USD                              AS gross_usd_amount,
    src.NET_VALUE_DOC_CURRENCY                      AS net_value_dcmt_amount,
    src.NET_VALUE_USD                               AS net_value_usd_amount,
    (src.LIST_VALUE_USD - src.NET_VALUE_USD)        AS discount_usd_amount,

    -- =========================================================
    -- NULL - SEGMENT CODE + GEO DIMENSION
    -- Pending: Geo Hierarchy BMT table name not yet confirmed
    -- =========================================================
    CAST(NULL AS STRING)                                            AS segment_code,
    CAST(NULL AS STRING)                                            AS region_name,
    CAST(NULL AS STRING)                                            AS sales_geo,
    CAST(NULL AS STRING)                                            AS sales_geo_country_code,
    CAST(NULL AS STRING)                                            AS sales_geo_group_name,
    CAST(NULL AS STRING)                                            AS sales_geo_sub_group_name,

    -- =========================================================
    -- DIRECT MAPPINGS - MATERIAL / COGS
    -- =========================================================
    src.MATERIAL_AVAILABILITY_DATE                  AS material_availability_date,
    src.MATERIAL_GROUP                              AS material_group_code,
    src.COGS                                        AS enterprise_standard_cost_dcmt_amt,
    src.COGS_USD                                    AS enterprise_standard_cost_usd_amt,

    -- NULL - TAA REQUIRED FLAG (TBD)
    CAST(NULL AS STRING)                                            AS taa_required_flag,

    -- =========================================================
    -- DERIVED - RETURN FLAG
    -- =========================================================
    CASE WHEN src.ORDER_TYPE IN ('ZSR','ZRE') THEN 'Y'
         ELSE 'N'
    END                                             AS return_flag,

    -- =========================================================
    -- DERIVED - FINANCIAL CLOSE DATE
    -- Logic: UPDT_DTM = date update received = financial close date (per David)
    -- =========================================================
    CAST(src.UPDT_DTM AS DATE)                      AS financial_close_date,

    -- DEFAULT - GREENLAKE FLAG
    'N'                                             AS greenlake_flag,

    -- NULL - USAGE NAME (TBD)
    CAST(NULL AS STRING)                                            AS usage_name,

    -- =========================================================
    -- DERIVED - ORDER CANCELLED FLAG
    -- =========================================================
    CASE WHEN COALESCE(src.REASON_FOR_REJECTION,'') <> '' THEN 'Y'
         ELSE 'N'
    END                                             AS order_cancelled_flag,

    -- NULL - AGABI ONLY FIELDS (TBD)
    CAST(NULL AS STRING)                                            AS order_type_category_code,
    CAST(NULL AS STRING)                                            AS revenue_flag,
    CAST(NULL AS STRING)                                            AS legacy_order_Type_code,

    -- NULL - IMPACT ANALYSIS FIELDS
    CAST(NULL AS DATE)                                            AS order_reporting_date,
    CAST(NULL AS STRING)                                            AS order_completion_status_code,
    CAST(NULL AS STRING)                                            AS order_header_status_code,
    CAST(NULL AS STRING)                                            AS order_item_status_code,

    -- =========================================================
    -- CALENDAR DIMENSION - ORDER CREATE
    -- Table : vw_nnbu_clndr_dmnsn
    -- Join  : cldr_trns_dt = CAST(LINE_BOOKED_DATE AS DATE)
    -- Cols  : nnbu_fscl_yr_qtr_nr, fisc_yr_nm, nnbu_week_nr
    -- =========================================================
    clndr_create.fiscal_quarter                     AS order_create_fiscal_quarter,
    clndr_create.reporting_fiscal_year              AS order_create_reporting_fiscal_year,
    clndr_create.reporting_week_number              AS order_create_reporting_week_number,

    -- =========================================================
    -- DERIVED - ORDER CANCELLED DATE
    -- =========================================================
    CASE WHEN COALESCE(src.REASON_FOR_REJECTION,'') <> '' THEN src.CHANGED_ON
         ELSE NULL
    END                                             AS order_cancelled_date,

    -- =========================================================
    -- CALENDAR DIMENSION - ORDER CANCELLED
    -- Table : vw_nnbu_clndr_dmnsn
    -- Join  : cldr_trns_dt = CAST(CHANGED_ON AS DATE) when cancelled
    -- =========================================================
    clndr_cancel.fiscal_quarter                     AS order_cancelled_fiscal_quarter,
    clndr_cancel.reporting_fiscal_year              AS order_cancelled_reporting_fiscal_year,
    clndr_cancel.reporting_week_number              AS order_cancelled_reporting_week_number,

    -- =========================================================
    -- DERIVED - CANCELLED CATEGORY
    -- =========================================================
    CASE
        WHEN CAST(src.LINE_BOOKED_DATE AS DATE) >= DATE_TRUNC('quarter', CURRENT_DATE)
             AND COALESCE(src.REASON_FOR_REJECTION,'') <> '' THEN 'CQ'
        WHEN CAST(src.LINE_BOOKED_DATE AS DATE) < DATE_TRUNC('quarter', CURRENT_DATE)
             AND COALESCE(src.REASON_FOR_REJECTION,'') <> '' THEN 'PQ'
        ELSE ''
    END                                             AS cancelled_category,

    -- =========================================================
    -- CALENDAR DIMENSION - FINANCIAL CLOSE
    -- Table : vw_nnbu_clndr_dmnsn
    -- Join  : cldr_trns_dt = CAST(UPDT_DTM AS DATE)
    -- =========================================================
    clndr_fin.fiscal_quarter                        AS financial_close_fiscal_quarter,

    -- NULL - REMAINING TBD / IMPACT ANALYSIS
    CAST(NULL AS STRING)                                            AS customer_purchase_order_flag,
    CAST(NULL AS STRING)                                            AS revenue_recognition_fiscal_year_period,
    CAST(NULL AS DATE)                                            AS revenue_recognition_date,
    CAST(NULL AS STRING)                                            AS revenue_recognition_category_code,
    CAST(NULL AS STRING)                                            AS cp_country_name,
    CAST(NULL AS DECIMAL(18,2))                                            AS allocation_rate,
    CAST(NULL AS DECIMAL(18,2))                                            AS allocation_net_value_usd_amount,
    CAST(NULL AS DECIMAL(18,2))                                            AS allocation_cp_net_revenue_usd_amount,
    CAST(NULL AS DECIMAL(18,2))                                            AS allocated_net_value_order_quantity,
    CAST(NULL AS STRING)                                            AS financial_order_adjustment_flag,
    CAST(NULL AS STRING)                                            AS fiscal_week_code,
    CAST(NULL AS DECIMAL(18,2))                                            AS financial_order_item_net_value_usd_amt,

    -- =========================================================
    -- DIRECT MAPPINGS - PRODUCT FAMILY
    -- =========================================================
    src.PRODUCT_FAMILY                              AS product_family,
    src.PRODUCT_FAMILY_DESC                         AS product_family_descr,
    src.TAA_IDF_INDICATOR                           AS taa_idf_indicator,

    -- =========================================================
    -- DIRECT MAPPINGS - JUNIPER NATIVE PARTY COLUMNS
    -- =========================================================
    src.SOLD_TO_PARTY_ID                            AS jnpr_sold_to_party_id,
    src.SOLD_TO_PARTY_NAME                          AS jnpr_sold_to_party_name,
    src.SOLD_TO_COUNTRY                             AS jnpr_sold_to_country_code,
    src.SHIP_TO_PARTY_ID                            AS jnpr_ship_to_party_id,
    src.SHIP_TO_PARTY_NAME                          AS jnpr_ship_to_party_name,
    src.SHIP_TO_COUNTRY                             AS jnpr_ship_to_country_code,
    src.END_CUSTOMER_ID                             AS jnpr_end_customer_party_id,
    src.END_CUSTOMER_NAME                           AS jnpr_end_customer_party_name,
    src.END_CUSTOMER_COUNTRY                        AS jnpr_end_customer_country_code,
    src.END_CUSTOMER_PARENT_NO                      AS jnpr_end_customer_parent_number,
    src.END_CUSTOMER_PARENT_NAME                    AS jnpr_end_customer_parent_name,
    src.END_CUSTOMER_ULTIMATE_PARENT_NO             AS jnpr_end_customer_ultimate_parent_number,
    src.END_CUSTOMER_UTLIMATE_PARENT_NAME           AS jnpr_end_customer_ultimate_parent_name,
    src.END_CUSTOMER_GLOBAL_ULTIMATE_PARENT_NO      AS jnpr_end_customer_global_ultimate_parent_number,
    src.END_CUSTOMER_GLOBAL_ULTIMATE_PARENT_NAME    AS jnpr_end_customer_global_ultimate_parent_name,
    src.BILL_TO_PARTY_ID                            AS jnpr_bill_to_party_id,
    src.BILL_TO_PARTY_NAME                          AS jnpr_bill_to_party_name,
    src.BILL_TO_COUNTRY                             AS jnpr_bill_to_country_code,
    src.PAYER                                       AS jnpr_payer_party_id,
    src.PAYER_NAME                                  AS jnpr_payer_party_name,
    src.PAYER_COUNTRY                               AS jnpr_payer_country_code,
    src.DISTRIBUTOR                                 AS jnpr_distributor_id,
    src.DISTRIBUTOR_NAME                            AS jnpr_distributor_name,
    src.DISTRIBUTOR_COUNTRY                         AS jnpr_distributor_country_code,
    src.RESELLER                                    AS jnpr_reseller_party_id,
    src.RESELLER_NAME                               AS jnpr_reseller_party_name,
    src.RESELLER_COUNTRY                            AS jnpr_reseller_country_code,
    src.TOP_MODEL                                   AS jnpr_top_model,
    src.PRODUCT_CATEGORY                            AS jnpr_product_category_code,
    src.PRODUCT_CATEGORY_DESC                       AS jnpr_product_category_descr,
    src.PRODUCT_FAMILY                              AS jnpr_product_party_family_code,
    src.PRODUCT_FAMILY_DESC                         AS jnpr_product_party_family_descr,
    src.PRODUCT_HIERARCHY                           AS jnpr_product_hierarchy_code,
    src.PRODUCT_HIERARCHY_DESC                      AS jnpr_product_hierarchy_descr,
    src.PRODUCT_LINE                                AS jnpr_product_line,
    src.PRODUCT_LINE_DESC                           AS jnpr_product_line_descr,
    src.PRODUCT_OVERLENGTH_PART_NO                  AS jnpr_product_overlength_part_number,
    src.PRODUCT_TYPE                                AS jnpr_product_type_code,
    src.PRODUCT_TYPE_TEXT                           AS jnpr_product_type_descr,
    src.PROFIT_CENTER                               AS jnpr_profit_center_code,
    src.PROFIT_CENTER_TEXT                          AS jnpr_profit_center_descr,
    src.FISCAL_LINE_BOOK_QTR                        AS jnpr_order_create_fiscal_quarter,
    src.MATERIAL_GROUP_TEXT                         AS jnpr_material_group_descr,
    src.MATERIAL_INTERNAL_REPORTING                 AS jnpr_material_internal_reporting_code,
    src.MATERIAL_INTERNAL_REPORTING_TEXT            AS jnpr_material_internal_reporting_descr,

    -- =========================================================
    -- EDP TIMESTAMPS
    -- =========================================================
    src.EDP_CREATED                                 AS edp_create_pacific_dtm,
    CONVERT_TIMEZONE('UTC', src.EDP_CREATED)        AS edp_create_utc_dtm,
    src.EDP_LAST_UPDATED                            AS edp_update_pacific_dtm,
    CONVERT_TIMEZONE('UTC', src.EDP_LAST_UPDATED)   AS edp_update_utc_dtm,

    -- =========================================================
    -- SOURCE LOAD METADATA + AUDIT TIMESTAMPS
    -- =========================================================
    src.SOURCE_FILE_NAME                            AS source_filename,
    src.INS_DTM                                     AS source_load_insert_utc_dtm,
    src.UPDT_DTM                                    AS source_load_update_utc_dtm,
    to_utc_timestamp(current_timestamp(), 'UTC')    AS insert_utc_timestamp,
    to_utc_timestamp(current_timestamp(), 'UTC')    AS update_utc_timestamp

-- =============================================================================
-- FROM + JOINS
-- =============================================================================
FROM src

-- BUSINESS AREA: LEFT(business_area_cd,2) = HPE_PRODUCT_LINE
LEFT JOIN biz_area
    ON biz_area.pl = src.HPE_PRODUCT_LINE

-- PROFIT CENTER HIERARCHY: pft_cntr_ky = PROFIT_CENTER
LEFT JOIN pft_cntr
    ON pft_cntr.pft_cntr_ky = src.PROFIT_CENTER

-- CALENDAR - ORDER CREATE: cldr_trns_dt = LINE_BOOKED_DATE
LEFT JOIN clndr  clndr_create
    ON clndr_create.cldr_trns_dt = CAST(src.LINE_BOOKED_DATE AS DATE)

-- CALENDAR - ORDER CANCELLED: cldr_trns_dt = CHANGED_ON (only when cancelled)
LEFT JOIN clndr  clndr_cancel
    ON clndr_cancel.cldr_trns_dt = CAST(
           CASE WHEN COALESCE(src.REASON_FOR_REJECTION,'') <> ''
                THEN src.CHANGED_ON ELSE NULL END
       AS DATE)

-- CALENDAR - FINANCIAL CLOSE: cldr_trns_dt = UPDT_DTM
LEFT JOIN clndr  clndr_fin
    ON clndr_fin.cldr_trns_dt = CAST(src.UPDT_DTM AS DATE)

-- ACCOUNT PARTY - SOLD TO: party_id = HPE_SOLD_TO_PARTY_ID
LEFT JOIN acct_party  sold_to
    ON sold_to.party_id = src.HPE_SOLD_TO_PARTY_ID

-- ACCOUNT PARTY - SHIP TO: party_id = HPE_SHIP_TO_PARTY_ID
LEFT JOIN acct_party  ship_to
    ON ship_to.party_id = src.HPE_SHIP_TO_PARTY_ID

-- ACCOUNT PARTY - END CUSTOMER: party_id = HPE_END_CUSTOMER_PARTY_ID
LEFT JOIN acct_party  end_cust
    ON end_cust.party_id = src.HPE_END_CUSTOMER_PARTY_ID

-- PARTNER XREF - RESELLER: juniper_sap_id = RESELLER (moved before reseller_xref which depends on it)
LEFT JOIN partner_xref res_xref
    ON TRIM(UPPER(res_xref.juniper_sap_id)) = TRIM(UPPER(src.RESELLER))

-- ACCOUNT PARTY - RESELLER via xref: join on xref-resolved HPE party_id
LEFT JOIN acct_party  reseller_xref
    ON reseller_xref.party_id = res_xref.hpe_party_id

-- ACCOUNT PARTY - RESELLER fallback: join directly on CDO HPE_RESELLER_PARTY_ID
LEFT JOIN acct_party  reseller_hpe
    ON reseller_hpe.party_id = src.HPE_RESELLER_PARTY_ID

-- ACCOUNT PARTY - BILL TO: party_id = HPE_BILL_TO_PARTY_ID
LEFT JOIN acct_party  bill_to
    ON bill_to.party_id = src.HPE_BILL_TO_PARTY_ID

-- ACCOUNT PARTY - PAYER: party_id = HPE_PAYER_PARTY_ID
LEFT JOIN acct_party  payer
    ON payer.party_id = src.HPE_PAYER_PARTY_ID

-- PARTNER XREF - DISTRIBUTOR: juniper_sap_id = DISTRIBUTOR
LEFT JOIN partner_xref dist_xref
    ON TRIM(UPPER(dist_xref.juniper_sap_id)) = TRIM(UPPER(src.DISTRIBUTOR))
-- Dynamic WHERE alternative (uncomment when control table is ready):
-- WHERE updt_dtm = (
--     SELECT last_processed_timestamp
--     FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_source_control_table
--     WHERE table_name = 'vw_jnpr_netorders_stg'
-- )
