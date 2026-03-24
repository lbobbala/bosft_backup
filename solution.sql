--1.	Create jnpr_netorders_landing (same structure/DDL as jnpr_netorders_incremental)

CREATE TABLE bu_nnbu.sch_bu_nnbu.jnpr_netorders_landing
LIKE bu_nnbu.sch_bu_nnbu.jnpr_netorders_source_incremental;

--2.	Take the 1st set of data from jnpr_netorders_incremental (i.e. updt_dtm of March 5th) and load into jnpr_netorders_landing
INSERT INTO bu_nnbu.sch_bu_nnbu.jnpr_netorders_landing
SELECT *
FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_source_incremental
WHERE DATE(updt_dtm) = '2026-03-05';

---3.	Complete the stg view: need to change to point to _landing table
CREATE OR REPLACE VIEW bu_nnbu.sch_bu_nnbu.vw_jnpr_netorders_stg AS
SELECT *
FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_landing; -- query will provided


--4.	Load the stg_view into jnpr_netorders_stg
DROP TABLE IF EXISTS bu_nnbu.sch_bu_nnbu.jnpr_netorders_stg;
CREATE TABLE bu_nnbu.sch_bu_nnbu.jnpr_netorders_stg AS
SELECT *
FROM bu_nnbu.sch_bu_nnbu.vw_jnpr_netorders_stg
WHERE 1=2;

INSERT INTO bu_nnbu.sch_bu_nnbu.jnpr_netorders_stg
SELECT *
FROM bu_nnbu.sch_bu_nnbu.vw_jnpr_netorders_stg;

SELECT COUNT(*) 
FROM bu_nnbu.sch_bu_nnbu.jnpr_netorders_stg;

--5.	Work on the cancellation logic and update jnpr_netorders_stg data as required (flowchart in ppt)
---------------------------------------------------------------------------------------------------------------
UPDATE bu_nnbu.sch_bu_nnbu.jnpr_netorders_stg
SET
    -- 🔹 Cancel Date
    order_cancelled_date =
        CASE
            WHEN rejection_reason_code IS NOT NULL THEN
                COALESCE(order_item_change_date, order_item_create_date, order_create_date)
            ELSE NULL
        END,

    -- 🔹 Cancelled Fiscal Quarter
    order_cancelled_fiscal_quarter =
        CASE
            WHEN rejection_reason_code IS NOT NULL THEN
                CONCAT(
                    'FY',
                    YEAR(COALESCE(order_item_change_date, order_item_create_date, order_create_date)),
                    'Q',
                    QUARTER(COALESCE(order_item_change_date, order_item_create_date, order_create_date))
                )
        END,

    -- 🔹 Reporting Week
    order_cancelled_reporting_week_number =
        CASE
            WHEN rejection_reason_code IS NOT NULL THEN
                WEEKOFYEAR(
                    COALESCE(order_item_change_date, order_item_create_date, order_create_date)
                )
        END,

    -- 🔹 Cancelled Category (CQ / PQ)
    cancelled_category =
        CASE
            WHEN rejection_reason_code IS NOT NULL THEN
                CASE
                    WHEN YEAR(order_item_create_date) = YEAR(
                            COALESCE(order_item_change_date, order_item_create_date, order_create_date)
                         )
                     AND QUARTER(order_item_create_date) = QUARTER(
                            COALESCE(order_item_change_date, order_item_create_date, order_create_date)
                         )
                    THEN 'CQ'
                    ELSE 'PQ'
                END
            ELSE NULL
        END,

    -- 🔹 Change Type
    order_item_change_type =
        CASE
            WHEN rejection_reason_code IS NOT NULL THEN 'CANCELLED'
            ELSE 'ACTIVE'
        END;
		
----------------------------------------------------------------------
		