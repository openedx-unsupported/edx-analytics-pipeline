/*
--    Progress done till tag: WIP
*/

SET SESSION AUTOCOMMIT TO on;
-- BEGIN;


/* base tables DDL */

/* simplified version of f_orderitem_transactions

-- two primary laws govern base_transactions
*** include only transactions with a course_id which represent a transaction for a valid product
*** include only transactions with a transaction_amount_per_item which represent valid receipt of payment from cybersource or paypal

-- relies on product classes, product details, and product skus coming directly as imputed form the ecommerce system
---- product detail inconsistencies require checking a product's sku setup directly in ecommerce

*/

-- the finance schema contains personally identifiable information only business intelligence, product, and finance should have access

DROP TABLE IF EXISTS finance.production.base_transactions_swap CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_transactions_swap
(
    org_id_key INT,
    white_label BOOLEAN, --BOOLEAN can easily cast to INT for numerical operations (e.g. sum(white_label::INT))
    course_id_key INT,
    transaction_fiscal_year INT NOT NULL,
    transaction_fiscal_quarter INT,
    payment_date DATE, --payment processed and received
    order_timestamp TIMESTAMP,
    payment_ref_id_key INT,
    initial_transaction BOOLEAN,
    order_product_class VARCHAR(128),
    order_product_detail VARCHAR(255),
    user_id INT,
    transaction_type VARCHAR(255),
    order_id INT,
    order_product_count INT,
    order_voucher_id INT,
    order_voucher_code VARCHAR(255),
    order_line_id INT,
    order_product_id INT,
    partner_sku VARCHAR(128),
    line_item_list_price NUMERIC(12,2),
    line_item_sales_price NUMERIC(12,2),
    line_item_discount NUMERIC(12,2),
    -- transaction_amount NUMERIC(12,2), --retiring transaction_amount now with the introduction of multiple item basket purchases
    transaction_amount_per_item NUMERIC(12,2),
    payment_ref_id VARCHAR(128),
    transaction_id VARCHAR(255),
    transaction_payment_gateway_id VARCHAR(128),
    transaction_payment_gateway_account_id VARCHAR(128),
    transaction_payment_method VARCHAR(128),
    transaction_iso_currency_code VARCHAR(12),
    order_username VARCHAR(30),
    course_run_id_number INT,
    order_course_id VARCHAR(255),
    order_org_id VARCHAR(128),
    order_processor VARCHAR(32),
    course_uuid VARCHAR(255),
    expiration_date TIMESTAMP,
    duplicate_charges BOOLEAN,
    duplicate_refunds BOOLEAN,
    single_transaction_refund BOOLEAN,
    duplicate_charges_without_refund BOOLEAN,
    transaction_key INT,
    unique_transaction_id VARCHAR(255),
    created_time TIMESTAMP_TZ DEFAULT current_timestamp()
    -- inserted TIMESTAMP_TZ DEFAULT current_timestamp()
) PARTITION BY transaction_fiscal_year;

GRANT ALL PRIVILEGES ON finance.production.base_transactions_swap TO automationrole;

/*
* creates a join table for a "master" org_id mapping
* partner removes the "x" from the suffix, would preferably reference the institutions full unabbreviated name
*/
DROP TABLE IF EXISTS finance.production.base_course_partner_swap CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_course_partner_swap
(
    org_id_key INT, --based off original org_id
    white_label BOOLEAN,
    org_id_original VARCHAR(255),
    org_id_new VARCHAR(255),
    partner VARCHAR(1036)
);

GRANT ALL PRIVILEGES ON finance.production.base_course_partner_swap TO automationrole;


/*
* flags tests and duplicate courses to exclude from catalog run number
*/
DROP TABLE IF EXISTS finance.production.base_course_tests CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_course_tests
(
    course_id_key INT,
    course_id VARCHAR(255),
    catalog_course VARCHAR(255),
    catalog_course_title VARCHAR(255),
    partner_short_code VARCHAR(8),
    reporting_type VARCHAR(20),
    test BOOLEAN
);

GRANT ALL PRIVILEGES ON finance.production.base_course_tests TO automationrole;

/*
* uses the master org_id mapping above to reassign course_id to one master org_id
* e.g. HarvardX replaces harvardx and Harvardx
* this also has a small impact on run number
* catalog_run used for determining minimum contribution for revenue share payouts to partner
*/
DROP TABLE IF EXISTS finance.production.base_course_title_swap CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_course_title_swap
(
course_id_key INT,
org_id VARCHAR(255),
pacing_type_id INT,
catalog_course VARCHAR(255),
course_id VARCHAR(255),
catalog_course_title VARCHAR(255),
white_label BOOLEAN,
start_time TIMESTAMP,
end_time TIMESTAMP,
time_reference TIMESTAMP,
valid_start_range BOOLEAN NOT NULL,
new_run BOOLEAN,
new_run_number INT,
run_number INT,
partner_course_number INT,
catalog_run INT,
catalog_offering INT
);

GRANT ALL PRIVILEGES ON finance.production.base_course_title_swap TO automationrole;

/*
* creates distinct mapping for course_id and program type
* if the course has any MicroMasters tag variant then it receives the MicroMasters tag, otherwise it takes the last ID used from d_program_course
*/
DROP TABLE IF EXISTS finance.production.base_program_courses_swap CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_program_courses_swap
(
    course_id_key INT,
    program_type_key INT,
    program_title_key INT,
    program_type VARCHAR(32),
    program_title VARCHAR(255),
    course_id VARCHAR(255),
    -- program_type_micromasters_launch VARCHAR(32),
    former_xs BOOLEAN,
    program_slot_number INT,
    courses_in_program INT,
    avg_course_in_program_length_weeks INT
);

GRANT ALL PRIVILEGES ON finance.production.base_program_courses_swap TO automationrole;

/*
* course level tables that calculates total exam fees
* detail not available at payment level
* the aggregate table will distribute the fees by the count of rows tied to the course_id so that sum(exam_fees) will aggregate by course_id
*/

DROP TABLE IF EXISTS finance.production.base_course_exams CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_course_exams
(
    course_id_key INT,
    course_id VARCHAR(255),
    unique_exams INT,
    exams_proctored INT,
    exam_fees INT,
    exams_proctored_practice INT
);

GRANT ALL PRIVILEGES ON finance.production.base_course_exams TO automationrole;

/*
* this could come from business_intelligence.course_master
* created_time a quick one off table in the interest of time while testing during the day
*/

DROP TABLE IF EXISTS finance.production.base_course_price CASCADE;
CREATE TABLE IF NOT EXISTS finance.production.base_course_price
(
    course_id_key INT,
    course_id VARCHAR(255),
    course_seat_price FLOAT
);

GRANT ALL PRIVILEGES ON finance.production.base_course_price TO automationrole;

/*
*
* end table DDL
*
*/


/*
*
* start base table build
*
*/

/*
* intended for use only for backfilling untagged subjects
************ LOOK HERE ************
******* course subjects should be modified directly in discovery and will flow through to business_intelligence.dim_course
************           ************
* can depreciate base_course_subject stop gap if backfill no longer needed
*/

DROP TABLE IF EXISTS finance.production.base_course_subject CASCADE;
CREATE TABLE finance.production.base_course_subject AS /*+ direct */
WITH catalog_subject AS (
SELECT
    dc.course_id,
    catalog_course,
--	Max(subject_title) subject_title,
    MIN(org_id) org_id,
    MIN(catalog_course_title) catalog_course_title,
    MIN(marketing_url) marketing_url,
    MAX(MIN(dcsrn.row_number)) OVER (PARTITION BY catalog_course ORDER BY CASE WHEN MAX(subject_title) IS NULL THEN 1 ELSE 0 END) as row_number,
    COUNT(dcsrn.row_number)>1 multiple_subjects,
    SUM(COUNT(DISTINCT dc.course_id)) OVER (PARTITION BY catalog_course) > 1 multiple_runs,
    MAX(CASE WHEN dcsrn.subject_title IS NULL THEN 1 ELSE 0 END) no_subject
FROM
    production.production.d_course dc
LEFT JOIN
(
    select *, ROW_NUMBER() over (order by course_id) as row_number
    from production.production.d_course_subjects
) dcsrn
ON dc.course_id = dcsrn.course_id
GROUP BY 1, 2
       )
SELECT
       HASH(cs.course_id) course_id_key,
       HASH(COALESCE(
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY catalog_course ORDER BY cs.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY org_id,regexp_substr(catalog_course,'\\b[[:alpha:]]+', 1,2, 'ie') ORDER BY dcsrn.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY org_id,catalog_course_title ORDER BY dcsrn.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY catalog_course_title ORDER BY dcsrn.row_number)
       )) subject_key,
       COALESCE(
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY catalog_course ORDER BY cs.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY org_id,regexp_substr(catalog_course,'\\b[[:alpha:]]+', 1,2, 'ie') ORDER BY dcsrn.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY org_id,catalog_course_title ORDER BY dcsrn.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY catalog_course_title ORDER BY dcsrn.row_number)
       ) subject_title,
       catalog_course,
       catalog_course_title,
       marketing_url,
       multiple_subjects,
       multiple_runs,
       CASE WHEN COALESCE(no_subject,1) = 1
              AND
              COALESCE(
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY catalog_course ORDER BY cs.row_number),
              FIRST_VALUE(subject_title) IGNORE NULLS OVER (PARTITION BY org_id,regexp_substr(catalog_course,'\\b[[:alpha:]]+', 1,2, 'ie') dcsrn.row_number)
                     ) IS NOT NULL
              THEN TRUE
              ELSE FALSE
       END proxy_subject
FROM
       catalog_subject cs
LEFT JOIN
(
    select *, ROW_NUMBER() over (order by course_id) as row_number
    from production.production.d_course_subjects
) dcsrn
ON
       cs.course_id = dcsrn.course_id
AND
       cs.row_number = dcsrn.row_number
ORDER BY
       1;

GRANT SELECT on finance.production.base_course_subject to automationrole;

-- *
-- * 11/27/2018
-- ************ LOOK HERE ************
-- * replace the block for temp table lms_user_ids with business_intelligence.dim_user when committed
-- ************           ************
-- * depreciate after conversion to dim_user
-- *

-- join retired users to transactions, using integers to efficiently join email and username later
DROP TABLE IF EXISTS app_data.wwc.lms_user_ids;
CREATE LOCAL TEMP TABLE app_data.wwc.lms_user_ids ON COMMIT PRESERVE ROWS AS /*+ direct */
SELECT
       du.user_id,
       HASH(COALESCE(uaur.original_username,du.user_username)) AS user_username_key,
       HASH(COALESCE(uaur.original_email,du.user_email)) AS user_email_key,
       COALESCE(uaur.original_username,du.user_username) AS user_username,
       COALESCE(uaur.original_email,du.user_email) as user_email,
       ROW_NUMBER() OVER (PARTITION BY COALESCE(uaur.original_email,du.user_email) ORDER BY uaur.original_name IS NULL) sort_order,
       COALESCE(uaur.original_username IS NOT NULL,FALSE) retired_user
FROM
       production.production.d_user du
LEFT JOIN
       app_data.wwc.user_api_userretirementstatus uaur
ON
       du.user_id = uaur.user_id;

-- return lms_user_id if exists
DROP TABLE IF EXISTS app_data.ecommerce.ecommerce_user_strip;
CREATE LOCAL TEMP TABLE app_data.ecommerce.ecommerce_user_strip ON COMMIT PRESERVE ROWS AS /*+ direct */
SELECT
       id AS order_user_id,
       regexp_substr(tracking_context::VARCHAR,'"lms_user_id"\:(\d+)',1,1,'e')::INT AS tracked_user_id, --look into json extract functions if available
       HASH(username) username_key,
       HASH(email) email_key,
       username,
       email
FROM
       app_data.ecommerce.ecommerce_user
ORDER BY
       1;

-- return the lms user_id from that placed the order instead of the ecommerce user_id
-- must remain unique by order_user_id
-- this will miss user_ids created on the day of the order
-- may depreciate the user of username in the future for base_transactions, useful today to check against ecommerce user
DROP TABLE IF EXISTS app_data.ecommerce.order_user_ids;
CREATE LOCAL TEMP TABLE app_data.ecommerce.order_user_ids ON COMMIT PRESERVE ROWS AS /*+ direct */
SELECT
       order_user_id,
       COALESCE(
              eu.tracked_user_id,
              lui1.user_id,
              lui2.user_id,
              lui3.user_id
              ) AS user_id,
       COALESCE(
              lui1.user_username,
              lui2.user_username,
              lui3.user_username
              ) AS user_username,
       COALESCE(
              lui1.retired_user,
              lui2.retired_user,
              lui3.retired_user
              ) AS retired_user
FROM
       app_data.ecommerce.ecommerce_user_strip eu
LEFT JOIN
       app_data.wwc.lms_user_ids lui1
ON
       eu.tracked_user_id = lui1.user_id
LEFT JOIN
       app_data.wwc.lms_user_ids lui2
ON
       eu.username_key = lui2.user_username_key
LEFT JOIN
       app_data.wwc.lms_user_ids lui3
ON
       eu.email_key = lui3.user_email_key
AND
       lui2.user_username IS NULL;

/*
* preserve as much detail from the original order as possible
* join back to dim_course for reporting cleanup of subject & org
*/

TRUNCATE TABLE finance.production.base_transactions_swap;
INSERT /*+ direct */ INTO finance.production.base_transactions_swap
WITH product_counts as (
SELECT
       order_id,
       COUNT(id) AS order_product_count
FROM
       app_data.ecommerce.order_line
GROUP BY
       order_id
ORDER BY
       order_id
       ),
return_order_line as (
SELECT
       order_id,
       product_id,
       partner_sku,
       id AS order_line_id
FROM
       app_data.ecommerce.order_line
ORDER BY
       order_id,
       product_id
       )
SELECT
       HASH(CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX' ELSE order_org_id END) AS org_id_key,
       CASE WHEN fot.partner_short_code ILIKE 'edx' THEN FALSE ELSE TRUE END AS white_label,
       -- CASE WHEN fot.partner_short_code ILIKE 'edx' or dc.partner_site_id > 1 THEN FALSE ELSE TRUE END AS white_label,
       -- possible modification: use  the line above as a secondary check with the dim_course partner site tagging to avoid incorrect ecommerce product setups which have occurred historically
       -- potentially subject to the same error if course setup incorrectly in discovery
       HASH(CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END) AS course_id_key,
       YEAR(TIMESTAMPADD('m',6,COALESCE(transaction_date::DATE,order_timestamp))) AS transaction_fiscal_year,
       ((QUARTER(COALESCE(transaction_date::DATE,order_timestamp))+1)%4)+1 AS transaction_fiscal_quarter,
       COALESCE(transaction_date::DATE,order_timestamp)::DATE AS payment_date,
       order_timestamp AS order_timestamp,
       HASH(payment_ref_id) AS payment_ref_id_key,
       ROW_NUMBER() OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id
              ORDER BY COALESCE(transaction_date::DATE,order_timestamp)::DATE,(case when (transaction_type='sale') then 1 else 0 end)::INT,HASH(COALESCE(fot.transaction_date::DATE,fot.order_timestamp)::DATE,payment_ref_id,unique_order_id,unique_transaction_id,unique_order_line_item_id,order_line_item_product_id,transaction_audit_code))=1 AS initial_transaction,
       (CASE
            WHEN order_product_class='seat' AND order_product_detail='audit' AND transaction_amount_per_item <> 0 THEN 'support_fee'
            WHEN order_product_class='seat' AND order_product_detail='' AND transaction_amount_per_item <> 0 THEN 'support_fee'
            ELSE order_product_class
        END) AS order_product_class,
       (CASE
            WHEN order_product_class='seat' AND order_product_detail='audit' AND transaction_amount_per_item <> 0 THEN 'support_fee'
            WHEN order_product_class='seat' AND order_product_detail='' AND transaction_amount_per_item <> 0 THEN 'support_fee'
            ELSE order_product_detail
        END) AS order_product_detail,
       COALESCE(oui.user_id,lui.user_id) AS user_id,
       transaction_type,
       fot.order_id,
       pc.order_product_count,
       order_voucher_id,
       NULLIF(order_voucher_code,''),
       fot.order_line_item_id, -- order_line_id, --updated from rol.order_line_id to fot.order_line_item_id
       order_line_item_product_id, -- order_product_id
       rol.partner_sku, --same here, preferably data engineering brings in SKU directly
       order_line_item_unit_price, --list price
       order_line_item_price, --sales price
       order_line_item_unit_price - order_line_item_price, --item discount
       -- transaction_amount,
       transaction_amount_per_item,
       payment_ref_id,
       transaction_id,
       transaction_payment_gateway_id,
       transaction_payment_gateway_account_id,
       transaction_payment_method,
       transaction_iso_currency_code,
       COALESCE(oui.user_username,lui.user_username) AS order_username,
       CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN -HASH('edX direct donation') ELSE dcr.course_run_id_number END AS course_run_id_number, -- negative to never intersect with a real course id since direct donation does not exist as a course product
       CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END AS order_course_id,
       COALESCE(
              CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX' ELSE order_org_id END,
              dc.org_id --proxy org for entitlements
              )
              AS order_org_id,
       order_processor,
       COALESCE(dc.course_uuid,fot.course_uuid) AS course_uuid,
       expiration_date,
       SUM((case when (transaction_type='sale') then 1 else 0 end)::INT) OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id )>1 AS duplicate_charges,
       SUM((case when (transaction_type='refund') then 1 else 0 end)::INT) OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id )>1 AS duplicate_refund,
       SUM((case when (transaction_type='sale') then 1 else 0 end)::INT) OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id )=0
            AND SUM((case when (transaction_type='refund') then 1 else 0 end)::INT) OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id )>0  AS single_transaction_refund,
       SUM((case when (transaction_type='sale') then 1 else 0 end)::INT) OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id )>1
            AND SUM((case when (transaction_type='refund') then 1 else 0 end)::INT) OVER (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id )=0 AS duplicate_charges_without_refund,
       HASH(COALESCE(fot.transaction_date::DATE,fot.order_timestamp)::DATE,payment_ref_id,unique_order_id,unique_transaction_id,unique_order_line_item_id,order_line_item_product_id,transaction_audit_code) AS transaction_key, --create a unique row identifier
       unique_transaction_id
FROM
       finance.production.f_orderitem_transactions fot
LEFT JOIN
       app_data.wwc.lms_user_ids lui
ON
       fot.order_processor = 'shoppingcart'
AND
       fot.order_user_id = lui.user_id
LEFT JOIN
       app_data.ecommerce.order_user_ids oui
ON
       fot.order_processor = 'otto'
AND
       fot.order_user_id = oui.order_user_id
LEFT JOIN
       product_counts pc
ON
       fot.order_id = pc.order_id
LEFT JOIN
       return_order_line rol
ON
       fot.order_id = rol.order_id
AND
       fot.order_line_item_product_id = rol.product_id
LEFT JOIN
       business_intelligence.production.dim_course dc
ON
       REPLACE(fot.course_uuid,'-','') = dc.course_uuid --course_uuid should only populate for entitlements
LEFT JOIN
       business_intelligence.production.dim_course_run dcr
ON
       (case -- only the following two courses have incorrect letter case used for X in the course_id
              when fot.order_course_id = 'course-v1:PekingX+04833050x+1T2016' then 'course-v1:PekingX+04833050X+1T2016'
              when fot.order_course_id = 'BerkeleyX/CS.CS169.1X/3T2013' then 'BerkeleyX/CS.CS169.1x/3T2013'
              else fot.order_course_id
       end)
              = dcr.course_id
WHERE
       transaction_amount_per_item IS NOT NULL
AND
       (CASE
              WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation'
              when order_product_class='course-entitlement' then 'course-entitlement'
              ELSE order_course_id
       END) IS NOT NULL
/*

WINDOW
       course_payment_partition AS (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id),
       first_payment_partition AS
       (PARTITION BY CASE WHEN order_product_detail='donation' AND order_product_class='donation' THEN 'edX direct donation' ELSE order_course_id END,payment_ref_id
              ORDER BY
                     COALESCE(transaction_date::DATE,order_timestamp)::DATE,(transaction_type='refund')::INT,HASH(COALESCE(fot.transaction_date::DATE,fot.order_timestamp)::DATE,payment_ref_id,unique_order_id,unique_transaction_id,unique_order_line_item_id,order_line_item_product_id,transaction_audit_code));

*/

DROP TABLE IF EXISTS finance.production.base_transactions CASCADE;
ALTER TABLE finance.production.base_transactions_swap RENAME TO finance.production.base_transactions;


TRUNCATE TABLE finance.production.base_course_partner_swap;
INSERT /*+ direct */ INTO finance.production.base_course_partner_swap
WITH base_org as (
SELECT
       COALESCE(order_org_id,dc.org_id) AS org_id,
       CASE
              WHEN COALESCE(order_org_id,dc.org_id) ILIKE 'mitprof%' THEN 'MITProfessionalX'
              ELSE REPLACE(LOWER(COALESCE(order_org_id,dc.org_id)),'_','')
       END AS org_id_join, --need a common denominator
       MAX(( case when(dc.partner_short_code NOT ILIKE '%edx%') then 1 ELSE 0 END )) AS white_label_setup,
       -- MAX(case when dc.start_time<current_date then dc.start_time end) AS latest_start_time,
       COUNT(payment_ref_id_key) AS total_org_occurences,
       ROW_NUMBER() OVER (PARTITION BY REPLACE(LOWER(COALESCE(order_org_id,dc.org_id)),'_','') ORDER BY COUNT(payment_ref_id_key) DESC) AS total_org_occurence_order
       --org ranked by the number of purchases made under that org_id and works well on a few spot checked, but likely does not always represent the correct org_id name
       -- ideally this would match the logo represented on edx.org, but have not had time to go through and match
FROM
       production.production.d_course dc
FULL JOIN
       finance.production.base_transactions bt
ON
       dc.org_id = bt.order_org_id
GROUP BY 1,2
       )
SELECT
       HASH(COALESCE(oiu.org_id,bo.org_id)) AS org_id_key,
       white_label_setup=1 AS white_label,
       bo.org_id AS org_id_original,
       CASE
              WHEN oiu.org_id IS NOT NULL THEN oiu.updated_org_id
              ELSE FIRST_VALUE(bo.org_id) OVER (PARTITION BY org_id_join ORDER BY total_org_occurence_order)
       END AS org_id_new,  --chooses the org with the most recent start time then highest number of purchases
       CASE
              WHEN bo.org_id IN ('edX_Learning_Sciences','e0dX') THEN 'edX'
              WHEN bo.org_id ILIKE '%edX%' THEN regexp_replace(bo.org_id,'edx','edX',1,1,'bi')
              WHEN regexp_like(org_id_join,'mitpro|mitxpro','bi') THEN 'MITProfessional'
              WHEN oiu.org_id IS NOT NULL THEN oiu.updated_org_id
              ELSE REGEXP_REPLACE(FIRST_VALUE(bo.org_id) OVER (PARTITION BY org_id_join ORDER BY total_org_occurence_order),'x(\\b|d)', '',1,1,'i')
       END AS partner --this column is for formatting, 4/3/2018 updated for consistency with org_id_new, but will potentially rework/depreciate
FROM
       base_org bo
LEFT JOIN
       finance.production.org_id_updates oiu
ON
       bo.org_id=oiu.org_id;
DROP TABLE IF EXISTS finance.production.base_course_partner CASCADE;
ALTER TABLE finance.production.base_course_partner_swap RENAME to finance.production.base_course_partner;


-- can use base_course_tests to catch any additional private courses not marked unpublished from dim_course_run
TRUNCATE TABLE finance.production.base_course_tests;
INSERT /*+ direct */ INTO finance.production.base_course_tests
SELECT
       HASH(course_id) AS course_id_key,
       course_id,
       catalog_course,
       catalog_course_title,
       partner_short_code,
       reporting_type,
       regexp_like (
        regexp_substr(catalog_course_title, '(([^A-Za-z]|\\b)(test|tests|testak|Testing for WL course creation|RaphTestCourse|duplicate|dupe)(\\b|\\s|\_))(.*[^p][^r][^e][^p])?', 1, 1, 'ie') ,
      '([^A-Za-z]|\\b)(test|tests|testak|Testing for WL course creation|RaphTestCourse|duplicate|dupe)(\\b|\\s|\_)','i'
        ) test
FROM
       production.production.d_course dc -- could replace with dim_course & dim_course_run
WHERE
       regexp_like (
        regexp_substr(catalog_course_title, '(([^A-Za-z]|\\b)(test|tests|testak|Testing for WL course creation|RaphTestCourse|duplicate|dupe)(\\b|\\s|\_))(.*[^p][^r][^e][^p])?', 1, 1, 'ie') ,
          '([^A-Za-z]|\\b)(test|tests|testak|Testing for WL course creation|RaphTestCourse|duplicate|dupe)(\\b|\\s|\_)','i'
        )
       OR
       regexp_like (
        regexp_substr(course_id, '(([^A-Za-z]|\\b)(test|tests|testak|Testing for WL course creation|RaphTestCourse|duplicate|dupe)(\\b|\\s|\_))(.*[^p][^r][^e][^p])?', 1, 1, 'ie') ,
      '([^A-Za-z]|\\b)(test|tests|testak|Testing for WL course creation|RaphTestCourse|duplicate|dupe)(\\b|\\s|\_)','i'
        );


TRUNCATE TABLE finance.production.base_course_title_swap;
INSERT /*+ direct */ INTO finance.production.base_course_title_swap
WITH catalog_first_run as (
SELECT
       catalog_course,
       MIN(start_time) AS first_start_time,
       MAX(start_time) as last_start_time,
       MAX(CASE WHEN start_time BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '2 years' THEN start_time ELSE CURRENT_DATE-1 end) last_start_time_two_years
FROM
       production.production.d_course -- replace reference to dim_course_run if time allows
GROUP BY
       catalog_course
ORDER BY
       catalog_course
       ),
before_new_run AS (
SELECT
       HASH(dc.course_id) AS course_id_key,
       COALESCE(cp.org_id_new,dc.org_id) AS org_id,
       CASE dc.pacing_type
              WHEN 'instructor_paced' THEN 0
              WHEN 'self_paced' THEN 1
              ELSE 0
       END AS pacing_type_id,
       dc.catalog_course,
       dc.course_id,

       /*
       --       WIP: Work done till here
       */

       FIRST_VALUE(dc.catalog_course_title) OVER (
                 PARTITION BY dc.catalog_course ORDER BY (dc.catalog_course_title IS NULL)::INT, --nulls to last rank
                 (dc.catalog_course_title ilike '%DELETE%')::INT, --deletes to last rank
                 (dc.start_time>TIMESTAMPADD('year',2,CURRENT_DATE)), -- extreme start dates to last rank
                 COALESCE(start_time,announcement_time,enrollment_start_time,end_time) DESC,
                 dc.course_id) AS catalog_course_title,
       (dc.partner_short_code ilike '%edx%')::INT=0 AS white_label,
       start_time,
       end_time,
       COALESCE(start_time,end_time) AS time_reference,
       COALESCE(start_time BETWEEN first_start_time AND last_start_time_two_years,TRUE) valid_start_range,
       ROW_NUMBER() OVER ( --used to resort the run order for courses not correctly assigned to the appropriate catalog course
                            PARTITION BY CASE
                                                 WHEN dc.course_id in ('LinuxFoundationX/LFS101x/2T2014','course-v1:LinuxFoundationX+LFS101x.2+1T2015','course-v1:LinuxFoundationX+LFS101x.2+1T2016','course-v1:LinuxFoundationX+LFS101x.2+1T2016') THEN 'LinuxFoundationX+LFS101x'
                            -- partner manage identified course belongs to Introduction to Linux series
                                                 WHEN dc.course_id='UPValenciaX/EX101x/2T2015' THEN 'UPValenciaX+xls101x'
                            -- partner manager identified reassignment of course [Deciphering Secrets: Unlocking the Manuscripts of Medieval Burgos (Spain)] to the same catalog course
                                                 WHEN dc.course_id in ('course-v1:UC3Mx+HGA.2.1x+1T2017','course-v1:UC3Mx+HGA.2.2x+2T2017') THEN 'UC3Mx+HGA.2x'
                            -- partner manager identified potential reassignment of course Human Rights Defenders in different languages to this catalog course
                            -- currently discussing with AmnestyInternationalX and not implemented
                                                 -- WHEN dc.org_id='AmnestyInternationalX' AND regexp_like(dc.catalog_course,'Rights3x\w+') THEN 'AmnestyInternationalX+Rights3x'
                                                 ELSE dc.catalog_course END
                            ORDER BY COALESCE(start_time,end_time),dc.catalog_course_title
                            ) = 1 AS new_run,
                            -- partner manager identified course number changed inconsistently, but should belong to the same run
                            -- the case corrects the new run number tagging for UPValencia's Excel course

       ROW_NUMBER() OVER ( --used to resort the run order for courses not correctly assigned to the appropriate catalog course
                            PARTITION BY CASE
                                                 WHEN dc.course_id in ('LinuxFoundationX/LFS101x/2T2014','course-v1:LinuxFoundationX+LFS101x.2+1T2015','course-v1:LinuxFoundationX+LFS101x.2+1T2016','course-v1:LinuxFoundationX+LFS101x.2+1T2016') THEN 'LinuxFoundationX+LFS101x'
                            -- partner manage identified course belongs to Introduction to Linux series
                                                 WHEN dc.course_id='UPValenciaX/EX101x/2T2015' THEN 'UPValenciaX+xls101x'
                            -- partner manager identified reassignment of course [Deciphering Secrets: Unlocking the Manuscripts of Medieval Burgos (Spain)] to the same catalog course
                                                 WHEN dc.course_id in ('course-v1:UC3Mx+HGA.2.1x+1T2017','course-v1:UC3Mx+HGA.2.2x+2T2017') THEN 'UC3Mx+HGA.2x'
                            -- partner manager identified potential reassignment of course Human Rights Defenders in different languages to this catalog course
                            -- currently discussing with AmnestyInternationalX and not implemented
                                                 -- WHEN dc.org_id='AmnestyInternationalX' AND regexp_like(dc.catalog_course,'Rights3x\w+') THEN 'AmnestyInternationalX+Rights3x'
                                                 ELSE dc.catalog_course END
                            ORDER BY COALESCE(start_time,end_time),dc.catalog_course_title
                            )  AS run_number,
                            -- partner manager identified course number changed inconsistently, but should belong to the same run
                            -- the case corrects the new run number tagging for UPValencia's Excel course

       ROW_NUMBER() OVER (PARTITION BY COALESCE(cp.org_id_new,dc.org_id) ORDER BY COALESCE(start_time,end_time),end_time,dc.catalog_course_title,dc.course_id) AS partner_course_number,
       DENSE_RANK() OVER (PARTITION BY COALESCE(cp.org_id_new,dc.org_id) ORDER BY fr.first_start_time,end_time,dc.catalog_course_title,dc.course_id) AS catalog_run,
       DENSE_RANK() OVER (PARTITION BY COALESCE(cp.org_id_new,dc.org_id) ORDER BY fr.first_start_time,dc.catalog_course) AS catalog_offering
FROM
       production.production.d_course dc -- replace reference to dim_course_run & dim course if time allows, column names will require changes
LEFT JOIN
       finance.production.base_course_partner cp
ON
       dc.org_id = cp.org_id_original
LEFT JOIN
       catalog_first_run fr
ON
       dc.catalog_course = fr.catalog_course
WHERE
       NOT EXISTS (SELECT course_id_key FROM finance.production.base_course_tests ct WHERE dc.course_id = ct.course_id)

/*      To be Removed

WINDOW -- allows specifying the partition used in a window function outside of select statement for simplifying long over clauses and identifying analytic functions using the same partition, no performance benefit (maybe)
       catalog_course_partition AS (PARTITION BY dc.catalog_course ORDER BY (dc.catalog_course_title IS NULL)::INT, --nulls to last rank
       (dc.catalog_course_title ilike '%DELETE%')::INT, --deletes to last rank
       (dc.start_time>TIMESTAMPADD('year',2,CURRENT_DATE)), -- extreme start dates to last rank
       COALESCE(start_time,announcement_time,enrollment_start_time,end_time) DESC,
       dc.course_id),
       catalog_partition AS ( --used to resort the run order for courses not correctly assigned to the appropriate catalog course
                            PARTITION BY CASE
                                                 WHEN dc.course_id in ('LinuxFoundationX/LFS101x/2T2014','course-v1:LinuxFoundationX+LFS101x.2+1T2015','course-v1:LinuxFoundationX+LFS101x.2+1T2016','course-v1:LinuxFoundationX+LFS101x.2+1T2016') THEN 'LinuxFoundationX+LFS101x'
                            -- partner manage identified course belongs to Introduction to Linux series
                                                 WHEN dc.course_id='UPValenciaX/EX101x/2T2015' THEN 'UPValenciaX+xls101x'
                            -- partner manager identified reassignment of course [Deciphering Secrets: Unlocking the Manuscripts of Medieval Burgos (Spain)] to the same catalog course
                                                 WHEN dc.course_id in ('course-v1:UC3Mx+HGA.2.1x+1T2017','course-v1:UC3Mx+HGA.2.2x+2T2017') THEN 'UC3Mx+HGA.2x'
                            -- partner manager identified potential reassignment of course Human Rights Defenders in different languages to this catalog course
                            -- currently discussing with AmnestyInternationalX and not implemented
                                                 -- WHEN dc.org_id='AmnestyInternationalX' AND regexp_like(dc.catalog_course,'Rights3x\w+') THEN 'AmnestyInternationalX+Rights3x'
                                                 ELSE dc.catalog_course END
                            ORDER BY COALESCE(start_time,end_time),dc.catalog_course_title
                            ) -- partner manager identified course number changed inconsistently, but should belong to the same run
                              -- the case corrects the new run number tagging for UPValencia's Excel course
       )
*/
SELECT
       course_id_key,
       org_id,
       pacing_type_id,
       catalog_course,
       course_id,
       catalog_course_title,
       white_label,
       start_time,
       end_time,
       time_reference,
       valid_start_range,
       new_run,
       SUM(new_run::INT) OVER (PARTITION BY org_id,new_run ORDER BY catalog_run) AS new_run_number, --essential to revenue share, heavy impact on payout
       run_number,
       partner_course_number,
       catalog_run,
       catalog_offering
FROM
       before_new_run;

DROP TABLE IF EXISTS finance.production.base_course_title CASCADE;
ALTER TABLE finance.production.base_course_title_swap RENAME to finance.production.base_course_title;


-- ************ LOOK HERE ************
-- potentially fully able to depreciate with dim_prgoram_course_run, but need to verify
-- ************           ************
TRUNCATE TABLE finance.production.base_program_courses_swap;
INSERT /*+ direct */ INTO finance.production.base_program_courses_swap
WITH course_program_count AS (
SELECT
       HASH(dpc.course_id) AS course_id_key,
       dpc.course_id,
       MAX(dpc.id) AS id,
       COUNT(DISTINCT program_type) AS program_tie,
       MAX(CASE WHEN program_type='MicroMasters' then 1 else 0 END) AS MicroMasters,
       (COUNT(DISTINCT CASE WHEN program_type = 'XSeries' THEN 1 END)+COUNT(DISTINCT CASE WHEN program_type = 'MicroMasters' THEN 1 END))>1 AS former_xs,
       MAX(timestampdiff('week',start_time,end_time)) course_length_weeks
FROM
       production.production.d_program_course dpc
LEFT JOIN
       production.production.d_course dc
ON
       dpc.course_id=dc.course_id
GROUP BY
       1,2
ORDER BY
       1
       ),
program_titles as (
SELECT
       cpc.course_id_key,
       HASH(CASE WHEN program_tie>1 AND MicroMasters=1 THEN 'MicroMasters' ELSE dpc.program_type END) AS program_type_key,
       HASH(FIRST_VALUE(program_title) OVER catalog_partition) AS program_title_key,
       CASE WHEN program_tie>1 AND MicroMasters=1 THEN 'MicroMasters' ELSE dpc.program_type END AS program_type, --tag as MicroMasters if ever tagged with MicroMasters and has multiple program tagging
       FIRST_VALUE(program_title) OVER catalog_partition AS program_title,
       cpc.course_id,
       -- CASE WHEN program_tie>1 AND MicroMasters=1 AND start_time>'2016-09-20' THEN 'MicroMasters' ELSE dpc.program_type END AS program_type_micromasters_launch, --tag as MicroMasters if ever tagged with MicroMasters and has multiple program tagging
       former_xs,
       program_slot_number,
       course_length_weeks
FROM
       course_program_count cpc
JOIN
       production.production.d_program_course dpc
ON
       cpc.course_id = dpc.course_id
AND
       cpc.id = dpc.id
LEFT JOIN
       finance.production.base_course_title bct
ON
       cpc.course_id_key=bct.course_id_key
WINDOW
       catalog_partition AS (PARTITION BY dpc.catalog_course ORDER BY cpc.id*-1)
       )
SELECT
       course_id_key,
       program_type_key,
       program_title_key,
       program_type,
       program_title,
       course_id,
       -- program_type_micromasters_launch,
       former_xs,
       program_slot_number,
       MAX(program_slot_number) OVER PROGRAM_PARTITION courses_in_program,
       AVG(course_length_weeks) OVER PROGRAM_PARTITION avg_course_in_program_length_weeks
FROM
       program_titles
WINDOW
       PROGRAM_PARTITION AS (PARTITION BY program_type_key,program_title_key);
DROP TABLE IF EXISTS finance.production.base_program_courses CASCADE;
ALTER TABLE finance.production.base_program_courses_swap RENAME TO finance.production.base_program_courses;


TRUNCATE TABLE finance.production.base_course_exams;
INSERT /*+ direct */ INTO finance.production.base_course_exams
SELECT
       HASH(course_id) AS course_id_key,
       course_id,
       COUNT(DISTINCT pp.id) AS unique_exams,
       SUM(is_proctored::INT) AS exams_proctored,
       SUM(is_proctored::INT)*10 AS exam_fees, --$10 per exam
       SUM(is_practice_exam::INT) AS exams_proctored_practice
FROM
       app_data.wwc.proctoring_proctoredexam pp
JOIN
       app_data.wwc.proctoring_proctoredexamsoftwaresecurereview pps --exam level
ON
       pp.id = pps.exam_id
GROUP BY
       1,2;


-- ************ LOOK HERE ************
-- THIS CAN RETIRE if all references to base_course_price switch to dim_course_run
-- ************           ************
TRUNCATE TABLE finance.production.base_course_price;
INSERT /*+ direct */ INTO finance.production.base_course_price
SELECT
       hash(course_id) AS course_id_key,
       course_id,
       course_seat_price
FROM
       production.production.d_course_seat
WHERE
       regexp_like(course_seat_type,'verified|prof','bi')
GROUP BY
       1,2,3;


COMMIT;