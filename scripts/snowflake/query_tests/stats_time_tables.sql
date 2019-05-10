--Script to pull enrollments, verifications, and bookings over time at the course_id level
--Daily Enrolls by course_id
DROP TABLE IF EXISTS production.production.tmp_course_enrolls_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_course_enrolls_daily ON COMMIT PRESERVE ROWS AS
SELECT
    first_enrollment_time::date AS enroll_date,
    course_id,
    COUNT(*) AS cnt_enrolls
FROM
    production.production.d_user_course
GROUP BY
    enroll_date,
    course_id;


--Daily Unenrolls by course_id
DROP TABLE IF EXISTS production.production.tmp_course_unenrolls_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_course_unenrolls_daily ON COMMIT PRESERVE ROWS AS
SELECT
    last_unenrollment_time::date AS unenroll_date,
    course_id,
    COUNT(last_unenrollment_time) AS cnt_unenrolls
FROM
    production.production.d_user_course
WHERE
    last_unenrollment_time IS NOT NULL
GROUP BY
    unenroll_date,
    course_id;


--Daily Verifications by course_id
DROP TABLE IF EXISTS production.production.tmp_course_verifications_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_course_verifications_daily ON COMMIT PRESERVE ROWS AS
SELECT
    first_verified_enrollment_time::date AS verification_date,
    course_id,
    COUNT(first_verified_enrollment_time) AS cnt_verifications
FROM
    production.production.d_user_course
WHERE
    first_verified_enrollment_time IS NOT NULL
GROUP BY
    verification_date,
    course_id;


--Daily Transactions by course_id
DROP TABLE IF EXISTS finance.production.tmp_course_transactions_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS finance.production.tmp_course_transactions_daily ON COMMIT PRESERVE ROWS AS
SELECT
    transaction_date::date AS transaction_date,
    order_course_id AS course_id,
    SUM(CASE
            WHEN order_product_class IN ('seat','course-entitlement') AND transaction_type = 'sale' THEN 1
            ELSE 0
        END) AS cnt_paid_enrollments,
    SUM(transaction_amount_per_item) AS sum_bookings,
    SUM(CASE
            WHEN order_product_class = 'seat' THEN transaction_amount_per_item
            ELSE 0
        END) AS sum_seat_bookings,
    SUM(CASE
            WHEN order_product_class = 'donation' THEN transaction_amount_per_item
            ELSE 0
        END) AS sum_donations_bookings,
    SUM(CASE
            WHEN order_product_class = 'reg-code' THEN transaction_amount_per_item
            ELSE 0
        END) AS sum_reg_code_bookings
FROM
    finance.production.f_orderitem_transactions
WHERE
    order_id IS NOT NULL
AND
    transaction_date IS NOT NULL
GROUP BY
    transaction_date,
    order_course_id;


--Daily Enrolls for VTR Calculation by course_id
DROP TABLE IF EXISTS production.production.tmp_course_enrolls_vtr_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_course_enrolls_vtr_daily ON COMMIT PRESERVE ROWS AS
SELECT
    c.first_enrollment_time::date AS enroll_date_vtr,
    c.course_id,
    s.course_seat_price,
    SUM(CASE
            WHEN s.course_seat_upgrade_deadline IS NULL THEN 1
            WHEN c.first_enrollment_time <= s.course_seat_upgrade_deadline THEN 1
            ELSE 0
        END) AS cnt_enrolls_vtr
FROM
    production.production.d_user_course c
LEFT JOIN
    production.production.d_course_seat s
ON
    c.course_id = s.course_id
AND
    s.course_seat_type = 'verified'
GROUP BY
    enroll_date_vtr,
    c.course_id,
    s.course_seat_price;


--Daily Certificates by course_id
DROP TABLE IF EXISTS production.production.tmp_course_certs_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_course_certs_daily ON COMMIT PRESERVE ROWS AS
SELECT
    modified_date::date AS cert_date,
    cert.course_id,
    COUNT(*) AS cnt_certificates
FROM
    production.production.d_user_course_certificate cert
JOIN
    production.production.d_user_course user_course
ON
    cert.user_id = user_course.user_id
AND
    cert.course_id = user_course.course_id
AND
    user_course.current_enrollment_mode NOT IN ('honor', 'audit')
AND
    cert.is_certified = 1
GROUP BY
    cert_date,
    cert.course_id;


--Daily Completions by course_id
DROP TABLE IF EXISTS business_intelligence.production.tmp_course_completions_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS business_intelligence.production.tmp_course_completions_daily ON COMMIT PRESERVE ROWS AS
SELECT
    passed_timestamp::date AS completion_date,
    course_id,
    COUNT(*) AS cnt_completions
FROM
    business_intelligence.production.course_completion_user
WHERE
    passed_timestamp IS NOT NULL
GROUP BY
    completion_date,
    course_id;


--Combining Completions and Certificates Data here
DROP TABLE IF EXISTS business_intelligence.production.tmp_course_certs_completions_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS business_intelligence.production.tmp_course_certs_completions_daily ON COMMIT PRESERVE ROWS AS
SELECT
    COALESCE(
        completions.completion_date,
        certificates.cert_date
    ) AS date,
    COALESCE(
        completions.course_id,
        certificates.course_id
    ) AS course_id,
    cnt_completions,
    cnt_certificates
FROM
    business_intelligence.production.tmp_course_completions_daily completions
FULL JOIN
    production.production.tmp_course_certs_daily certificates
ON
    completions.completion_date = certificates.cert_date
AND
    completions.course_id = certificates.course_id;


--Daily VTR by course_id
DROP TABLE IF EXISTS production.production.tmp_course_vtr_daily;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_course_vtr_daily ON COMMIT PRESERVE ROWS AS
SELECT
    COALESCE(
        enrolls.enroll_date_vtr,
        verifications.verification_date
    ) AS vtr_date,
    COALESCE(
        enrolls.course_id,
        verifications.course_id
    ) AS vtr_course_id,
    cnt_enrolls_vtr,
    cnt_verifications,
    cnt_verifications/NULLIF(cnt_enrolls_vtr,0) AS daily_course_vtr,
    course_seat_price
FROM
    production.production.tmp_course_enrolls_vtr_daily enrolls
FULL JOIN
    production.production.tmp_course_verifications_daily verifications
ON
    enrolls.enroll_date_vtr = verifications.verification_date
AND
    enrolls.course_id = verifications.course_id
GROUP BY
    vtr_date,
    vtr_course_id,
    cnt_enrolls_vtr,
    cnt_verifications,
    course_seat_price;


--Creating a staging table for course_stats_time
DROP TABLE IF EXISTS production.production.pre_course_stats_time;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.pre_course_stats_time ON COMMIT PRESERVE ROWS AS
SELECT
    COALESCE(
        vtr.vtr_date,
        enrolls.enroll_date,
        unenrolls.unenroll_date
    ) AS date,
    COALESCE(
        vtr_course_id,
        enrolls.course_id,
        unenrolls.course_id
    ) AS course_id,
    cnt_enrolls_vtr,
    cnt_verifications,
    daily_course_vtr,
    cnt_enrolls,
    cnt_unenrolls,
    course_seat_price
FROM
    production.production.tmp_course_vtr_daily vtr
FULL JOIN
    production.production.tmp_course_enrolls_daily enrolls
ON
    vtr.vtr_date = enrolls.enroll_date
AND
    vtr.vtr_course_id = enrolls.course_id
FULL JOIN
    production.production.tmp_course_unenrolls_daily unenrolls
ON
    vtr.vtr_date = unenrolls.unenroll_date
AND
    vtr.vtr_course_id = unenrolls.course_id;


--Adding Transactions to summary table
DROP TABLE IF EXISTS production.production.pre_trans_course_stats_time;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.pre_trans_course_stats_time ON COMMIT PRESERVE ROWS AS
SELECT
    COALESCE(
        pre_cst.date,
        transactions.transaction_date
    ) AS date,
    COALESCE(
        pre_cst.course_id,
        transactions.course_id
    ) AS course_id,
    cnt_enrolls_vtr,
    cnt_verifications,
    daily_course_vtr,
    cnt_enrolls,
    cnt_unenrolls,
    course_seat_price,
    cnt_paid_enrollments,
    sum_bookings,
    sum_seat_bookings,
    sum_donations_bookings,
    sum_reg_code_bookings
FROM
    production.production.pre_course_stats_time pre_cst
FULL JOIN
    finance.production.tmp_course_transactions_daily transactions
ON
    pre_cst.date = transactions.transaction_date
AND
    pre_cst.course_id = transactions.course_id;


--Adding Certificate and Completions data to summary table
DROP TABLE IF EXISTS production.production.pre_calc_course_stats_time;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.pre_calc_course_stats_time ON COMMIT PRESERVE ROWS AS
SELECT
    COALESCE(
        pre_trans_cst.date,
        cert_comp.date
    ) AS date,
    COALESCE(
        pre_trans_cst.course_id,
        cert_comp.course_id
    ) AS course_id,
    cnt_enrolls_vtr,
    cnt_verifications,
    daily_course_vtr,
    cnt_enrolls,
    cnt_unenrolls,
    cnt_paid_enrollments,
    course_seat_price,
    sum_bookings,
    sum_seat_bookings,
    sum_donations_bookings,
    sum_reg_code_bookings,
    cnt_completions,
    cnt_certificates
FROM
    production.production.pre_trans_course_stats_time pre_trans_cst
FULL JOIN
    business_intelligence.production.tmp_course_certs_completions_daily cert_comp
ON
    pre_trans_cst.date = cert_comp.date
AND
    pre_trans_cst.course_id = cert_comp.course_id;


--Adding rolling and running total calculations here
DROP TABLE IF EXISTS business_intelligence.production.course_stats_time;
CREATE TABLE IF NOT EXISTS business_intelligence.production.course_stats_time AS
SELECT
    date,
    course_id,
    cnt_enrolls,
    SUM(cnt_enrolls) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_enrolls,
    cnt_enrolls_vtr,
    SUM(cnt_enrolls_vtr) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_enrolls_vtr,
    cnt_verifications,
    SUM(cnt_verifications) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_verifications,
    daily_course_vtr,
    SUM(cnt_verifications) OVER (PARTITION BY course_id ORDER BY date)/NULLIF(SUM(cnt_enrolls_vtr) OVER (PARTITION BY course_id ORDER BY date),0) AS cum_sum_course_vtr,
    cnt_unenrolls,
    SUM(cnt_unenrolls) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_unenrolls,
    course_seat_price * (SUM(cnt_verifications) OVER (PARTITION BY course_id ORDER BY date)/NULLIF(SUM(cnt_enrolls_vtr) OVER (PARTITION BY course_id ORDER BY date),0)) AS cum_bookings_per_enroll,
    cnt_completions,
    SUM(cnt_completions) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_completions,
    cnt_certificates,
    SUM(cnt_certificates) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_certificates,
    cnt_paid_enrollments,
    SUM(cnt_paid_enrollments) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_paid_enrollments,
    sum_bookings,
    SUM(sum_bookings) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_bookings,
    sum_seat_bookings,
    SUM(sum_seat_bookings) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_seat_bookings,
    sum_donations_bookings,
    SUM(sum_donations_bookings) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_donations_bookings,
    sum_reg_code_bookings,
    SUM(sum_reg_code_bookings) OVER (PARTITION BY course_id ORDER BY date) AS cum_sum_reg_code_bookings
FROM
    production.production.pre_calc_course_stats_time;


GRANT SELECT ON business_intelligence.production.course_stats_time TO automationrole;


--program_stats_time summary table
DROP TABLE IF EXISTS business_intelligence.production.program_stats_time;
CREATE TABLE IF NOT EXISTS business_intelligence.production.program_stats_time AS
SELECT
    date,
    program.course_id,
    program.program_type,
    program.program_title,
    program.org_id,
    cnt_enrolls,
    cum_sum_enrolls,
    cnt_enrolls_vtr,
    cum_sum_enrolls_vtr,
    cnt_verifications,
    cum_sum_verifications,
    daily_course_vtr,
    cum_sum_course_vtr,
    cnt_unenrolls,
    cum_sum_unenrolls,
    cum_bookings_per_enroll,
    cnt_completions,
    cum_sum_completions,
    cnt_certificates,
    cum_sum_certificates,
    cnt_paid_enrollments,
    cum_sum_paid_enrollments,
    sum_bookings,
    cum_sum_bookings,
    sum_seat_bookings,
    cum_sum_seat_bookings,
    sum_donations_bookings,
    cum_sum_donations_bookings,
    sum_reg_code_bookings,
    cum_sum_reg_code_bookings
FROM
    production.production.d_program_course program
LEFT JOIN
    business_intelligence.production.course_stats_time course_stats_time
ON
    program.course_id = course_stats_time.course_id
WHERE
    date IS NOT NULL;

GRANT SELECT ON business_intelligence.production.program_stats_time TO automationrole;


--Create Temp user table for registrations
--This will act as the base table since all users have to be registered to show up on any other tables
DROP TABLE IF EXISTS production.production.tmp_user_reg;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_user_reg ON COMMIT PRESERVE ROWS AS
SELECT
    user_account_creation_time::date AS reg_date,
    user_id,
    COUNT(*) AS registrations
FROM
    production.production.d_user
GROUP BY
    reg_date,
    user_id;


--Create Temp user table for enrollments
DROP TABLE IF EXISTS production.production.tmp_user_enroll;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_user_enroll ON COMMIT PRESERVE ROWS AS
SELECT
    first_enrollment_time::date AS enroll_date,
    user_id,
    COUNT(first_enrollment_time) AS enrolls
FROM
    production.production.d_user_course
GROUP BY
    enroll_date,
    user_id;


--Create Temp user table for unenrolls
DROP TABLE IF EXISTS production.production.tmp_user_unenroll;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_user_unenroll ON COMMIT PRESERVE ROWS AS
SELECT
    last_unenrollment_time::date AS unenroll_date,
    user_id,
    COUNT(last_unenrollment_time) AS unenrolls
FROM
    production.production.d_user_course
WHERE
    last_unenrollment_time IS NOT NULL
GROUP BY
    unenroll_date,
    user_id;


--Create Temp user table for verifications
DROP TABLE IF EXISTS production.production.tmp_user_verifications;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_user_verifications ON COMMIT PRESERVE ROWS AS
SELECT
    first_verified_enrollment_time::date AS verification_date,
    user_id,
    COUNT(first_verified_enrollment_time) AS verifications
FROM
    production.production.d_user_course
WHERE
    first_verified_enrollment_time IS NOT NULL
GROUP BY
    verification_date,
    user_id;


--Create Temp user table for enrollments for VTR calculation
DROP TABLE IF EXISTS production.production.tmp_user_enroll_vtr;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_user_enroll_vtr ON COMMIT PRESERVE ROWS AS
SELECT
    first_enrollment_time::date AS enroll_vtr_date,
    user_id,
    SUM(CASE
            WHEN c.course_seat_upgrade_deadline IS NULL THEN 1
            WHEN first_enrollment_time <= c.course_seat_upgrade_deadline THEN 1
            ELSE 0
        END) AS enrolls_vtr
FROM
    production.production.d_user_course uc
LEFT JOIN
    production.production.d_course_seat c
ON
    uc.course_id = c.course_id
AND
    c.course_seat_type = 'verified'
GROUP BY
    enroll_vtr_date,
    user_id;


--Create Temp user table for certificates
DROP TABLE IF EXISTS production.production.tmp_user_certs;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_user_certs ON COMMIT PRESERVE ROWS AS
SELECT
    modified_date::date AS cert_date,
    user_id,
    COUNT(*) AS certificates
FROM
    production.production.d_user_course_certificate
WHERE
    is_certified = 1
GROUP BY
    cert_date,
    user_id;


--Create Temp user table for completions
DROP TABLE IF EXISTS business_intelligence.production.tmp_user_completions;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS business_intelligence.production.tmp_user_completions ON COMMIT PRESERVE ROWS AS
SELECT
    passed_timestamp::date AS completion_date,
    user_id,
    COUNT(*) AS completions
FROM
    business_intelligence.production.course_completion_user
WHERE
    passed_timestamp IS NOT NULL
GROUP BY
    completion_date,
    user_id;


--Create Temp user table for transactions and bookings
DROP TABLE IF EXISTS finance.production.tmp_user_transactions;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS finance.production.tmp_user_transactions ON COMMIT PRESERVE ROWS AS
SELECT
    transaction_date::date AS transaction_date,
    order_username,
    u.user_id,
    SUM(transaction_amount_per_item) AS bookings,
    SUM(CASE
            WHEN order_product_class = 'seat' THEN transaction_amount_per_item
            ELSE 0
        END) AS seat_bookings,
    SUM(CASE
            WHEN order_product_class = 'donation' THEN transaction_amount_per_item
            ELSE 0
        END) AS donations_bookings,
    SUM(CASE
            WHEN order_product_class = 'reg-code' THEN transaction_amount_per_item
            ELSE 0
        END) AS reg_code_bookings
FROM
    finance.production.f_orderitem_transactions t
LEFT JOIN
    production.production.d_user u
ON
    LOWER(order_username) = LOWER(user_username)
WHERE
    order_id IS NOT NULL
AND
    transaction_date IS NOT NULL
GROUP BY
    transaction_date,
    order_username,
    u.user_id;


--Create Pre Temp summary table to get all user_ids without any nulls and then join cleanly to d_user for final aggregation
DROP TABLE IF EXISTS production.production.tmp_stats_summary_user;
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS production.production.tmp_stats_summary_user ON COMMIT PRESERVE ROWS AS
SELECT
    COALESCE(
        reg_date,
        enroll_date,
        verification_date,
        enroll_vtr_date,
        unenroll_date,
        cert_date,
        transaction_date,
        completion_date
    ) AS absolute_date,
    COALESCE(
        reg.user_id,
        enroll.user_id,
        ver.user_id,
        enroll_vtr.user_id,
        unenroll.user_id,
        cert.user_id,
        trans.user_id,
        completions.user_id
    ) AS user_id,
    registrations,
    enrolls,
    enrolls_vtr,
    verifications,
    unenrolls,
    certificates,
    completions,
    bookings,
    seat_bookings,
    reg_code_bookings,
    donations_bookings
FROM
    production.production.tmp_user_reg reg
FULL JOIN
    production.production.tmp_user_enroll enroll
ON
    reg_date = enroll_date
AND
    reg.user_id = enroll.user_id
FULL JOIN
    production.production.tmp_user_verifications ver
ON
    reg_date = verification_date
AND
    reg.user_id = ver.user_id
FULL JOIN
    production.production.tmp_user_enroll_vtr enroll_vtr
ON
    reg_date = enroll_vtr_date
AND
    reg.user_id = enroll_vtr.user_id
FULL JOIN
    production.production.tmp_user_unenroll unenroll
ON
    reg_date = unenroll_date
AND
    reg.user_id = unenroll.user_id
FULL JOIN
    production.production.tmp_user_certs cert
ON
    reg_date = cert_date
AND
    reg.user_id = cert.user_id
FULL JOIN
    finance.production.tmp_user_transactions trans
ON
    reg_date = transaction_date
AND
    reg.user_id = trans.user_id
FULL JOIN
    business_intelligence.production.tmp_user_completions completions
ON
    reg_date = completions.completion_date
AND
    reg.user_id = completions.user_id;


--Create Summary Table for All Users
DROP TABLE IF EXISTS business_intelligence.production.stats_summary_user;
CREATE TABLE IF NOT EXISTS business_intelligence.production.stats_summary_user AS
SELECT
    absolute_date,
    u.user_last_location_country_code AS user_country_code,
    CASE
        WHEN spanish.country_name IS NOT NULL THEN 'spanish_language_country'
        ELSE 'non_spanish_language_country'
    END AS user_spanish_language_country,
    CASE
        WHEN user_gender IN ('m', 'f') THEN user_gender
        ELSE 'undefined'
    END AS user_gender,
    extract(year from current_date) - user_year_of_birth AS user_age,
    user_level_of_education AS user_education_level,
    SUM(registrations) AS cnt_registrations,
    SUM(enrolls) AS cnt_enrolls,
    SUM(enrolls_vtr) AS cnt_enrolls_vtr,
    SUM(verifications) AS cnt_verifications,
    SUM(unenrolls) AS cnt_unenrolls,
    SUM(certificates) AS cnt_certificates,
    SUM(completions) AS cnt_completions,
    SUM(bookings) AS sum_bookings,
    SUM(seat_bookings) AS sum_seat_bookings,
    SUM(reg_code_bookings) AS sum_reg_code_bookings,
    SUM(donations_bookings) AS sum_donations_bookings
FROM
    production.production.tmp_stats_summary_user summ
LEFT JOIN
    production.production.d_user u
ON
    summ.user_id = u.user_id
LEFT JOIN
    business_intelligence.production.spanish_language_countries spanish
ON
    u.user_last_location_country_code = spanish.user_last_location_country_code
GROUP BY
    absolute_date,
    u.user_last_location_country_code,
    3,
    4,
    user_age,
    user_education_level;

GRANT SELECT ON business_intelligence.production.stats_summary_user TO automationrole;


DROP TABLE IF EXISTS production.production.tmp_course_enrolls_daily;
DROP TABLE IF EXISTS production.production.tmp_course_unenrolls_daily;
DROP TABLE IF EXISTS production.production.tmp_course_verifications_daily;
DROP TABLE IF EXISTS finance.production.tmp_course_transactions_daily;
DROP TABLE IF EXISTS production.production.tmp_course_enrolls_vtr_daily;
DROP TABLE IF EXISTS production.production.tmp_course_certs_daily;
DROP TABLE IF EXISTS business_intelligence.production.tmp_course_completions_daily;
DROP TABLE IF EXISTS business_intelligence.production.tmp_course_certs_completions_daily;
DROP TABLE IF EXISTS production.production.tmp_course_vtr_daily;
DROP TABLE IF EXISTS production.production.pre_course_stats_time;
DROP TABLE IF EXISTS production.production.pre_trans_course_stats_time;
DROP TABLE IF EXISTS production.production.pre_calc_course_stats_time;
DROP TABLE IF EXISTS production.production.tmp_user_reg;
DROP TABLE IF EXISTS production.production.tmp_user_enroll;
DROP TABLE IF EXISTS production.production.tmp_user_unenroll;
DROP TABLE IF EXISTS production.production.tmp_user_verifications;
DROP TABLE IF EXISTS production.production.tmp_user_enroll_vtr;
DROP TABLE IF EXISTS production.production.tmp_user_certs;
DROP TABLE IF EXISTS business_intelligence.production.tmp_user_completions;
DROP TABLE IF EXISTS finance.production.tmp_user_transactions;
DROP TABLE IF EXISTS production.production.tmp_stats_summary_user;