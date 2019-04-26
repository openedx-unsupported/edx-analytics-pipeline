
-- Google Analytics data uses this date format in the raw_json:date location.
ALTER SESSION SET DATE_INPUT_FORMAT = 'YYYYMMDD';

WITH month_count AS (
-- Count the number of months between now and the beginning of edX-time.
SELECT (DATEDIFF(
  'day',
  DATE_TRUNC('month', '2012-09-01'::date),
  DATEADD('month', 1, CURRENT_DATE)
  ) - 1) AS mc
),
numbers AS (
-- Generate a zero-based sequence of the first 10,000 integers.
SELECT seq4(0) AS num FROM table(generator(rowcount => 10000))
),
numbers_limited AS (
-- Limit the above sequence to the computed number of months.
SELECT num FROM numbers WHERE num < (SELECT mc FROM month_count LIMIT 1)
),
calendar_larger_temporary as (
-- Calculate 30-day interval dates sliding forward one day at a time until the present.
SELECT
  DATE_TRUNC(
    'day',
    DATEADD(
      'day', num,
      DATE_TRUNC('month', '2012-09-06'::date)
    )::TIMESTAMP
  ) AS timestamp_date,
  DATEADD(
    'day', -30,
    DATE_TRUNC(
      'day',
      DATEADD(
        'day', num,
        DATE_TRUNC('month', '2012-09-06'::date)
      )::timestamp
    )
  ) AS timestamp_date_30_interval
FROM
  numbers_limited
ORDER BY 1
),
unique_users AS (
(
  SELECT DATE_TRUNC('day', TO_TIMESTAMP_LTZ(ga.raw_json:visitStartTime)) AS visit_date_est_utc_5,
         COALESCE(du.user_username, du_two.user_username) AS username
  FROM ga_events.raw_events.ga_sessions ga
    JOIN LATERAL FLATTEN(input => ga.raw_json:customDimensions)
       LEFT JOIN app_data.ecommerce.ecommerce_user eu
         ON value:value LIKE 'ecommerce%' AND
            REGEXP_SUBSTR(value:value,'\\d+')::INT = eu.id
       LEFT JOIN production.production.d_user du
         ON eu.username = du.user_username
       LEFT JOIN production.production.d_user du_two
         ON value:value = du_two.user_id
  WHERE ga.raw_json:date::DATE BETWEEN '2017-06-27'::DATE AND DATEADD('day', -1, CURRENT_DATE) AND
        value:index::INT = 40 AND
        COALESCE(du.user_username, du_two.user_username) IS NOT NULL
  GROUP BY 1,2 HAVING SUM(ga.raw_json:totals.pageviews::INT) > 0
)
UNION DISTINCT
(
  SELECT DATE_TRUNC('day', TO_TIMESTAMP_LTZ(timestamp)) visit_date_est_utc_5,
         username
  FROM events.events.json_event_records
  WHERE TO_TIMESTAMP_LTZ(timestamp)::DATE BETWEEN '2017-06-27'::DATE and CURRENT_TIMESTAMP::DATE
        AND TO_TIMESTAMP_LTZ(timestamp)::DATE BETWEEN '2017-06-27'::DATE AND DATEADD('day', -1, CURRENT_DATE)
        AND username IS NOT NULL
  GROUP BY 1,2
)),
daily_users AS (
  SELECT visit_date_est_utc_5, username FROM unique_users
),
daily_user_registrations AS (
  SELECT TO_TIMESTAMP_LTZ(user_account_creation_time)::DATE visit_date_est_utc_5,
         COUNT(user_id) user_registrations
  FROM production.production.d_user
  GROUP BY 1
  ORDER BY 1
)
SELECT dd.timestamp_date,
  COUNT(
    DISTINCT (CASE
      WHEN dd.timestamp_date = ucv.visit_date_est_utc_5 THEN username END)
  ) AS active_users,
  COUNT(
    DISTINCT (CASE
      WHEN ucv.visit_date_est_utc_5 between dd.timestamp_date_30_interval and dd.timestamp_date THEN ucv.username END)
  ) AS active_users_thirty_days,
  MAX(dur.user_registrations) AS user_registrations
FROM calendar_larger_temporary dd
  CROSS JOIN daily_users ucv
  LEFT JOIN daily_user_registrations dur
    ON dd.timestamp_date = dur.visit_date_est_utc_5
WHERE dd.timestamp_date BETWEEN '2017-06-27'::DATE AND DATEADD('day', -1, CURRENT_DATE)
GROUP BY 1
ORDER BY 1;




