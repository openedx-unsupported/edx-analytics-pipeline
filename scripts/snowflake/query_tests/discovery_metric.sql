WITH
first_user_course_engagement AS (
  --the date of each users first course engagement for all their engaged enrollments
  SELECT activity_date as first_course_engagement_date, user_id, course_id, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY activity_date) AS course_order
  FROM (
    --getting the order of engaged site visits per user course
    SELECT date as activity_date, user_id, course_id, ROW_NUMBER() OVER(PARTITION BY user_id, course_id ORDER BY date) AS activity_order
    FROM BUSINESS_INTELLIGENCE.PRODUCTION.ACTIVITY_ENGAGEMENT_USER_DAILY
    WHERE is_engaged = 1
    ) A
  WHERE activity_order = 1 --and user_id = 262 -- unit testing
),
conversion_date AS (
  SELECT first_course_engagement_date, user_id
  FROM first_user_course_engagement
  WHERE course_order = 1
),
conversion_count AS (
SELECT first_course_engagement_date, count(*) AS conversions
FROM conversion_date
GROUP BY first_course_engagement_date
ORDER BY first_course_engagement_date DESC
),
unregistered_visits AS (
  --gets the visits each day from visitors who never logged in
  SELECT date, fullVisitorId FROM (
  SELECT
    date, fullVisitorId, MAX(user_id) AS user_id
    FROM (
      SELECT
        raw_json:date::string as date, raw_json:fullVisitorId::string as fullVisitorId, TRY_CAST(( cd.value:value::string ) AS NUMBER) AS user_id
      FROM
        GA_EVENTS.RAW_EVENTS.GA_SESSIONS
        , lateral flatten( input => RAW_JSON:customDimensions ) cd
	    where cd.value:index::number = 40
          AND raw_json:totals.pageviews::number >0
 --         AND raw_json:date::string = '20181224'
    ) T0
    --combine the visitor sessions for the day
    GROUP BY date, fullVisitorId
    ) T1
    --now remove the visitors with some user id attached
    WHERE user_id IS null
),
count_unregistered_visits AS (
  --daily count of unregistered visits
  SELECT date, count(*) AS daily_unregistered_visits
  FROM unregistered_visits
  GROUP BY date
), -- WITH
logged_in_visits AS (
  -- gets the visits each day from users who logged in
  SELECT
    date, user_id
  FROM (        -- select * from demo_db.public.testabdul
   SELECT raw_json:date as datenew,
      raw_json:date::string as date, raw_json:fullVisitorId::string as fullVisitorId, TRY_CAST(( cd.value:value::string ) AS NUMBER) AS user_id
    FROM
        GA_EVENTS.RAW_EVENTS.GA_SESSIONS
        , lateral flatten( input => RAW_JSON:customDimensions ) cd
      where cd.value:index::number = 40
          AND raw_json:totals.pageviews::number >0
    --      AND raw_json:date::string = '20180422'
  ) T0
  WHERE user_id IS not null
  GROUP BY date, user_id
),
logged_in_nonconverting_visits AS (
  --gets visits from users who logged in but haven't had an active enrollment yet
  SELECT logged_in_visits.user_id, date, first_course_engagement_date
  FROM logged_in_visits
  LEFT JOIN
  conversion_date
  ON
  conversion_date.user_id = logged_in_visits.user_id
  WHERE (first_course_engagement_date IS null OR first_course_engagement_date >= TO_DATE(date, 'YYYYMMDD'))
),
count_registered_nonconverting_visits AS (
  SELECT date, count(*) as daily_registered_nonconverting_visits
  FROM logged_in_nonconverting_visits
  GROUP BY date
),
convertible_count AS (
  SELECT TO_DATE( COALESCE( CRNV.date, CUV.date ), 'YYYYMMDD' ) AS date, daily_registered_nonconverting_visits, daily_unregistered_visits
  , daily_registered_nonconverting_visits + daily_unregistered_visits AS convertible_visits
  FROM
  count_registered_nonconverting_visits CRNV
  FULL OUTER JOIN
  count_unregistered_visits CUV
  ON CUV.date = CRNV.date
)
SELECT COALESCE( first_course_engagement_date, date) AS date, conversions, convertible_visits
, daily_registered_nonconverting_visits, daily_unregistered_visits, conversions/convertible_visits AS conversion_rate
FROM convertible_count
FULL OUTER JOIN
conversion_count
ON conversion_count.first_course_engagement_date = convertible_count.date
ORDER BY COALESCE(first_course_engagement_date, date) DESC;
