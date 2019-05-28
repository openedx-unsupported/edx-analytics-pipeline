SELECT user_id, user_type, full_agent, count(*) AS n, month
FROM
( -- Extract full agent
  SELECT
    raw_event:properties:context:agent AS full_agent,
    user_id, user_type, month
  FROM
  ( -- Look up type of learner
    SELECT
      raw_event, user_id,
      -- Aggregate at month / year level
      CONCAT(
        EXTRACT('year', date)::string, '-',
        EXTRACT('month', date)::string
      ) AS month,
      CASE WHEN ee.lms_user_id IS NULL THEN 'consumer' ELSE 'enterprise' END AS user_type
    FROM events.events.json_event_records AS er
    LEFT OUTER JOIN (
      SELECT distinct lms_user_id::string AS lms_user_id
      FROM business_intelligence.production.enterprise_enrollment
     ) AS ee
    ON er.user_id=ee.lms_user_id
    WHERE
    -- Use 4 different 1-week periods so we can compare browser share over time
    -- Each of these periods starts on a Sunday and ends on a Saturday
      received_at between '2018-12-02' and '2018-12-08'
      OR received_at between '2018-06-03' and '2018-06-09'
      OR received_at between '2017-12-03' and '2017-12-09'
      OR received_at between '2017-06-04' and '2017-06-10'
  )
)
WHERE full_agent IS NOT NULL
GROUP BY user_id, user_type, full_agent, month
ORDER BY month, user_id, n DESC;