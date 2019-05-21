-- This SQL script creates a database, so needs higher privileges.
--USE ROLE ACCOUNTADMIN;

-- Create a new DB/schema/role for this test.
DROP SCHEMA IF EXISTS TEST_ACTIVITY_ENGAGE_TIME CASCADE;
CREATE SCHEMA IF NOT EXISTS TEST_ACTIVITY_ENGAGE_TIME GRANT USAGE, CREATE ON SCHEMA TEST_ACTIVITY_ENGAGE_TIME TO public;
-- USE SCHEMA TEST_ACTIVITY_ENGAGE_TIME;



CREATE TABLE IF NOT EXISTS TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily (
        date DATE,
        user_id INTEGER,
        course_id VARCHAR(255),
        is_active INTEGER,
        cnt_active_activity INTEGER,
        is_engaged INTEGER,
        cnt_engaged_activity INTEGER,
        is_engaged_video INTEGER,
        cnt_video_activity INTEGER,
        is_engaged_problem INTEGER,
        cnt_problem_activity INTEGER,
        is_engaged_forum INTEGER,
        cnt_forum_activity INTEGER
);
-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--dummy insert to initialize the table

INSERT INTO TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily (date)
SELECT
    '2012-08-31'
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily
HAVING
    COUNT(date)=0;

--daily record of what a user did in any given course on any given day

INSERT INTO TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily (

    SELECT
        date,
        user_id,
        course_id,
        SUM(CASE WHEN activity_type = 'ACTIVE' THEN 1 ELSE 0 END) AS is_active,
        SUM(CASE WHEN activity_type = 'ACTIVE' THEN number_of_activities ELSE 0 END) AS cnt_active_activity,
        CASE
            WHEN SUM(CASE WHEN activity_type IN ('PLAYED_VIDEO', 'ATTEMPTED_PROBLEM', 'POSTED_FORUM') THEN 1 ELSE 0 END) > 0 THEN 1
        ELSE 0
        END AS is_engaged,
        SUM(CASE WHEN activity_type IN ('PLAYED_VIDEO', 'ATTEMPTED_PROBLEM', 'POSTED_FORUM') THEN number_of_activities ELSE 0 END) AS cnt_engaged_activity,
        SUM(CASE WHEN activity_type = 'PLAYED_VIDEO' THEN 1 ELSE 0 END) AS is_engaged_video,
        SUM(CASE WHEN activity_type = 'PLAYED_VIDEO' THEN number_of_activities ELSE 0 END) AS cnt_video_activity,
        SUM(CASE WHEN activity_type = 'ATTEMPTED_PROBLEM' THEN 1 ELSE 0 END) AS is_engaged_problem,
        SUM(CASE WHEN activity_type = 'ATTEMPTED_PROBLEM' THEN number_of_activities ELSE 0 END) AS cnt_problem_activity,
        SUM(CASE WHEN activity_type = 'POSTED_FORUM' THEN 1 ELSE 0 END) AS is_engaged_forum,
        SUM(CASE WHEN activity_type = 'POSTED_FORUM' THEN number_of_activities ELSE 0 END) AS cnt_forum_activity
    FROM
        production.f_user_activity a
    JOIN
    (
        SELECT
            MAX(date) AS latest_date
        FROM
            TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily
    ) b
    ON
        a.date > b.latest_date
    WHERE a.date BETWEEN '2016-11-21' AND '2018-04-26'
    GROUP BY
        date,
        user_id,
        course_id
);

--rollup of user level view at the course level

CREATE TABLE IF NOT EXISTS TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_course_daily AS

SELECT
    date,
    course_id,
    SUM(is_active) AS cnt_active_users,
    SUM(cnt_active_activity) AS sum_active_activity,
    SUM(is_engaged) AS cnt_engaged_users,
    SUM(cnt_engaged_activity) AS sum_engaged_activity,
    SUM(is_engaged_video) AS cnt_engaged_video_users,
    SUM(cnt_video_activity) AS sum_video_activity,
    SUM(is_engaged_problem) AS cnt_engaged_problem_users,
    SUM(cnt_problem_activity) AS sum_problem_activity,
    SUM(is_engaged_forum) AS cnt_engaged_forum_users,
    SUM(cnt_forum_activity) AS sum_forum_activity
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily
GROUP BY
    date,
    course_id;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_course_daily SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_course_daily TO ROLE SNOWFLAKE_EVALUATION_ROLE;


--initialize the eligible users table

CREATE TABLE IF NOT EXISTS TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users (
        date DATE,
        user_id INTEGER,
        course_id VARCHAR(255),
        content_availability_date DATE,
        first_enrollment_date DATE,
        last_unenrollment_date DATE,
        course_pass_date DATE,
        days_from_content_availability INTEGER,
        week VARCHAR(255)
);

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--insert earliest activity date to initialize table

INSERT INTO TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users (date)

SELECT
    activity.min_date_activity
FROM
(
    SELECT
        MIN(date) AS min_date_users
    FROM
        TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users
) users
FULL OUTER JOIN
(
    SELECT
        MIN(date) AS min_date_activity
    FROM
        production.f_user_activity
) activity
ON
    1=1
WHERE
    min_date_users IS NULL;


--identify the new users that we want to start tracking for engagement

CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS tmp_activity_engagement_eligible_users ON COMMIT PRESERVE ROWS AS

SELECT
    content_availability.user_id,
    content_availability.course_id,
    content_availability.content_availability_date,
    DATE_TRUNC('DAY', content_availability.first_enrollment_time) AS first_enrollment_date,
    DATE_TRUNC('DAY', content_availability.last_unenrollment_time) AS last_unenrollment_date,
    CASE
        WHEN cert.has_passed = 1 THEN cert.modified_date::TIMESTAMP
        ELSE NULL
    END AS course_pass_date,
    course.course_start_date
FROM
    business_intelligence.user_content_availability_date content_availability
JOIN
    business_intelligence.course_master course
ON
    content_availability.course_id = course.course_id
AND
    content_availability.content_availability_date BETWEEN CURRENT_DATE()-7 AND CURRENT_DATE()
LEFT JOIN
(
    SELECT
        user_id,
        course_id
    FROM
       business_intelligence.user_activity_engagement_eligible_users
    WHERE
        days_from_content_availability = 0
    AND
        date BETWEEN TIMESTAMPADD(day, -14, '4-26-2018'::TIMESTAMP) AND '4-26-2018'::TIMESTAMP
    GROUP BY
        user_id,
        course_id
) latest
ON
    content_availability.user_id = latest.user_id
AND
    content_availability.course_id = latest.course_id
LEFT JOIN
    production.d_user_course_certificate cert
ON
    content_availability.user_id = cert.user_id
AND
    content_availability.course_id = cert.course_id
WHERE
    latest.user_id IS NULL
;


--add the new users from above into the master tracking table
--add new dates for all users who are within 35 days of content availability

INSERT INTO TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users (

    SELECT
        cal.date,
        users.user_id,
        users.course_id,
        users.content_availability_date,
        users.first_enrollment_date,
        users.last_unenrollment_date,
        users.course_pass_date,
        DATEDIFF('day', users.content_availability_date, cal.date) AS days_from_content_availability,
        CASE
            WHEN DATEDIFF('day', users.content_availability_date, cal.date) BETWEEN 0 AND 6 THEN 'week_1'
            WHEN DATEDIFF('day', users.content_availability_date, cal.date) BETWEEN 7 AND 13 THEN 'week_2'
            WHEN DATEDIFF('day', users.content_availability_date, cal.date) BETWEEN 14 AND 20 THEN 'week_3'
            WHEN DATEDIFF('day', users.content_availability_date, cal.date) BETWEEN 21 AND 27 THEN 'week_4'
            WHEN DATEDIFF('day', users.content_availability_date, cal.date) BETWEEN 28 AND 34 THEN 'week_5'
            ELSE 'week_5'
        END AS week
    FROM
    (
        SELECT
            user_id,
            course_id,
            content_availability_date,
            first_enrollment_date,
            last_unenrollment_date,
            course_pass_date,
            content_availability_date AS latest_date
        FROM
            tmp_activity_engagement_eligible_users

        UNION ALL

        SELECT
            user_id,
            course_id,
            content_availability_date,
            first_enrollment_date,
            last_unenrollment_date,
            course_pass_date,
            MAX(date) + 1 AS latest_date
        FROM
            business_intelligence.user_activity_engagement_eligible_users
        GROUP BY
            user_id,
            course_id,
            content_availability_date,
            first_enrollment_date,
            last_unenrollment_date,
            course_pass_date
        HAVING
            DATEDIFF('day', content_availability_date, MAX(date) + 1) BETWEEN 0 AND 34
    ) users
    JOIN
        business_intelligence.calendar cal
    ON
        cal.date BETWEEN users.latest_date AND CURRENT_DATE()
    AND
        DATEDIFF('day', users.content_availability_date, cal.date) BETWEEN 0 AND 34
);


--map each (day, user_id, course_id) to their activity for that day
--remove users from calculation as soon as they unenroll or complete
--choosing to do this over the entire table everyday in case we add new engagement actions that we would like to backfill

CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS tmp_user_activity_engagement_daily ON COMMIT PRESERVE ROWS AS

SELECT
    users.date,
    users.user_id,
    users.course_id,
    users.content_availability_date,
    users.first_enrollment_date,
    users.last_unenrollment_date,
    users.course_pass_date,
    users.days_from_content_availability,
    users.week,
    COALESCE(activity.is_active, 0) AS is_active,
    COALESCE(activity.cnt_active_activity, 0) AS cnt_active_activity,
    COALESCE(activity.is_engaged, 0) AS is_engaged,
    COALESCE(activity.cnt_engaged_activity, 0) AS cnt_engaged_activity,
    COALESCE(activity.is_engaged_video, 0) AS is_engaged_video,
    COALESCE(activity.cnt_video_activity, 0) AS cnt_video_activity,
    COALESCE(activity.is_engaged_problem, 0) AS is_engaged_problem,
    COALESCE(activity.cnt_problem_activity, 0) AS cnt_problem_activity,
    COALESCE(activity.is_engaged_forum, 0) AS is_engaged_forum,
    COALESCE(activity.cnt_forum_activity, 0) AS cnt_forum_activity
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users users
LEFT JOIN
    TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily activity
ON
    users.user_id = activity.user_id
AND
    users.course_id = activity.course_id
AND
    users.date = activity.date
WHERE
(
    users.date <= users.last_unenrollment_date
    OR
    users.last_unenrollment_date IS NULL
)
AND
(
    users.date <= users.course_pass_date
    OR
    users.course_pass_date IS NULL
)
;


--daily rollup per (user_id, course_id)

CREATE TABLE IF NOT EXISTS TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily AS

SELECT
    date,
    user_id,
    course_id,
    week,
    is_active,
    cnt_active_activity,
    is_engaged,
    cnt_engaged_activity,
    is_engaged_video,
    cnt_video_activity,
    is_engaged_problem,
    cnt_problem_activity,
    is_engaged_forum,
    cnt_forum_activity,
    CASE
        WHEN SUM(is_engaged) OVER (PARTITION BY user_id, course_id, week ORDER BY date) = 0 THEN 'no_engagement'
        WHEN SUM(is_engaged) OVER (PARTITION BY user_id, course_id, week ORDER BY date) = 1 THEN 'minimal_engagement'
        ELSE 'high_engagement'
    END AS weekly_engagement_level
FROM
    tmp_user_activity_engagement_daily
GROUP BY
    date,
    user_id,
    course_id,
    week,
    is_active,
    cnt_active_activity,
    is_engaged,
    cnt_engaged_activity,
    is_engaged_video,
    cnt_video_activity,
    is_engaged_problem,
    cnt_problem_activity,
    is_engaged_forum,
    cnt_forum_activity;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--weekly rollup per (user_id, course_id)

CREATE TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly AS
SELECT
    user_id,
    course_id,
    week,
    SUM(is_engaged) AS days_engaged,
    SUM(is_active) AS days_active,
    CASE
        WHEN SUM(is_engaged) = 0 THEN 'no_engagement'
        WHEN SUM(is_engaged) = 1 THEN 'minimal_engagement'
        ELSE 'high_engagement'
    END AS weekly_engagement_level
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily
GROUP BY
    user_id,
    course_id,
    week;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--aggregated daily rollup

CREATE TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily_agg AS
SELECT
    daily.date,
    daily.course_id,
    daily.week,
    daily.weekly_engagement_level,
    week1.weekly_engagement_level AS week_1_engagement_level,
    COUNT(1) AS cnt_users
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily daily
JOIN
    TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly week1
ON
    daily.user_id = week1.user_id
AND
    daily.course_id = week1.course_id
AND
    week1.week = 'week_1'
GROUP BY
    daily.date,
    daily.course_id,
    daily.week,
    daily.weekly_engagement_level,
    week1.weekly_engagement_level;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily_agg SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily_agg TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--aggregated weekly rollup

CREATE TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly_agg AS
SELECT
    summary.course_id,
    summary.week,
    summary.weekly_engagement_level,
    week1.weekly_engagement_level AS week_1_engagement_level,
    COUNT(1) AS cnt_users
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly summary
JOIN
    (
        SELECT
            user_id,
            course_id,
            weekly_engagement_level
        FROM
            TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly
        WHERE
            week = 'week_1'
    ) week1
ON
    summary.user_id = week1.user_id
AND
    summary.course_id = week1.course_id
GROUP BY
    summary.course_id,
    summary.week,
    summary.weekly_engagement_level,
    week1.weekly_engagement_level;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly_agg SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly_agg TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--Beginning of Daily Course and Course Level Week 1 Engagement Tables
--Temp Table for Engagement Eligible Users by Date and Course_id
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS tmp_daily_eligible ON COMMIT PRESERVE ROWS AS
SELECT
    date,
    course_id,
    COUNT(DISTINCT user_id) AS week_1_cnt_engagement_eligible_users
FROM
    (
        SELECT
            MAX(date) AS date,
            user_id,
            course_id
        FROM
            TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily
        WHERE
            week = 'week_1'
        GROUP BY
            user_id,
            course_id
    ) engagement_eligible
GROUP BY
    date,
    course_id
;


--Temp table for Engaged Users by Date and Course_id
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS tmp_daily_engaged ON COMMIT PRESERVE ROWS AS
SELECT
    date,
    course_id,
    COUNT(DISTINCT user_id) AS week_1_cnt_engaged_users
FROM
    (
        SELECT
            MAX(date) AS date,
            user_id,
            course_id
        FROM
            TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily
        WHERE
            week = 'week_1'
        AND
            weekly_engagement_level IN ('minimal_engagement', 'high_engagement')
        GROUP BY
            user_id,
            course_id
    ) engaged
GROUP BY
    date,
    course_id
;


--Temp table for High Engaged Users by Date and Course_id
CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS tmp_daily_high_engaged ON COMMIT PRESERVE ROWS AS
SELECT
    date,
    course_id,
    COUNT(DISTINCT user_id) AS week_1_cnt_high_engaged_users
FROM
    (
        SELECT
            MAX(date) AS date,
            user_id,
            course_id
        FROM
            TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily
        WHERE
            week = 'week_1'
        AND
            weekly_engagement_level = 'high_engagement'
        GROUP BY
            user_id,
            course_id
    ) high_engaged
GROUP BY
    date,
    course_id
;



--Combine all Tables into One Master Table
CREATE TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_daily AS
SELECT
    cal.date,
    eligible.course_id,
    eligible.week_1_cnt_engagement_eligible_users,
    COALESCE(engaged.week_1_cnt_engaged_users, 0) AS week_1_cnt_engaged_users,
    COALESCE(high_engaged.week_1_cnt_high_engaged_users, 0) AS week_1_cnt_high_engaged_users,
    COALESCE(engaged.week_1_cnt_engaged_users, 0) - COALESCE(high_engaged.week_1_cnt_high_engaged_users, 0) AS week_1_cnt_minimal_engaged_users
FROM
    business_intelligence.calendar cal
LEFT JOIN
    tmp_daily_eligible eligible
ON
    cal.date = eligible.date
LEFT JOIN
    tmp_daily_engaged engaged
ON
    cal.date = engaged.date
AND
    eligible.course_id = engaged.course_id
LEFT JOIN
    tmp_daily_high_engaged high_engaged
ON
    cal.date = high_engaged.date
AND
    eligible.course_id = high_engaged.course_id;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_daily SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_daily TO ROLE SNOWFLAKE_EVALUATION_ROLE;

--Course Level Engagement Summary Table
CREATE TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_summary AS
SELECT
    a.course_id,
    SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        ) AS week_1_cnt_engaged_users,
    SUM(
        CASE
            WHEN weekly_engagement_level = 'high_engagement' THEN cnt_users
            ELSE 0 END
        ) AS week_1_cnt_high_engaged_users,
    SUM(
        CASE
            WHEN weekly_engagement_level = 'minimal_engagement' THEN cnt_users
            ELSE 0 END
        ) AS week_1_cnt_minimal_engaged_users,
    SUM(cnt_users) AS week_1_cnt_engagement_eligible_users,
    SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        ) AS week_1_cnt_users,
    COALESCE(week_2_cnt_engaged_users, 0) AS week_2_cnt_engaged_users,
        SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        )/(CASE WHEN SUM(cnt_users) = 0 THEN 1 ELSE SUM(cnt_users) END) AS week_1_engagement_rate,
    SUM(
        CASE
            WHEN weekly_engagement_level = 'high_engagement' THEN cnt_users
            ELSE 0 END
        )/(CASE WHEN SUM(cnt_users) = 0 THEN 1 ELSE SUM(cnt_users) END) AS week_1_high_engagement_rate,
    SUM(
        CASE
            WHEN weekly_engagement_level = 'minimal_engagement' THEN cnt_users
            ELSE 0 END
        )/(CASE WHEN SUM(cnt_users) = 0 THEN 1 ELSE SUM(cnt_users) END) AS week_1_minimal_engagement_rate,
    SUM(
        CASE
            WHEN weekly_engagement_level = 'high_engagement' THEN cnt_users
            ELSE 0 END
        )/(CASE WHEN SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        ) = 0 THEN 1 ELSE SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        ) END) AS week_1_return_rate,
    COALESCE(week_2_cnt_engaged_users, 0)/(CASE WHEN SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        ) = 0 THEN 1 ELSE SUM(
        CASE
            WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
            ELSE 0 END
        ) END) AS week_1_retention_rate
FROM
    TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly_agg a
LEFT JOIN
    (
        SELECT
            course_id,
            SUM(
                CASE
                    WHEN weekly_engagement_level IN ('minimal_engagement', 'high_engagement') THEN cnt_users
                    ELSE 0 END
            ) AS week_2_cnt_engaged_users
        FROM
            TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly_agg
        WHERE
            week_1_engagement_level IN ('minimal_engagement', 'high_engagement')
        AND
            week = 'week_2'
        GROUP BY
            course_id
    ) b
ON
    a.course_id = b.course_id
WHERE
    week = 'week_1'
GROUP BY
    a.course_id,
    week_2_cnt_engaged_users;
-- ALTER TABLE TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_summary SET COMMENT='activity_engagement_time.sql test';

-- GRANT SELECT ON TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_summary TO ROLE SNOWFLAKE_EVALUATION_ROLE;

DROP TABLE IF EXISTS tmp_activity_engagement_eligible_users;
DROP TABLE IF EXISTS tmp_user_activity_engagement_daily;
DROP TABLE IF EXISTS tmp_daily_eligible;
DROP TABLE IF EXISTS tmp_daily_engaged;
DROP TABLE IF EXISTS tmp_daily_high_engaged;

SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_user_daily;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_activity_engagement_course_daily;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_eligible_users;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_daily_agg;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_user_activity_engagement_weekly_agg;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_daily;
SELECT COUNT(*) FROM TEST_ACTIVITY_ENGAGE_TIME.tt_course_week_1_engagement_summary;


