select user_id, user_type, full_agent, count(*) as n, month
from
( -- Extract full agent
  select
    json_extract(raw_event, '$.properties.context.agent') as full_agent,
    user_id, user_type, month
  from
  ( -- Look up type of learner
    select
      raw_event, user_id,
      -- Aggregate at month / year level
      concat(
        cast(extract(year from date) as string), '-',
        cast(extract(month from date) as string)
      ) as month,
      case when ee.lms_user_id is null then 'consumer' else 'enterprise' end as user_type
    from `edx-data-eng-experiments.events.json_event_records` as er
    left join (
      select distinct cast(lms_user_id as string) as lms_user_id
      from `edx-data-warehouse.business_intelligence.enterprise_enrollment`
     ) as ee
    on er.user_id=ee.lms_user_id
    where
    -- Use 4 different 1-week periods so we can compare browser share over time
    -- Each of these periods starts on a Sunday and ends on a Saturday
      _PARTITIONTIME between '2018-12-02' and '2018-12-08'
      or _PARTITIONTIME between '2018-06-03' and '2018-06-09'
      or _PARTITIONTIME between '2017-12-03' and '2017-12-09'
      or _PARTITIONTIME between '2017-06-04' and '2017-06-10'
  )
)
where full_agent is not null
group by user_id, user_type, full_agent, month
order by month, user_id, n desc;
