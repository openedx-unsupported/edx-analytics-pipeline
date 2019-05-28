select
  enrollment.date,
  registration.registration_count,
  enrollment.enrollment_count,
  enrollment.enrollment_is_engaged_count,
  enrollment.enrollment_is_complete_count,
  enrollment.enrollment_is_verified_count,
  engaged_enrollment.engaged_enrollment_count,
  verified_enrollment.verified_enrollment_count,
  completion.completion_count,
  payment.deposit_count,
  payment.refund_count,
  payment.deposit_amount,
  payment.refund_amount,
  payment.deposit_count_verification,
  payment.refund_count_verification,
  payment.deposit_amount_verification,
  payment.refund_amount_verification
from (
    select uc.first_enrollment_time::date date,
      count(1) enrollment_count,
      sum(case when engagement.user_id is not null then 1 else 0 end) enrollment_is_engaged_count,
      sum(case when completion.user_id is not null then 1 else 0 end) enrollment_is_complete_count,
      sum(case when uc.first_verified_enrollment_time is not null then 1 else 0 end) enrollment_is_verified_count
    from production.d_user_course uc
    join production.d_course c
      on c.course_id = uc.course_id
    left outer join (
        select distinct user_id, course_id
        from business_intelligence.activity_engagement_user_daily
        where is_engaged = 1
      ) engagement
      on  engagement.user_id = uc.user_id
      and engagement.course_id = uc.course_id
    left outer join (
        select distinct user_id, course_id
        from business_intelligence.course_completion_user
        where passed_timestamp is not null
      ) completion
      on  completion.user_id = uc.user_id
      and completion.course_id = uc.course_id
    where lower(c.org_id) not in ('edx')
      and c.partner_short_code = 'edx'
      and uc.user_id::varchar || '|' || uc.course_id not in (    -- exclude enterprise enrollments
          select enterprise_user.user_id::varchar || '|' || enroll.course_id
          from lms_read_replica.enterprise_enterprisecourseenrollment enroll
          join lms_read_replica.enterprise_enterprisecustomeruser enterprise_user
            on enroll.enterprise_customer_user_id = enterprise_user.id
        )
   group by 1
  ) enrollment
left outer join (
    select first_engaged_date date, count(1) engaged_enrollment_count
    from (
        select uaed.user_id, uaed.course_id, min(uaed.date) first_engaged_date
        from business_intelligence.activity_engagement_user_daily uaed
        join production.d_course c
          on c.course_id = uaed.course_id
        where uaed.is_engaged = 1
          and lower(c.org_id) not in ('edx')
          and c.partner_short_code = 'edx'
          and uaed.user_id::varchar || '|' || uaed.course_id not in (    -- exclude enterprise enrollments
              select enterprise_user.user_id::varchar || '|' || enroll.course_id
              from lms_read_replica.enterprise_enterprisecourseenrollment enroll
              join lms_read_replica.enterprise_enterprisecustomeruser enterprise_user
                on enroll.enterprise_customer_user_id = enterprise_user.id
            )
        group by 1, 2
      ) temp
    group by 1
  ) engaged_enrollment
  on engaged_enrollment.date = enrollment.date
left outer join (
    select uc.first_verified_enrollment_time::date date, count(1) verified_enrollment_count
    from production.d_user_course uc
    join production.d_course c
      on c.course_id = uc.course_id
    where uc.first_verified_enrollment_time is not null
      and lower(c.org_id) not in ('edx')
      and c.partner_short_code = 'edx'
      and uc.user_id::varchar || '|' || uc.course_id not in (    -- exclude enterprise enrollments
          select enterprise_user.user_id::varchar || '|' || enroll.course_id
          from lms_read_replica.enterprise_enterprisecourseenrollment enroll
          join lms_read_replica.enterprise_enterprisecustomeruser enterprise_user
            on enroll.enterprise_customer_user_id = enterprise_user.id
        )
    group by 1
  ) verified_enrollment
  on verified_enrollment.date = enrollment.date
left outer join (
    select bt.payment_date date,
      sum(case when bt.transaction_amount_per_item > 0 then 1 else 0 end) deposit_count,
      sum(case when bt.transaction_amount_per_item < 0 then 1 else 0 end) refund_count,
      sum(case when bt.transaction_amount_per_item > 0 then bt.transaction_amount_per_item else 0 end) deposit_amount,
      sum(case when bt.transaction_amount_per_item < 0 then -bt.transaction_amount_per_item else 0 end) refund_amount,
      sum(case when bt.order_product_class in ('seat', 'course-entitlement') and bt.transaction_amount_per_item > 0
        then 1 else 0 end) deposit_count_verification,
      sum(case when bt.order_product_class in ('seat', 'course-entitlement') and bt.transaction_amount_per_item < 0
        then 1 else 0 end) refund_count_verification,
      sum(case when bt.order_product_class in ('seat', 'course-entitlement') and bt.transaction_amount_per_item > 0
        then bt.transaction_amount_per_item else 0 end) deposit_amount_verification,
      sum(case when bt.order_product_class in ('seat', 'course-entitlement') and bt.transaction_amount_per_item < 0
        then -bt.transaction_amount_per_item else 0 end) refund_amount_verification
    from finance.base_transactions bt
    where bt.white_label is false
      and (bt.user_id::varchar || '|' || bt.order_course_id not in (    -- exclude enterprise enrollments
          select enterprise_user.user_id::varchar || '|' || enroll.course_id
          from lms_read_replica.enterprise_enterprisecourseenrollment enroll
          join lms_read_replica.enterprise_enterprisecustomeruser enterprise_user
            on enroll.enterprise_customer_user_id = enterprise_user.id
        ) or bt.order_course_id is null)
    group by 1
  ) payment
  on payment.date = enrollment.date
left outer join (
    select ccu.passed_timestamp::date date, count(1) completion_count
    from business_intelligence.course_completion_user ccu
    join production.d_course c
      on c.course_id = ccu.course_id
    where lower(c.org_id) not in ('edx')
      and c.partner_short_code = 'edx'
      and ccu.user_id::varchar || '|' || ccu.course_id not in (    -- exclude enterprise enrollments
          select enterprise_user.user_id::varchar || '|' || enroll.course_id
          from lms_read_replica.enterprise_enterprisecourseenrollment enroll
          join lms_read_replica.enterprise_enterprisecustomeruser enterprise_user
            on enroll.enterprise_customer_user_id = enterprise_user.id
        )
    group by 1
  ) completion
  on completion.date = enrollment.date
left outer join (
    select user_account_creation_time::date date, count(1) registration_count
    from production.d_user
    where user_id not in (    -- exclude whitelabel-only users
        select user_id
        from lms_read_replica.student_userattribute
        where name = 'created_on_site'
          and not (value = 'edx.org' or value like '%.edx.org')
          and user_id not in (
              select user_id
              from production.d_user_course uc
              join production.d_course c
                on c.course_id = uc.course_id
              where c.partner_short_code = 'edx'
            )
      )
    group by 1
  ) registration
  on registration.date = enrollment.date;
