DROP TABLE IF EXISTS `grades_persistentcoursegrade`;

CREATE TABLE `grades_persistentcoursegrade` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `course_id` varchar(255) NOT NULL,
  `course_edited_timestamp` datetime(6) DEFAULT NULL,
  `course_version` varchar(255) NOT NULL,
  `grading_policy_hash` varchar(255) NOT NULL,
  `percent_grade` double NOT NULL,
  `letter_grade` varchar(255) NOT NULL,
  `passed_timestamp` datetime(6),
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  PRIMARY KEY (`id`)
);

INSERT INTO `grades_persistentcoursegrade` (
  `user_id`, `course_id`, `course_version`, `grading_policy_hash`, `percent_grade`, `letter_grade`, `passed_timestamp`, `created`, `modified`
)
VALUES
(
  1, 'edX/Open_DemoX/edx_demo_course', 'version-1', 'grading-policy-1', 0.7, 'C', '2017-01-31T00:05:00', '2017-02-01T00:00:00', '2017-02-01T00:00:00'
),
(
  2, 'edX/Open_DemoX/edx_demo_course', 'version-1', 'grading-policy-1', 0.8, 'B', '2017-01-31T00:05:00', '2017-02-01T00:00:00', '2017-02-01T00:00:00'
),
(
  3, 'edX/Open_DemoX/edx_demo_course', 'version-1', 'grading-policy-1', 0.2, 'Fail', NULL, '2017-02-01T00:00:00', '2017-02-01T00:00:00'
),
(
  4, 'edX/Open_DemoX/edx_demo_course', 'version-1', 'grading-policy-1', 0, '', '2017-01-31T00:05:00', '2017-02-01T00:00:00', '2017-02-01T00:00:00'
);
