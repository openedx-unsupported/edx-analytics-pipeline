--
-- Table structure for table `student_courseenrollment`
--

DROP TABLE IF EXISTS `grades_persistentcoursegrade`;
CREATE TABLE `grades_persistentcoursegrade` (
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `course_id` varchar(255) NOT NULL,
  `course_edited_timestamp` datetime(6) DEFAULT NULL,
  `course_version` varchar(255) NOT NULL,
  `grading_policy_hash` varchar(255) NOT NULL,
  `percent_grade` double NOT NULL,
  `letter_grade` varchar(255) NOT NULL,
  `passed_timestamp` datetime(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `course_user` (`course_id`,`user_id`),
  KEY `user_id` (`user_id`),
  KEY `passed_time` (`passed_timestamp`,`course_id`),
  KEY `modified_time` (`modified`,`course_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `grades_persistentcoursegrade`
--

INSERT INTO `grades_persistentcoursegrade` VALUES
  ('2017-05-09 16:27:33.421413','2017-07-06 16:42:00.613812',1,11,'edX/Open_DemoX/edx_demo_course','2017-05-30 08:06:58.485000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.81,'Pass','2017-05-09 16:27:34.690065'),
  ('2017-05-09 16:27:33.519065','2017-07-22 09:24:36.149447',2,11,'course-v1:edX+Open_DemoX+edx_demo_course2','2017-06-09 14:39:18.871000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.4,'',NULL),
  ('2017-05-09 16:27:34.047921','2017-08-09 17:19:11.281575',3,12,'course-v1:edX+Open_DemoX+edx_demo_course2','2017-07-18 22:07:23.462000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',0,'',NULL),
  ('2017-05-09 16:27:34.090088','2017-06-21 08:26:19.534269',4,13,'course-v1:edX+Open_DemoX+edx_demo_course2','2017-06-06 07:50:15.303000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.03,'',NULL),
  ('2017-05-09 16:27:34.396107','2017-06-06 15:15:35.496627',5,12,'course-v1:edX+Testing102x+1T2017','2017-06-05 12:03:25.067000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.98,'Pass','2017-05-09 16:27:33.526363'),
  ('2017-05-09 16:27:34.460666','2017-09-22 01:15:51.524507',6,11,'course-v1:edX+Testing102x+1T2017','2017-09-21 19:57:46.390000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.64,'',NULL),
  ('2017-05-09 16:27:34.661796','2017-05-24 07:22:04.981837',7,14,'course-v1:edX+Testing102x+1T2017','2017-05-19 23:08:02.344000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.75,'Pass','2017-05-09 16:27:34.690065'),
  ('2017-05-09 16:27:33.421413','2017-07-06 16:42:00.613812',8,15,'edX/Open_DemoX/edx_demo_course','2017-05-30 08:06:58.485000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.85,'Pass','2017-05-09 16:27:34.690065'),
  ('2017-05-09 16:27:33.519065','2017-07-22 09:24:36.149447',9,15,'course-v1:edX+Open_DemoX+edx_demo_course2','2017-06-09 14:39:18.871000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.5,'',NULL),
  ('2017-05-09 16:27:34.460666','2017-09-22 01:15:51.524507',10,15,'course-v1:edX+Testing102x+1T2017','2017-09-21 19:57:46.390000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.74,'',NULL),
  ('2019-09-04 16:27:34.470666','2019-09-05 01:15:51.544507',11,16,'course-v1:edX+Testing102x+1T2017','2017-09-21 19:57:46.380000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.3,'','2019-09-04 16:27:34.690065'),
  ('2017-05-09 16:27:33.519065','2017-07-22 09:24:36.149447',12,15,'course-v1:edX+Open_DemoX+edx_demo_course3','2017-06-09 14:39:18.871000','592d2822c7d5444623e4fde6','Lgr54dD6fJnWX1psNWfej2bF9CU=',.5,'',NULL);
