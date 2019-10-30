--
-- Table structure for table `enterprise_enterprisecourseenrollment`
--

DROP TABLE IF EXISTS `enterprise_enterprisecourseenrollment`;
CREATE TABLE `enterprise_enterprisecourseenrollment` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  `course_id` varchar(255) NOT NULL,
  `enterprise_customer_user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `enterprise_customeruser_course` (`enterprise_customer_user_id`,`course_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `enterprise_enterprisecourseenrollment`
--

INSERT INTO `enterprise_enterprisecourseenrollment` VALUES
  (1,'2014-06-27 16:02:38','2014-06-27 16:02:38','edX/Open_DemoX/edx_demo_course',1),
  (2,'2016-03-22 20:56:09.320098','2016-03-22 20:56:09.320098','course-v1:edX+Open_DemoX+edx_demo_course2',1),
  (3,'2016-03-22 20:59:12.281696','2016-03-22 20:59:12.281696','course-v1:edX+Open_DemoX+edx_demo_course2',2),
  (4,'2016-03-22 21:02:09.195616','2016-03-22 21:02:09.195616','course-v1:edX+Open_DemoX+edx_demo_course2',3),
  (5,'2016-03-22 21:04:08.211237','2016-03-22 21:04:08.211237','course-v1:edX+Testing102x+1T2017',2),
  (6,'2016-03-22 21:08:08.432870','2016-03-22 21:08:08.432870','course-v1:edX+Testing102x+1T2017',1),
  (7,'2014-06-27 16:02:38','2014-06-27 16:02:38','edX/Open_DemoX/edx_demo_course',4),
  (8,'2016-03-22 20:56:09.320098','2016-03-22 20:56:09.320098','course-v1:edX+Open_DemoX+edx_demo_course2',4),
  (9,'2016-03-22 21:08:08.432870','2016-03-22 21:08:08.432870','course-v1:edX+Testing102x+1T2017',4),
  (10,'2019-09-04 21:08:08.432870','2019-09-04 21:08:08.432870','course-v1:edX+Testing102x+1T2017',5),
  (11,'2016-03-22 20:56:09.320098','2016-03-22 20:56:09.320098','course-v1:edX+Open_DemoX+edx_demo_course3',4),
  (12,'2020-03-22 21:08:08.432870','2020-03-25 21:08:08.432870','course-v1:edX+Testing102x+1T2020',6),
  (13,'2020-09-04 21:08:08.432870','2020-09-10 21:08:08.432870','course-v1:edX+Testing102x+1T2020',7);
