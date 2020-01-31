--
-- Table structure for table `student_courseenrollment`
--
DROP TABLE IF EXISTS `student_courseenrollment`;

CREATE TABLE `student_courseenrollment` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `course_id` varchar(255) NOT NULL,
  `created` datetime DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL,
  `mode` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `student_courseenrollment`
--
INSERT INTO `student_courseenrollment` VALUES
  (1,11,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:38',1,'verified'),
  (25,11,'course-v1:edX+Open_DemoX+edx_demo_course2','2016-03-22 20:56:09.320098',1,'verified'),
  (26,12,'course-v1:edX+Open_DemoX+edx_demo_course2','2016-03-22 20:59:12.281696',1,'verified'),
  (27,13,'course-v1:edX+Open_DemoX+edx_demo_course2','2016-03-22 21:02:09.195616',1,'no-id-professional'),
  (28,12,'course-v1:edX+Testing102x+1T2017','2016-03-22 21:04:08.211237',1,'no-id-professional'),
  (29,11,'course-v1:edX+Testing102x+1T2017','2016-03-22 21:08:08.432870',1,'credit'),
  (30,14,'course-v1:edX+Testing102x+1T2017','2016-03-22 21:08:08.432870',1,'credit'),
  (31,15,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:38',1,'verified'),
  (32,15,'course-v1:edX+Open_DemoX+edx_demo_course2','2016-03-22 20:56:09.320098',1,'verified'),
  (33,15,'course-v1:edX+Testing102x+1T2017','2016-03-22 21:08:08.432870',1,'credit'),
  (34,16,'course-v1:edX+Testing102x+1T2017','2019-03-22 20:56:09.320098',1,'verified'),
  (35,15,'course-v1:edX+Open_DemoX+edx_demo_course3','2016-03-22 20:56:09.320098',1,'verified');
