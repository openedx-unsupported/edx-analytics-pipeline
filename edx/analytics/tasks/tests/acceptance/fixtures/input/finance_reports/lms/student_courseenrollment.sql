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
  (1,11,'DemoX/DemoCourse/1T2015','2014-06-27 16:02:38',1,'verified'),
  (25,11,'course-v1:AccTest+Otto+Verified','2016-03-22 20:56:09.320098',0,'verified'),
  (26,12,'course-v1:AccTest+Otto+Verified','2016-03-22 20:59:12.281696',1,'verified'),
  (27,11,'course-v1:AccTest+Otto+Professional','2016-03-22 21:02:09.195616',1,'no-id-professional'),
  (28,12,'course-v1:AccTest+Otto+Professional','2016-03-22 21:04:08.211237',0,'no-id-professional'),
  (29,11,'course-v1:AccTest+Otto+Credit','2016-03-22 21:08:08.432870',1,'credit'),
  (30,11,'course-v1:AccTest+ShoppingCart+Verified','2016-03-22 21:12:57.589154',1,'verified'),
  (31,11,'BerkeleyX/CS-184.1x/2013_October','2017-11-20 16:29:36',1,'verified');
