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
INSERT INTO `student_courseenrollment` VALUES (1,11,'DemoX/DemoCourse/1T2015','2014-06-27 16:02:38',1,'verified');
