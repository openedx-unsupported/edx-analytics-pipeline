--
-- Table structure for table `student_courseenrollment`
--
DROP TABLE IF EXISTS `student_courseenrollment`;

CREATE TABLE `student_courseenrollment` (`id` int(11) NOT NULL AUTO_INCREMENT,`user_id` int(11) NOT NULL,`course_id` varchar(255) NOT NULL,`created` datetime DEFAULT NULL,`is_active` tinyint(1) NOT NULL,`mode` varchar(100) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `student_courseenrollment`
--

INSERT INTO `student_courseenrollment` VALUES (1,1,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:38',0,'honor'),(2,2,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:40',1,'verified'),(3,3,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:41',1,'verified'),(4,4,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:43',0,'honor'),(5,5,'edX/Open_DemoX/edx_demo_course','2014-06-30 21:19:04',1,'honor'),(6,6,'edX/Open_DemoX/edx_demo_course','2014-06-30 21:24:13',1,'verified'),(7,4,'course-v1:edX+Open_DemoX+edx_demo_course2','2014-06-27 16:02:44',1,'honor'),(8,5,'course-v1:edX+Open_DemoX+edx_demo_course2','2014-06-28 16:02:45',1,'audit');
