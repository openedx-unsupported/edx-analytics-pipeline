DROP TABLE IF EXISTS `student_courseenrollment`;

CREATE TABLE `student_courseenrollment` (`id` int(11) NOT NULL AUTO_INCREMENT,`user_id` int(11) NOT NULL,`course_id` varchar(255) NOT NULL,`created` datetime DEFAULT NULL,`is_active` tinyint(1) NOT NULL,`mode` varchar(100) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;

INSERT INTO `student_courseenrollment` VALUES (1,1,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:38',0,'honor'),(2,2,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:40',1,'audit'),(3,3,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:41',1,'verified'),(4,4,'edX/Open_DemoX/edx_demo_course','2014-06-27 16:02:43',1,'honor'),(5,4,'course-v1:edX+Open_DemoX+edx_demo_course2','2014-06-27 16:02:43',1,'honor');
