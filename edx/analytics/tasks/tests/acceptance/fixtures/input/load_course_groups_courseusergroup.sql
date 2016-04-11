CREATE TABLE `course_groups_courseusergroup` (`id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(255) NOT NULL, `course_id` varchar(255) NOT NULL, `group_type` varchar(20) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `name` (`name`,`course_id`), KEY `course_groups_courseusergroup_ff48d8e5` (`course_id`)) ENGINE=InnoDB AUTO_INCREMENT=527 DEFAULT CHARSET=utf8;

INSERT INTO `course_groups_courseusergroup` VALUES (1,'Group 1','edX/DemoX/Demo_Course','cohort'),(2,'Group 2','edX/DemoX/Demo_Course_2','cohort'),(3,'Group 3','course-v1:edX+DemoX+Demo_Course_2015','cohort');
