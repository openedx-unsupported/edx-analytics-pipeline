

--
-- Table structure for table `auth_user`
--

DROP TABLE IF EXISTS `auth_user`;
CREATE TABLE `auth_user` (`id` int(11) NOT NULL AUTO_INCREMENT, `username` varchar(30) NOT NULL, `first_name` varchar(30) NOT NULL, `last_name` varchar(30) NOT NULL, `email` varchar(75) NOT NULL, `password` varchar(128) NOT NULL, `is_staff` tinyint(1) NOT NULL, `is_active` tinyint(1) NOT NULL, `is_superuser` tinyint(1) NOT NULL, `last_login` datetime NOT NULL, `date_joined` datetime NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `username` (`username`), UNIQUE KEY `email` (`email`)) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `auth_user`
--

LOCK TABLES `auth_user` WRITE;
INSERT INTO `auth_user` VALUES (1,'honor','','','honor@example.com','pbkdf2_sha256$10000$t2YjuAM6IhLH$Sx+NEm3f2ABjxGqLd9PGPwA+SxOM7FX8x7aP0wQ9XRQ=',0,1,0,'2015-04-16 21:02:32','2014-11-19 04:06:46'),(2,'audit','','','audit@example.com','pbkdf2_sha256$10000$kCoPFRHEytwO$NRIu3+xsyI3FwIKDhrJQIvcx5/IBGF7ev5vlt3XCcaI=',0,1,0,'2014-11-19 04:06:49','2014-11-19 04:06:49'),(3,'verified','','','verified@example.com','pbkdf2_sha256$10000$XjlPhSQMJoHM$O10EF9vKSaXgMZxeL/o/HuRQC1ncO/UEYW47iyF1eZw=',0,1,0,'2014-11-19 04:06:52','2014-11-19 04:06:52'),(4,'staff','','','staff@example.com','pbkdf2_sha256$10000$fepf8MOW3Ds5$5AAnU1sMOlbV2P9nWJl65wcK+iAs2700Egk0T3Jtdp4=',1,1,0,'2015-04-13 18:29:49','2014-11-19 04:06:54');
UNLOCK TABLES;

--
-- Table structure for table `course_groups_courseusergroup`
--

DROP TABLE IF EXISTS `course_groups_courseusergroup`;
CREATE TABLE `course_groups_courseusergroup` (`id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(255) NOT NULL, `course_id` varchar(255) NOT NULL, `group_type` varchar(20) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `name` (`name`,`course_id`), KEY `course_groups_courseusergroup_ff48d8e5` (`course_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;


--
-- Dumping data for table `course_groups_courseusergroup`
--

LOCK TABLES `course_groups_courseusergroup` WRITE;
INSERT INTO `course_groups_courseusergroup` VALUES (1, 'best-cohort', 'edX/DemoX/Demo_Course', 'cohort'), (2, 'other-cohort', 'edX/DemoX/Demo_Course', 'cohort'), (3, 'new-cohort', 'course-v1:edX+DemoX+Demo_Course_2015', 'cohort'), (4, 'additional-cohort', 'course-v1:edX+DemoX+Demo_Course_2015', 'cohort');

UNLOCK TABLES;

--
-- Table structure for table `course_groups_courseusergroup_users`
--

DROP TABLE IF EXISTS `course_groups_courseusergroup_users`;
CREATE TABLE `course_groups_courseusergroup_users` (`id` INT(11) NOT NULL AUTO_INCREMENT, `courseusergroup_id` INT(11) NOT NULL, `user_id` INT(11) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `courseusergroup_id` (`courseusergroup_id`, `user_id`), KEY `course_groups_courseusergroup_users_caee1c64` (`courseusergroup_id`), KEY `course_groups_courseusergroup_users_fbfc09f1` (`user_id`), CONSTRAINT `courseusergroup_id_refs_id_d26180aa` FOREIGN KEY (`courseusergroup_id`) REFERENCES `course_groups_courseusergroup` (`id`), CONSTRAINT `user_id_refs_id_bf33b47a` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `course_groups_courseusergroup_users`
--

LOCK TABLES `course_groups_courseusergroup_users` WRITE;
INSERT INTO `course_groups_courseusergroup_users` VALUES (1, 1, 1), (2, 1, 2), (3, 2, 4), (4, 3, 2), (5, 4, 3);
UNLOCK TABLES;
