CREATE TABLE `auth_userprofile` ( `id` int(11) NOT NULL, `user_id` int(11) NOT NULL, `name` varchar(255) NOT NULL, `language` varchar(255) NOT NULL, `location` varchar(255) NOT NULL, `meta` longtext NOT NULL, `courseware` varchar(255) NOT NULL, `gender` varchar(6), `mailing_address` longtext, `year_of_birth` int(11), `level_of_education` varchar(6), `goals` longtext, `allow_certificate` tinyint(1) NOT NULL, `country` varchar(2), `city` longtext, PRIMARY KEY (`id`), UNIQUE KEY `user_id` (`user_id`) );

INSERT INTO `auth_userprofile` VALUES (1,1,'honor','','','','course.xml','m',NULL,1984,'a',NULL,1,'',NULL);
INSERT INTO `auth_userprofile` VALUES (2,2,'audit','','','','course.xml','m',NULL,1975,'b',NULL,1,'',NULL);
INSERT INTO `auth_userprofile` VALUES (3,3,'verified','','','','course.xml','',NULL,2000,'b',NULL,1,'',NULL);
INSERT INTO `auth_userprofile` VALUES (4,4,'staff','','','','course.xml',NULL,NULL,2000,'',NULL,1,'',NULL);