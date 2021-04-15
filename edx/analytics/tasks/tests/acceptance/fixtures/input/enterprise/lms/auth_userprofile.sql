--
-- Table structure for table `auth_user`
--

DROP TABLE IF EXISTS `auth_userprofile`;
CREATE TABLE `auth_userprofile` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `meta` longtext NOT NULL,
  `courseware` varchar(255) NOT NULL,
  `language` varchar(255) NOT NULL,
  `location` varchar(255) NOT NULL,
  `year_of_birth` int(11) DEFAULT NULL,
  `gender` varchar(6) DEFAULT NULL,
  `level_of_education` varchar(6) DEFAULT NULL,
  `mailing_address` longtext,
  `city` longtext,
  `country` varchar(2) DEFAULT NULL,
  `goals` longtext,
  `bio` varchar(3000) DEFAULT NULL,
  `profile_image_uploaded_at` datetime(6) DEFAULT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  KEY `auth_userprofile_b068931c` (`name`),
  KEY `auth_userprofile_8512ae7d` (`language`),
  KEY `auth_userprofile_d5189de0` (`location`),
  KEY `auth_userprofile_8939d49d` (`year_of_birth`),
  KEY `auth_userprofile_cc90f191` (`gender`),
  KEY `auth_userprofile_a895faa8` (`level_of_education`),
  CONSTRAINT `auth_userprofile_user_id_62634b27_fk` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8;


--
-- Dumping data for table `auth_userprofile`
--
INSERT INTO `auth_userprofile` VALUES
    (5, 'Test User', '', 'course.xml', '', '', 1989, 'm', '', '', '', 'US', '', NULL, NULL, 11),
    (6, 'Test User2', '', 'course.xml', '', '', 1989, 'f', '', '', '', 'US', '', NULL, NULL, 12),
    (7, 'Test User3', '', 'course.xml', '', '', 1989, 'm', '', '', '', 'US', '', NULL, NULL, 13),
    (8, 'Test User4', '', 'course.xml', '', '', 1989, 'f', '', '', '', 'US', '', NULL, NULL, 14),
    (9, 'Test User5', '', 'course.xml', '', '', 1989, 'f', '', '', '', 'US', '', NULL, NULL, 15),
    (10, 'Test User6', '', 'course.xml', '', '', 1989, 'f', '', '', '', 'US', '', NULL, NULL, 16),
    (11, 'Test User7', '', 'course.xml', '', '', 1989, 'f', '', '', '', 'US', '', NULL, NULL, 17),
    (12, 'Test User8', '', 'course.xml', '', '', 1989, 'f', '', '', '', 'US', '', NULL, NULL, 18);
