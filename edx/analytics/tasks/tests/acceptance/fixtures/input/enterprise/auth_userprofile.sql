--
-- Table structure for table `auth_userprofile`
--

DROP TABLE IF EXISTS `auth_userprofile`;
CREATE TABLE `auth_userprofile` (
    `id` int(11) NOT NULL,
    `user_id` int(11) NOT NULL,
    `name` varchar(255) NOT NULL,
    `language` varchar(255) NOT NULL,
    `location` varchar(255) NOT NULL,
    `meta` longtext NOT NULL,
    `courseware` varchar(255) NOT NULL,
    `gender` varchar(6),
    `mailing_address` longtext,
    `year_of_birth` int(11),
    `level_of_education` varchar(6),
    `goals` longtext,
    `allow_certificate` tinyint(1) NOT NULL,
    `country` varchar(2),
    `city` longtext,
    PRIMARY KEY (`id`),
    UNIQUE KEY `user_id` (`user_id`)
);

--
-- Dumping data for table `auth_userprofile`
--

INSERT INTO `auth_userprofile` VALUES
  (1,11,'honor','','Europe','','course.xml','m',NULL,1984,'a',NULL,1,'ES', 'Madrid'),
  (2,12,'audit','','','','course.xml','m',NULL,1975,'b',NULL,1,'',NULL),
  (3,13,'verified','','','','course.xml','f',NULL,2000,'b',NULL,1,'',NULL),
  (4,14,'not_enterprise','','','','course.xml','f',NULL,2000,'b',NULL,1,'',NULL);
