DROP TABLE IF EXISTS `auth_user`;

CREATE TABLE `auth_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(30) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(30) NOT NULL,
  `email` varchar(75) NOT NULL,
  `password` varchar(128) NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `last_login` datetime NOT NULL,
  `date_joined` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

INSERT INTO `auth_user` VALUES
    (1,'jane','','','jane@example.com','no_password',0,1,0,'2014-01-18 00:00:00','2014-01-15 00:00:00'),
    (2,'alex','','','alex@example.com','no_password',0,1,0,'2014-02-18 00:00:00','2014-02-15 00:00:00'),
    (3,'cary','','','cary@example.com','no_password',0,1,0,'2014-03-18 00:00:00','2014-03-15 00:00:00'),
    (4,'erin','','','erin@example.com','no_password',1,1,0,'2014-04-18 00:00:00','2014-04-15 00:00:00'),
    (5,'lane','','','lane@example.com','no_password',0,1,0,'2014-05-18 00:00:00','2014-05-15 00:00:00');

DROP TABLE IF EXISTS `auth_userprofile`;

CREATE TABLE `auth_userprofile` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
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
  `bio` varchar(3000),
  `profile_image_uploaded_at` datetime,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  KEY `auth_userprofile_52094d6e` (`name`),
  KEY `auth_userprofile_8a7ac9ab` (`language`),
  KEY `auth_userprofile_b54954de` (`location`),
  KEY `auth_userprofile_fca3d292` (`gender`),
  KEY `auth_userprofile_d85587` (`year_of_birth`),
  KEY `auth_userprofile_551e365c` (`level_of_education`),
  CONSTRAINT `user_id_refs_id_628b4c11` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

INSERT INTO `auth_userprofile`
  (`user_id`, `name`, `gender`, `year_of_birth`, `level_of_education`, `language`, `location`, `meta`, `courseware`, `allow_certificate`)
VALUES
  (1, 'Jane Doe', 'f', 1980, 'b', '', '', '', 'course.xml', 1),
  (2, 'Alex Doe', '',  1990, '',  '', '', '', 'course.xml', 1),
  (3, 'Cary Doe', 'm', 1985, 'hs','', '', '', 'course.xml', 1),
  (4, 'Erin Doe', 'f', 1995, '',  '', '', '', 'course.xml', 1),
  (5, 'Lane Doe', 'm', NULL, 'm', '', '', '', 'course.xml', 1);