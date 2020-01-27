--
-- Table structure for table `auth_user`
--

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
  `status` varchar(2) NOT NULL,
  `email_key` varchar(32) DEFAULT NULL,
  `avatar_type` varchar(1) NOT NULL,
  `country` varchar(2) NOT NULL,
  `show_country` tinyint(1) NOT NULL,
  `date_of_birth` date DEFAULT NULL,
  `interesting_tags` longtext NOT NULL,
  `ignored_tags` longtext NOT NULL,
  `email_tag_filter_strategy` smallint(6) NOT NULL,
  `display_tag_filter_strategy` smallint(6) NOT NULL,
  `consecutive_days_visit_count` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `auth_user`
--

INSERT INTO `auth_user` VALUES
  (11,'test_user','','','test@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2015-09-09 15:03:48','2015-02-12 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (12,'test_user2','','','test2@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2015-09-09 15:03:48','2015-02-12 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (13,'test_user3','','','test3@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2015-09-09 15:03:48','2015-02-12 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (14,'test_user4','','','test4@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2015-09-09 15:03:48','2015-02-12 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (15,'test_user5','','','test5@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2015-09-09 15:03:48','2015-02-12 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (16,'test_user6','','','test6@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2019-09-04 15:03:48','2019-09-03 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (17,'test_user7','','','test7@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2015-09-09 15:03:48','2015-02-12 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0),
  (18,'test_user8','','','test8@example.com','0000000000000000000000000000000000000000000000000000000000000000000000000000',0,1,0,'2019-09-04 15:03:48','2019-09-03 23:14:35','w',NULL,'n','',0,NULL,'','',1,0,0);
