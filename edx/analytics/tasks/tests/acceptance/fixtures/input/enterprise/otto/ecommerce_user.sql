--
-- Table structure for table `ecommerce_user`
--

DROP TABLE IF EXISTS `ecommerce_user`;
CREATE TABLE `ecommerce_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `password` varchar(128) NOT NULL,
  `last_login` datetime DEFAULT NULL,
  `is_superuser` tinyint(1) NOT NULL,
  `username` varchar(30) NOT NULL,
  `first_name` varchar(30) NOT NULL,
  `last_name` varchar(30) NOT NULL,
  `email` varchar(254) NOT NULL,
  `is_staff` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `date_joined` datetime NOT NULL,
  `tracking_context` longtext,
  `full_name` varchar(255),
  `lms_user_id` int(11),
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `ecommerce_user`
--

INSERT INTO `ecommerce_user` VALUES
  (39,'0000000000000000000000000000000000000000','2015-09-11 16:38:05.627130',0,'test_user','Test','User','test@example.com',0,0,'2015-04-02 16:38:09','{\"lms_user_id\":11,\"lms_client_id\":\"0000000000.0000000000\"}','Test User',11),
  (40,'0000000000000000000000000000000000000000','2015-09-11 16:38:05.627130',0,'test_user2','Test','User2','test2@example.com',0,0,'2015-04-02 16:38:09','{\"lms_user_id\":12,\"lms_client_id\":\"0000000000.0000000000\"}','Test User2',12),
  (41,'0000000000000000000000000000000000000000','2015-09-11 16:38:05.627130',0,'test_user3','Test','User3','test3@example.com',0,0,'2015-04-02 16:38:09','{\"lms_user_id\":13,\"lms_client_id\":\"0000000000.0000000000\"}','Test User3',13),
  (42,'0000000000000000000000000000000000000000','2015-09-11 16:38:05.627130',0,'test_user4','Test','User4','test4@example.com',0,0,'2015-04-02 16:38:09','{\"lms_user_id\":14,\"lms_client_id\":\"0000000000.0000000000\"}','Test User4',14),
  (43,'0000000000000000000000000000000000000000','2015-09-11 16:38:05.627130',0,'test_user5','Test','User5','test5@example.com',0,0,'2015-04-02 16:38:09','{\"lms_user_id\":15,\"lms_client_id\":\"0000000000.0000000000\"}','Test User5',15);
