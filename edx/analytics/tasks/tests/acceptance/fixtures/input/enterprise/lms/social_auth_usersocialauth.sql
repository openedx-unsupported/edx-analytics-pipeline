--
-- Table structure for table `social_auth_usersocialauth`
--
DROP TABLE IF EXISTS `social_auth_usersocialauth`;

CREATE TABLE `social_auth_usersocialauth` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `provider` varchar(32) NOT NULL,
  `uid` varchar(255) NOT NULL,
  `extra_data` longtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `provider` (`provider`,`uid`),
  KEY `social_auth_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `social_auth_usersocialauth`
--
INSERT INTO `social_auth_usersocialauth` VALUES
  (1,11,'tpa-saml','ent1:harry', '{}'),
  (2,12,'tpa-saml','ent1:ron', '{}'),
  (3,13,'tpa-saml','ent2:ron', '{}'),
  (4,13,'tpa-saml','ent2:hermione', '{}'),
  (5,15,'tpa-saml','ent1:ginny', '{}'),
  (6,16,'tpa-saml','ent1:dory', '{}'),
  (7,17,'tpa-saml','ent1:luther', '{}'),
  (8,18,'tpa-saml','ent2:knight', '{}');
