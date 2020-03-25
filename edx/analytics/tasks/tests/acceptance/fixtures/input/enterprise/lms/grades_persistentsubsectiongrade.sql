--
-- Table structure for table `grades_persistentsubsectiongrade`
--

CREATE TABLE `grades_persistentsubsectiongrade` (
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `course_id` varchar(255) NOT NULL,
  `usage_key` varchar(255) NOT NULL,
  `subtree_edited_timestamp` datetime(6) DEFAULT NULL,
  `course_version` varchar(255) NOT NULL,
  `earned_all` double NOT NULL,
  `possible_all` double NOT NULL,
  `earned_graded` double NOT NULL,
  `possible_graded` double NOT NULL,
  `visible_blocks_hash` varchar(100) NOT NULL,
  `first_attempted` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `grades_persistentsubsect_course_id_user_id_usage__42820224_uniq` (`course_id`,`user_id`,`usage_key`),
  KEY `grades_persistentsub_visible_blocks_hash_20836274_fk_grades_vi` (`visible_blocks_hash`),
  KEY `grades_persistentsubsecti_modified_course_id_usage__80ab6572_idx` (`modified`,`course_id`,`usage_key`),
  KEY `grades_persistentsubsecti_first_attempted_course_id_f59f063c_idx` (`first_attempted`,`course_id`,`user_id`),
  CONSTRAINT `grades_persistentsub_visible_blocks_hash_20836274_fk_grades_vi` FOREIGN KEY (`visible_blocks_hash`) REFERENCES `grades_visibleblocks` (`hashed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

--
-- Dumping data for table `grades_persistentsubsectiongrade`
--

INSERT INTO `grades_persistentsubsectiongrade` VALUES
( '2017-04-25 20:22:42.843768' , '2017-04-25 20:22:42.845716' , 1 ,  11, 'edX/DemoX/Demo_Course', 'i4x://edX/DemoX/sequential/19a30717eff543078a5d94ae9d6c18a5', '2015-07-03 12:38:14.829000',1 , 3, 1, 3, '/DToiLniHJHJuLAaZUpyXHE/xy8=' , '2017-04-25 20:22:42.843594'),
( '2017-04-25 20:22:42.843768' , '2017-04-25 20:22:42.845716' , 2 ,  11, 'edX/DemoX/Demo_Course', 'i4x://edX/DemoX/sequential/basic_questions', '2015-07-03 12:38:14.829000',1 , 3, 1, 3, 'V22o8tNcOnsowslo/vWfHkQ+WyU=' , '2017-04-25 20:22:42.843594'),
( '2017-04-25 20:22:42.843768' , '2017-04-25 20:22:42.845716' , 3 ,  11, 'edX/DemoX/Demo_Course', 'i4x://edX/DemoX/sequential/graded_simulations', '2015-07-03 12:38:14.829000',1 , 3, 1, 3, 'SJxueL0urTTBAkq3bwXKrNfd6VM=' , '2017-04-25 20:22:42.843594'),
( '2017-04-25 20:22:42.843768' , '2017-04-25 20:22:42.845716' , 4 ,  11, 'edX/DemoX/Demo_Course', 'i4x://edX/DemoX/sequential/175e76c4951144a29d46211361266e0e', '2015-07-03 12:38:14.829000',1 , 3, 1, 3, 'sGwCz1Wbr4g0d3DSAxo6B/MDgQQ=' , '2017-04-25 20:22:42.843594'),
( '2017-04-25 20:22:42.843768' , '2017-04-25 20:22:42.845716' , 5 ,  11, 'edX/DemoX/Demo_Course', 'i4x://edX/DemoX/sequential/workflow', NULL,1 , 3, 1, 3, 'K4b8GvAypVnJFm9qTiwkOo1uchA=' , '2017-04-25 20:22:42.843594');
