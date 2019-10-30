--
-- Table structure for table `consent_datasharingconsent`
--

DROP TABLE IF EXISTS `consent_datasharingconsent`;
CREATE TABLE `consent_datasharingconsent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  `username` varchar(255) NOT NULL,
  `granted` tinyint(1) DEFAULT NULL,
  `course_id` varchar(255) NOT NULL,
  `enterprise_customer_id` char(32) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `consent_datasharing` (`enterprise_customer_id`,`username`,`course_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6728 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `consent_datasharingconsent`
--

INSERT INTO `consent_datasharingconsent` VALUES
  (1,'2014-06-27 16:02:38','2014-06-27 16:02:38','test_user',1,'edX/Open_DemoX/edx_demo_course','0381d3cb033846d48a5cb1475b589d7f'),
  (2,'2016-03-22 20:56:09.320098','2016-03-22 20:56:09.320098','test_user',1,'course-v1:edX+Open_DemoX+edx_demo_course2','0381d3cb033846d48a5cb1475b589d7f'),
  (3,'2016-03-22 20:59:12.281696','2016-03-22 20:59:12.281696','test_user2',1,'course-v1:edX+Open_DemoX+edx_demo_course2','0381d3cb033846d48a5cb1475b589d7f'),
  (4,'2016-03-22 21:02:09.195616','2016-03-22 21:02:09.195616','test_user3',1,'course-v1:edX+Open_DemoX+edx_demo_course2','03fc6c3a33d84580842576922275ca6f'),
  (5,'2016-03-22 21:04:08.211237','2016-03-22 21:04:08.211237','test_user2',1,'course-v1:edX+Testing102x+1T2017','0381d3cb033846d48a5cb1475b589d7f'),
  (6,'2016-03-22 21:08:08.432870','2016-03-22 21:08:08.432870','test_user',0,'course-v1:edX+Testing102x+1T2017','0381d3cb033846d48a5cb1475b589d7f'),
  (7,'2014-06-27 16:02:38','2014-06-27 16:02:38','test_user5',1,'edX/Open_DemoX/edx_demo_course','0381d3cb033846d48a5cb1475b589d7f'),
  (8,'2016-03-22 20:56:09.320098','2016-03-22 20:56:09.320098','test_user5',1,'course-v1:edX+Open_DemoX+edx_demo_course2','0381d3cb033846d48a5cb1475b589d7f'),
  (9,'2016-03-22 21:08:08.432870','2016-03-22 21:08:08.432870','test_user5',1,'course-v1:edX+Testing102x+1T2017','0381d3cb033846d48a5cb1475b589d7f'),
  (10,'2019-09-04 21:08:08.432870','2019-09-04 21:08:08.432870','test_user6',1,'course-v1:edX+Testing102x+1T2017','0381d3cb033846d48a5cb1475b589d7f'),
  (11,'2016-03-22 20:56:09.320098','2016-03-22 20:56:09.320098','test_user5',1,'course-v1:edX+Open_DemoX+edx_demo_course3','0381d3cb033846d48a5cb1475b589d7f');
