--
-- Table structure for table `catalogue_product`
--

DROP TABLE IF EXISTS `catalogue_product`;
CREATE TABLE `catalogue_product` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `structure` varchar(10) NOT NULL,
  `upc` varchar(64) DEFAULT NULL,
  `title` varchar(255) NOT NULL,
  `slug` varchar(255) NOT NULL,
  `description` longtext NOT NULL,
  `rating` double DEFAULT NULL,
  `date_created` datetime NOT NULL,
  `date_updated` datetime NOT NULL,
  `is_discountable` tinyint(1) NOT NULL,
  `parent_id` int(11),
  `product_class_id` int(11),
  `course_id` varchar(255),
  `expires` datetime,
  PRIMARY KEY (`id`),
  UNIQUE KEY `upc` (`upc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `catalogue_product`
--

INSERT INTO `catalogue_product` VALUES
  (1,'parent',NULL,'A demonstration course','parent-demo-demox-democourse-1T2015','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,NULL,1,'edX/Open_DemoX/edx_demo_course',NULL),
  (2,'child',NULL,'A demonstration course (ID verified)','demo-demox-democourse-1T2015-id-verified','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,1,NULL,'edX/Open_DemoX/edx_demo_course','2016-12-12 00:00:00'),
  (3,'parent',NULL,'Seat in edX Demo Course 2','seat-in','',NULL,'2016-03-22 20:32:49.444111','2016-03-22 20:32:49.476687',1,NULL,1,'course-v1:edX+Open_DemoX+edx_demo_course2',NULL),
  (4,'child',NULL,'Seat in edX Demo Course 2','seat-in-test-otto-verified-course','',NULL,'2016-03-22 20:32:49.520169','2016-03-22 20:32:49.520208',1,3,NULL,'course-v1:edX+Open_DemoX+edx_demo_course2',NULL),
  (5,'child',NULL,'Seat in edX Demo Verified Course 2 with verified certificate (and ID verification)','seat-in-test-otto-verified-course-with-verified-certificate-and-id-verification','',NULL,'2016-03-22 20:32:49.582976','2016-03-22 20:32:49.583011',1,3,NULL,'course-v1:edX+Open_DemoX+edx_demo_course2',NULL),
  (6,'parent',NULL,'Seat in edX Demo Professional Course 2','seat-in','',NULL,'2016-03-22 20:33:29.480679','2016-03-22 20:33:29.589327',1,NULL,1,'course-v1:edX+Open_DemoX+edx_demo_course2',NULL),
  (7,'child',NULL,'Seat in edX Demo Professional Course with professional certificate','seat-in-test-otto-professional-course-with-professional-certificate','',NULL,'2016-03-22 20:33:29.609508','2016-03-22 20:33:29.609558',1,6,NULL,'course-v1:edX+Open_DemoX+edx_demo_course2',NULL),
  (12,'standalone',NULL,'AccTest 25% off Otto Verified','578E9F3544','',NULL,'2016-03-22 20:44:23.266854','2016-03-22 20:44:23.310765',1,NULL,3,NULL,NULL),
  (13,'standalone',NULL,'AccTest $200 off Otto Prof','92D741F650','',NULL,'2016-03-22 20:46:39.846749','2016-03-22 20:46:39.958757',1,NULL,3,NULL,NULL),
  (14,'standalone',NULL,'AccTest Otto Pro Enroll Code','450483FB7B','',NULL,'2016-03-22 20:47:58.279367','2016-03-22 20:47:58.384073',1,NULL,3,NULL,NULL),
  (15,'standalone',NULL,'AccTest 40% off Otto Credit','901A4FE566','',NULL,'2016-03-22 20:50:27.485071','2016-03-22 20:50:27.538182',1,NULL,3,NULL,NULL),
  (16,'parent',NULL,'edX Test Course','parent-demo-edx-Testing-1T2017','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,NULL,1,'course-v1:edX+Testing102x+1T2017',NULL),
  (17,'child',NULL,'edX Test Course (ID verified)','demo-edx-Testing-1T2017-id-verified','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,1,NULL,'course-v1:edX+Testing102x+1T2017','2016-12-12 00:00:00'),
  (18,'child',NULL,'A demonstration course (audit track)','demo-demox-democourse-1T2015-audit','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',0,1,NULL,'edX/Open_DemoX/edx_demo_course','2016-12-12 00:00:00'),
  (19,'parent',NULL,'Parent Course Entitlement for Demo Professional Course','parent-demo-edx-Entitlement-1T2017','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,NULL,7,NULL,NULL);
