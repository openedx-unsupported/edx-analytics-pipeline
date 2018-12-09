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
  (5331,'parent',NULL,'A demonstration course','parent-demo-demox-democourse-1T2015','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,NULL,1,'DemoX/DemoCourse/1T2015',NULL),
  (5334,'child',NULL,'A demonstration course (ID verified)','demo-demox-democourse-1T2015-id-verified','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,5331,NULL,'DemoX/DemoCourse/1T2015','2016-12-12 00:00:00'),
  (11,'parent',NULL,'Seat in Test Otto Verified Course','seat-in','',NULL,'2016-03-22 20:32:49.444111','2016-03-22 20:32:49.476687',1,NULL,1,'course-v1:AccTest+Otto+Verified',NULL),
  (12,'child',NULL,'Seat in Test Otto Verified Course','seat-in-test-otto-verified-course','',NULL,'2016-03-22 20:32:49.520169','2016-03-22 20:32:49.520208',1,11,NULL,'course-v1:AccTest+Otto+Verified',NULL),
  (13,'child',NULL,'Seat in Test Otto Verified Course with verified certificate (and ID verification)','seat-in-test-otto-verified-course-with-verified-certificate-and-id-verification','',NULL,'2016-03-22 20:32:49.582976','2016-03-22 20:32:49.583011',1,11,NULL,'course-v1:AccTest+Otto+Verified',NULL),
  (14,'parent',NULL,'Seat in Test Otto Professional Course','seat-in','',NULL,'2016-03-22 20:33:29.480679','2016-03-22 20:33:29.589327',1,NULL,1,'course-v1:AccTest+Otto+Professional',NULL),
  (15,'child',NULL,'Seat in Test Otto Professional Course with professional certificate','seat-in-test-otto-professional-course-with-professional-certificate','',NULL,'2016-03-22 20:33:29.609508','2016-03-22 20:33:29.609558',1,14,NULL,'course-v1:AccTest+Otto+Professional',NULL),
  (16,'parent',NULL,'Seat in Test Otto Credit Course','seat-in','',NULL,'2016-03-22 20:39:56.815403','2016-03-22 20:39:56.865742',1,NULL,1,'course-v1:AccTest+Otto+Credit',NULL),
  (17,'child',NULL,'Seat in Test Otto Credit Course','seat-in-test-otto-credit-course','',NULL,'2016-03-22 20:39:56.897350','2016-03-22 20:39:56.897388',1,16,NULL,'course-v1:AccTest+Otto+Credit',NULL),
  (18,'child',NULL,'Seat in Test Otto Credit Course with verified certificate (and ID verification)','seat-in-test-otto-credit-course-with-verified-certificate-and-id-verification','',NULL,'2016-03-22 20:39:56.933124','2016-03-22 20:39:56.933162',1,16,NULL,'course-v1:AccTest+Otto+Credit',NULL),
  (19,'child',NULL,'Seat in Test Otto Credit Course with credit certificate (and ID verification)','seat-in-test-otto-credit-course-with-credit-certificate-and-id-verification','',NULL,'2016-03-22 20:39:56.976366','2016-03-22 20:39:56.976402',1,16,NULL,'course-v1:AccTest+Otto+Credit',NULL),
  (20,'standalone',NULL,'AccTest 25% off Otto Verified','578E9F3544','',NULL,'2016-03-22 20:44:23.266854','2016-03-22 20:44:23.310765',1,NULL,3,NULL,NULL),
  (21,'standalone',NULL,'AccTest $200 off Otto Prof','92D741F650','',NULL,'2016-03-22 20:46:39.846749','2016-03-22 20:46:39.958757',1,NULL,3,NULL,NULL),
  (22,'standalone',NULL,'AccTest Otto Pro Enroll Code','450483FB7B','',NULL,'2016-03-22 20:47:58.279367','2016-03-22 20:47:58.384073',1,NULL,3,NULL,NULL),
  (23,'standalone',NULL,'AccTest 40% off Otto Credit','901A4FE566','',NULL,'2016-03-22 20:50:27.485071','2016-03-22 20:50:27.538182',1,NULL,3,NULL,NULL),
  (29439,'parent',NULL,'Foundations of Computer Graphics',NULL,'',NULL,'2017-11-20 16:20:40','2017-11-20 16:28:23',1,NULL,7,NULL,NULL),
  (29440,'child',NULL,'Verified Seat in Foundations of Computer Graphics','verified-seat-in-foundations-of-computer-graphics','',NULL,'2017-11-20 16:28:23','2017-11-20 16:28:39',1,29439,NULL,NULL,NULL);
