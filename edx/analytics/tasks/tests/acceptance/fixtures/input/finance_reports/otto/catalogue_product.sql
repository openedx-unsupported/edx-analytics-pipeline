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

INSERT INTO `catalogue_product` VALUES (5331,'parent',NULL,'A demonstration course','parent-demo-demox-democourse-1T2015','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,NULL,1,'DemoX/DemoCourse/1T2015',NULL);
INSERT INTO `catalogue_product` VALUES (5334,'child',NULL,'A demonstration course (ID verified)','demo-demox-democourse-1T2015-id-verified','',NULL,'2015-08-06 19:06:21','2015-08-06 19:21:06',1,5331,NULL,'DemoX/DemoCourse/1T2015','2016-12-12 00:00:00');
