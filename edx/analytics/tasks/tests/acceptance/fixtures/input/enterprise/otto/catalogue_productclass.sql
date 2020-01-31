--
-- Table structure for table `catalogue_productclass`
--

DROP TABLE IF EXISTS `catalogue_productclass`;
CREATE TABLE `catalogue_productclass` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `slug` varchar(128) NOT NULL,
  `requires_shipping` tinyint(1) NOT NULL,
  `track_stock` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `slug` (`slug`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `catalogue_productclass`
--

INSERT INTO `catalogue_productclass` VALUES
  (1,'Seat','seat',0,0),
  (3,'Coupon','coupon',0,0),
  (4,'Enrollment Code','enrollment_code',0,0),
  (7,'Course Entitlement','course-entitlement',0,0),
  (8,'Donation','donation',1,1);
