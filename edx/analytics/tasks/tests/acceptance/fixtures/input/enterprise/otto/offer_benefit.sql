--
-- Table structure for table `offer_benefit`
--

DROP TABLE IF EXISTS `offer_benefit`;
CREATE TABLE `offer_benefit` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(128) NOT NULL,
  `value` decimal(12,2) DEFAULT NULL,
  `max_affected_items` int(10) unsigned DEFAULT NULL,
  `proxy_class` varchar(255) DEFAULT NULL,
  `range_id` int(11),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `offer_benefit`
--

INSERT INTO `offer_benefit` VALUES
(1,'Percentage',25.00,NULL,NULL,1),
(2,'Percentage',100.00,NULL,NULL,2),
(3,'Absolute',100.00,NULL,NULL,3),
(4,'Absolute',50.00,NULL,NULL,4),
(5,'Percentage',50.00,NULL,NULL,5),
(6,'Percentage',100.00,NULL,NULL,6),
(7,'Absolute',100.00,NULL,NULL,7),
(8,'',100.00,NULL,'ecommerce.enterprise.benefits.EnterprisePercentageDiscountBenefit',NULL);