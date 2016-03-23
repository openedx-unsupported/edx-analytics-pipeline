--
-- Table structure for table `order_orderdiscount`
--

DROP TABLE IF EXISTS `order_orderdiscount`;
CREATE TABLE `order_orderdiscount` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `category` varchar(64) NOT NULL,
  `offer_id` int(10) unsigned DEFAULT NULL,
  `offer_name` varchar(128) NOT NULL,
  `voucher_id` int(10) unsigned DEFAULT NULL,
  `voucher_code` varchar(128) NOT NULL,
  `frequency` int(10) unsigned DEFAULT NULL,
  `amount` decimal(12,2) NOT NULL,
  `message` longtext NOT NULL,
  `order_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `order_orderdiscount`
--

INSERT INTO `order_orderdiscount` VALUES
(3,'Basket',5,'Catalog [4]-Percentage-25',14,'OTTO_VER_25_PCT_OFF',1,64.00,'',14),
(4,'Basket',7,'Catalog [5]-Percentage-100',16,'VFF5A4MVV5KMNSQE',1,1000.00,'',15),
(5,'Basket',6,'Catalog [5]-Absolute-200',15,'OTTO_PRO_200_USD_OFF',1,200.00,'',16),
(6,'Basket',8,'Catalog [6]-Percentage-40',26,'OTTO_CRED_40_PCT_OFF',1,40.00,'',17);
