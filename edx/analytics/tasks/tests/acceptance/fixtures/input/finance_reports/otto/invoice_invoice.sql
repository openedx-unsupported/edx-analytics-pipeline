--
-- Table structure for table `invoice_invoice`
--

DROP TABLE IF EXISTS `invoice_invoice`;
CREATE TABLE `invoice_invoice` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created` datetime NOT NULL,
  `modified` datetime NOT NULL,
  `state` varchar(255) NOT NULL,
  `basket_id` int(11) DEFAULT NULL,
  `business_client_id` int(11) DEFAULT NULL,
  `order_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `invoice_invoice`
--

INSERT INTO `invoice_invoice` VALUES
  (1,'2016-03-23 09:55:06.689237','2016-03-23 10:37:07.169975','Paid',24,1,18);
