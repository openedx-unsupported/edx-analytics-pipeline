--
-- Table structure for table `order_order`
--

DROP TABLE IF EXISTS `order_order`;
CREATE TABLE `order_order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `number` varchar(128) NOT NULL,
  `currency` varchar(12) NOT NULL,
  `total_incl_tax` decimal(12,2) NOT NULL,
  `total_excl_tax` decimal(12,2) NOT NULL,
  `shipping_incl_tax` decimal(12,2) NOT NULL,
  `shipping_excl_tax` decimal(12,2) NOT NULL,
  `shipping_method` varchar(128) NOT NULL,
  `shipping_code` varchar(128) NOT NULL,
  `status` varchar(100) NOT NULL,
  `guest_email` varchar(75) NOT NULL,
  `date_placed` datetime NOT NULL,
  `basket_id` int(11) DEFAULT NULL,
  `billing_address_id` int(11) DEFAULT NULL,
  `shipping_address_id` int(11),
  `site_id` int(11),
  `user_id` int(11),
  PRIMARY KEY (`id`),
  UNIQUE KEY `number` (`number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `order_order`
--

INSERT INTO `order_order` VALUES (435,'EDX-1111','USD',100.00,100.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2015-08-17 20:29:03',527,150,NULL,1,38);
