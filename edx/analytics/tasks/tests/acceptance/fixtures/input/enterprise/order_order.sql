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

INSERT INTO `order_order` VALUES
  (1,'EDX-100019','USD',156.00,256.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:57:10.353473',19,NULL,NULL,1,38),
  (2,'EDX-100020','USD',192.00,192.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:59:58.292441',20,NULL,NULL,1,40),
  (3,'EDX-100021','USD',10.00,0.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',21,NULL,NULL,1,38),
  (4,'EDX-100022','USD',120.00,800.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:04:08.048200',22,NULL,NULL,1,40),
  (5,'EDX-100023','USD',60.00,60.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:09:45.318743',23,NULL,NULL,1,38);
