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
  (1,'EDX-100019','USD',156.00,156.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:57:10.353473',19,NULL,NULL,1,39),
  (2,'EDX-100020','USD',192.00,192.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:59:58.292441',20,NULL,NULL,1,39),
  (3,'EDX-100021','USD',10.00,10.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',21,NULL,NULL,1,40),
  (4,'EDX-100022','USD',20.00,20.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:04:08.048200',22,NULL,NULL,1,41),
  (5,'EDX-100023','USD',60.00,60.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:09:45.318743',23,NULL,NULL,1,40),
  (6,'EDX-100024','USD',56.00,56.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:57:10.353473',24,NULL,NULL,1,39),
  (7,'EDX-100025','USD',100.00,100.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:59:58.292441',25,NULL,NULL,1,42),
  (8,'EDX-100026','USD',56.00,56.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',26,NULL,NULL,1,43),
  (9,'EDX-100027','USD',20.00,20.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:04:08.048200',27,NULL,NULL,1,43),
  (10,'EDX-100028','USD',60.00,60.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:09:45.318743',28,NULL,NULL,1,43),
  (11,'EDX-100029','USD',0.00,0.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',29,NULL,NULL,1,43),
  (12,'EDX-157042','USD',225.00,225.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',30,NULL,NULL,1,43),
  (13,'EDX-157043','USD',57.00,57.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',31,NULL,NULL,1,43),
  (14,'EDX-157044','USD',99.00,99.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',32,NULL,NULL,1,43),
  (15,'EDX-157045','USD',57.00,57.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',33,NULL,NULL,1,43);
