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
  (435,'EDX-1111','USD',100.00,100.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2015-08-17 20:29:03',527,150,NULL,1,38),
  -- 4 "orders" to create the coupons
  (9,'OCX-100015','USD',256.00,256.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:44:23.379671',15,NULL,NULL,1,39),
  (10,'OCX-100016','USD',1000.00,1000.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:46:40.075408',16,NULL,NULL,1,39),
  (11,'OCX-100017','USD',1000.00,1000.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:47:58.448342',17,NULL,NULL,1,39),
  (12,'OCX-100018','USD',100.00,100.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:50:27.647375',18,NULL,NULL,1,39),
  -- test_user normal purchase of "Test Otto Verified Course", plus open refund (refund not complete):
  (13,'OCX-100019','USD',256.00,256.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:57:10.353473',19,NULL,NULL,1,38),
  -- test_user2 discounted purchase of "Test Otto Verified Course"
  (14,'OCX-100020','USD',192.00,192.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 20:59:58.292441',20,NULL,NULL,1,40),
  -- test_user enrollment code into professional course
  (15,'OCX-100021','USD',0.00,0.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:02:09.032234',21,NULL,NULL,1,38),
  -- test_user2 discounted purchase of professional course
  (16,'OCX-100022','USD',800.00,800.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:04:08.048200',22,NULL,NULL,1,40),
  -- test_user discounted purchase of credit course
  (17,'OCX-100023','USD',60.00,60.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2016-03-22 21:09:45.318743',23,NULL,NULL,1,38),
  -- test_user purchased a course entitlement
  (53148,'EDX-157245','USD',100.00,100.00,0.00,0.00,'No shipping required','no-shipping-required','Complete','','2017-11-20 16:29:34',57245,NULL,NULL,1,38);
