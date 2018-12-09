--
-- Table structure for table `shoppingcart_orderitem`
--

DROP TABLE IF EXISTS `shoppingcart_orderitem`;
CREATE TABLE `shoppingcart_orderitem` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `order_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `status` varchar(32) NOT NULL,
  `qty` int(11) NOT NULL,
  `unit_cost` decimal(30,2) NOT NULL,
  `list_price` decimal(30,2),
  `line_desc` varchar(1024) NOT NULL,
  `currency` varchar(8) NOT NULL,
  `fulfilled_time` datetime,
  `refund_requested_time` datetime,
  `service_fee` decimal(30,2) NOT NULL,
  `report_comments` longtext NOT NULL,
  `created` datetime NOT NULL,
  `modified` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `shoppingcart_orderitem`
--

INSERT INTO `shoppingcart_orderitem` VALUES
  (4,1,11,'refunded',1,5.00,NULL,'Certificate of Achievement, Verified Certificate for DemoCourse','usd','2013-09-12 21:24:38',NULL,0.00,'','2014-11-10 18:30:27','2014-11-10 18:30:29'),
  (32,12,11,'purchased',1,512.00,512.00,'Test SC Verified Option for course Test SC Verified Option','usd','2016-03-22 21:18:46.457400',NULL,0.00,'','2016-03-22 21:18:42.862374','2016-03-22 21:18:46.457560'),
  (34,13,11,'purchased',4,25.60,128.00,'Enrollment codes for Course: Test SC Paid Course','usd','2016-03-22 21:27:03.450290',NULL,0.00,'','2016-03-22 21:19:57.941323','2016-03-22 21:27:03.450518');

