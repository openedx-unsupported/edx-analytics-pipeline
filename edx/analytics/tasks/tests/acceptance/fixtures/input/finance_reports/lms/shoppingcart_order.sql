--
-- Table structure for table `shoppingcart_order`
--

DROP TABLE IF EXISTS `shoppingcart_order`;
CREATE TABLE `shoppingcart_order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `currency` varchar(8) NOT NULL,
  `status` varchar(32) NOT NULL,
  `purchase_time` datetime DEFAULT NULL,
  `bill_to_first` varchar(64) NOT NULL,
  `bill_to_last` varchar(64) NOT NULL,
  `bill_to_street1` varchar(128) NOT NULL,
  `bill_to_street2` varchar(128) NOT NULL,
  `bill_to_city` varchar(64) NOT NULL,
  `bill_to_state` varchar(8) NOT NULL,
  `bill_to_postalcode` varchar(16) NOT NULL,
  `bill_to_country` varchar(64) NOT NULL,
  `bill_to_ccnum` varchar(8) NOT NULL,
  `bill_to_cardtype` varchar(32) NOT NULL,
  `processor_reply_dump` longtext NOT NULL,
  `refunded_time` datetime,
  `company_name` varchar(255),
  `company_contact_name` varchar(255),
  `company_contact_email` varchar(255),
  `recipient_name` varchar(255),
  `recipient_email` varchar(255),
  `customer_reference_number` varchar(63),
  `order_type` varchar(32) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `shoppingcart_order`
--

INSERT INTO `shoppingcart_order` VALUES (1,11,'usd','refunded','2013-09-12 21:24:38','Test','User','','','Cambridge','MA','02140','us','','','',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'personal');

