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
