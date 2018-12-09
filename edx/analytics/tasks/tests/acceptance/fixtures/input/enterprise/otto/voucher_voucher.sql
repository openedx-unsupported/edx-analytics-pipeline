--
-- Table structure for table `voucher_voucher`
--

DROP TABLE IF EXISTS `voucher_voucher`;
CREATE TABLE `voucher_voucher` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `code` varchar(128) NOT NULL,
  `usage` varchar(128) NOT NULL,
  `start_datetime` datetime(6) NOT NULL,
  `end_datetime` datetime(6) NOT NULL,
  `num_basket_additions` int(10) unsigned NOT NULL,
  `num_orders` int(10) unsigned NOT NULL,
  `total_discount` decimal(12,2) NOT NULL,
  `date_created` datetime(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `code` (`code`)
) ENGINE=InnoDB AUTO_INCREMENT=489 DEFAULT CHARSET=utf8;

INSERT INTO `voucher_voucher` VALUES
  (1,'Pied Piper Discount','PJS4LCU435W6KGBS','Multi-use','2017-06-01 00:00:00.000000','2025-08-31 00:00:00.000000',0,66,662.00,'2017-06-08 00:00:00.000000'),
  (2,'Aviato','OTTO_VER_25_PCT_OFF','Multi-use','2017-06-01 00:00:00.000000','2025-08-31 00:00:00.000000',0,2,200.00,'2017-06-08 00:00:00.000000'),
  (3,'ENT - Email domain restricted','QAFWBFZ26GYYYIJS','Multi-use','2017-06-01 00:00:00.000000','2017-12-31 00:00:00.000000',0,1,0.00,'2017-06-12 00:00:00.000000'),
  (4,'ENT - No restrictions','ZSJHRVLCNTT6XFCJ','Multi-use','2017-06-01 00:00:00.000000','2017-12-31 00:00:00.000000',0,1,20.00,'2017-06-12 00:00:00.000000'),
  (5,'ENT - Discount','CQHVBDLY35WSJRZ4','Multi-use','2017-06-01 00:00:00.000000','2017-12-31 00:00:00.000000',0,1,20.00,'2017-06-12 00:00:00.000000');
