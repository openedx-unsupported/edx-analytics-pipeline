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
(1,'Basket',1,'Catalog [4]-Percentage-25',1,'PJS4LCU435W6KGBS',1,64.00,'',1),
(2,'Basket',2,'Catalog [5]-Percentage-100',2,'OTTO_VER_25_PCT_OFF',1,100.00,'',2),
(3,'Basket',3,'Catalog [5]-Absolute-100',3,'QAFWBFZ26GYYYIJS',1,200.00,'',3),
(4,'Basket',4,'Catalog [5]-Absolute-50',4,'ZSJHRVLCNTT6XFCJ',1,200.00,'',4),
(5,'Basket',5,'Catalog [5]-Percentage-50',5,'CQHVBDLY35WSJRZ4',1,200.00,'',5),
(6,'Basket',2,'Catalog [5]-Percentage-100',2,'OTTO_VER_25_PCT_OFF',1,100.00,'',6),
(7,'Basket',6,'Catalog [5]-Absolute-100',NULL,'',1,200.00,'',8),
(8,'Basket',7,'Catalog [5]-Absolute-50',NULL,'',1,200.00,'',9),
(9,'Basket',8,'Catalog [5]-Absolute-50',NULL,'',1,200.00,'',10),
(10,'Basket',6,'Catalog [5]-Absolute-100',NULL,'',1,200.00,'',13),
(11,'Basket',6,'Catalog [5]-Absolute-100',NULL,'',1,200.00,'',14),
(12,'Basket',6,'Catalog [5]-Absolute-100',NULL,'',1,200.00,'',15);
