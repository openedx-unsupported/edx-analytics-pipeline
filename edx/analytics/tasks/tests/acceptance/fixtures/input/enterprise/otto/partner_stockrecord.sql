--
-- Table structure for table `partner_stockrecord`
--

DROP TABLE IF EXISTS `partner_stockrecord`;
CREATE TABLE `partner_stockrecord` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `partner_sku` varchar(128) NOT NULL,
  `price_currency` varchar(12) NOT NULL,
  `price_excl_tax` decimal(12,2) DEFAULT NULL,
  `price_retail` decimal(12,2) DEFAULT NULL,
  `cost_price` decimal(12,2) DEFAULT NULL,
  `num_in_stock` int(10) unsigned DEFAULT NULL,
  `num_allocated` int(11) DEFAULT NULL,
  `low_stock_threshold` int(10) unsigned DEFAULT NULL,
  `date_created` datetime(6) NOT NULL,
  `date_updated` datetime(6) NOT NULL,
  `partner_id` int(11) NOT NULL,
  `product_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `partner_stockrecord_partner_id_8441e010_uniq` (`partner_id`,`partner_sku`),
  KEY `partner_stockrecord_product_id_62fd9e45_fk_catalogue_product_id` (`product_id`),
  KEY `partner_stockrecord_9474e4b5` (`date_updated`)
) ENGINE=InnoDB AUTO_INCREMENT=203 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `partner_stockrecord`
--

INSERT INTO `partner_stockrecord` VALUES
  (1,'68EFFFF','USD',300.00,NULL,NULL,NULL,NULL,NULL,'2017-06-07 18:42:40.561668','2018-01-17 21:19:17.146064',1,2),
  (2,'8CF08E5','USD',100.00,NULL,NULL,NULL,NULL,NULL,'2017-06-07 18:42:40.592853','2018-01-17 21:19:17.111078',1,4),
  (3,'D36213B','USD',200.00,NULL,NULL,NULL,NULL,NULL,'2017-06-08 20:02:29.482358','2017-06-08 20:02:29.482387',1,5),
  (4,'2278263','USD',100.00,NULL,NULL,NULL,NULL,NULL,'2017-06-08 21:17:07.233266','2017-06-08 21:17:07.233296',1,7),
  (5,'1D654A0','USD',100.00,NULL,NULL,NULL,NULL,NULL,'2017-06-12 09:04:57.693314','2017-06-12 09:04:57.693343',1,17),
  (6,'683FFFF','USD',0.00,NULL,NULL,NULL,NULL,NULL,'2017-06-07 18:42:40.561668','2018-01-17 21:19:17.146064',1,18);
