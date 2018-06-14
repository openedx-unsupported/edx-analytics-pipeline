--
-- Table structure for table `order_line`
--

DROP TABLE IF EXISTS `order_line`;
CREATE TABLE `order_line` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `partner_name` varchar(128) NOT NULL,
  `partner_sku` varchar(128) NOT NULL,
  `partner_line_reference` varchar(128) NOT NULL,
  `partner_line_notes` longtext NOT NULL,
  `title` varchar(255) NOT NULL,
  `upc` varchar(128) DEFAULT NULL,
  `quantity` int(10) unsigned NOT NULL,
  `line_price_incl_tax` decimal(12,2) NOT NULL,
  `line_price_excl_tax` decimal(12,2) NOT NULL,
  `line_price_before_discounts_incl_tax` decimal(12,2) NOT NULL,
  `line_price_before_discounts_excl_tax` decimal(12,2) NOT NULL,
  `unit_cost_price` decimal(12,2) DEFAULT NULL,
  `unit_price_incl_tax` decimal(12,2) DEFAULT NULL,
  `unit_price_excl_tax` decimal(12,2) DEFAULT NULL,
  `unit_retail_price` decimal(12,2) DEFAULT NULL,
  `status` varchar(255) NOT NULL,
  `est_dispatch_date` date DEFAULT NULL,
  `order_id` int(11) NOT NULL,
  `partner_id` int(11),
  `product_id` int(11),
  `stockrecord_id` int(11),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `order_line`
--

INSERT INTO `order_line` VALUES
  (1,'edX','D354B6A','','','Seat in edX Demo Course with verified certificate (and ID verification)','',1,256.00,256.00,256.00,256.00,NULL,256.00,256.00,NULL,'Complete',NULL,1,1,2,1),
  (2,'edX','D354B6A','','','Seat in edX Demo Verified Course 2 with verified certificate (and ID verification)','',1,192.00,192.00,256.00,256.00,NULL,256.00,256.00,NULL,'Complete',NULL,2,1,5,2),
  (3,'edX','CF9A708','','','Seat in edX Demo Professional Course 2 with professional certificate','',1,0.00,0.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,3,1,7,3),
  (4,'edX','CF9A708','','','Seat in edX Demo Professional Course with professional certificate certificate','',1,0.00,0.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,4,1,4,3),
  (5,'edX','CF9A708','','','Seat in Test Verified Course','',1,0.00,0.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,5,1,17,3);
