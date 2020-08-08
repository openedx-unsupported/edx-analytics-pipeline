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
  (1,'edX','D354B6A','','','A demonstration course (ID verified)','',1,300.00,300.00,300.00,300.00,300.00,300.00,300.00,300.00,'Complete',NULL,1,1,2,1),
  (2,'edX','D354B6A','','','Seat in edX Demo Verified Course 2 with verified certificate (and ID verification)','',1,200.00,200.00,200.00,200.00,200.00,200.00,200.00,200.00,'Complete',NULL,2,1,5,3),
  (3,'edX','CF9A708','','','Seat in edX Demo Verified Course 2 with verified certificate (and ID verification)','',1,200.00,200.00,200.00,200.00,200.00,200.00,200.00,200.00,'Complete',NULL,3,1,5,3),
  (4,'edX','CF9A708','','','Seat in edX Demo Professional Course with professional certificate','',1,100.00,100.00,100.00,100.00,100.00,100.00,100.00,100.00,'Complete',NULL,4,1,7,4),
  (5,'edX','CF9A708','','','edX Test Course (ID verified)','',1,100.00,100.00,100.00,100.00,100.00,100.00,100.00,100.00,'Complete',NULL,5,1,17,5),
  (6,'edX','D354B6A','','','edX Test Course (ID verified)','',1,100.00,100.00,100.00,100.00,100.00,100.00,100.00,100.00,'Complete',NULL,6,1,17,5),
  (7,'edX','D354B6A','','','edX Test Course (ID verified)','',1,100.00,100.00,100.00,100.00,100.00,100.00,100.00,100.00,'Complete',NULL,7,1,17,5),
  (8,'edX','CF9A708','','','A demonstration course (ID verified)','',1,199.00,199.00,199.00,199.00,199.00,199.00,199.00,199.00,'Complete',NULL,8,1,2,1),
  (9,'edX','CF9A708','','','Seat in edX Demo Verified Course 2 with verified certificate (and ID verification)','',1,200.00,200.00,200.00,200.00,200.00,200.00,200.00,200.00,'Complete',NULL,9,1,5,3),
  (10,'edX','CF9A708','','','edX Test Course (ID verified)','',1,100.00,100.00,100.00,100.00,100.00,100.00,100.00,100.00,'Complete',NULL,10,1,17,5),
  (11,'edX','CF9A708','','','A demonstration course (ID verified)','',1,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,'Complete',NULL,11,1,18,6),
  (12,'edX','CF9A708','','','A demonstration course (ID verified)','',1,225.00,225.00,225.00,225.00,225.00,225.00,225.00,225.00,'Complete',NULL,12,1,19,1),
  (13,'edX','CF9A708','','','A demonstration course (ID verified)','',1,230.00,230.00,230.00,230.00,230.00,230.00,230.00,230.00,'Complete',NULL,13,1,2,1),
  (14,'edX','CF9A708','','','A demonstration course (ID verified)','',1,199.00,199.00,199.00,199.00,199.00,199.00,199.00,199.00,'Complete',NULL,14,1,2,1),
  (15,'edX','CF9A708','','','edX Test Course (ID verified)','',1,100.00,100.00,100.00,100.00,100.00,100.00,100.00,100.00,'Complete',NULL,14,1,17,5),
  (16,'edX','CF9A708','','','A demonstration course (ID verified)','',1,230.00,230.00,230.00,230.00,230.00,230.00,230.00,230.00,'Complete',NULL,15,1,2,1);
