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
  (436,'edX','000F00C','','','Seat in a demonstration course (and ID verification)','',1,100.00,100.00,100.00,100.00,NULL,100.00,100.00,NULL,'Complete',NULL,435,1,5334,2146),
  -- 4 "orders" to create the coupons
  (9,'OpenCraft','25F38CF','','','AccTest 25% off Otto Verified','',1,256.00,256.00,256.00,256.00,NULL,256.00,256.00,NULL,'Complete',NULL,9,2,20,15),
  (10,'OpenCraft','123D6C8','','','AccTest $200 off Otto Prof','',1,1000.00,1000.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,10,2,21,16),
  (11,'OpenCraft','E8217BC','','','Otto Pro Enroll Code','',1,1000.00,1000.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,11,2,22,17),
  (12,'OpenCraft','AF1E512','','','AccTest 40% off Otto Credit','',1,100.00,100.00,100.00,100.00,NULL,100.00,100.00,NULL,'Complete',NULL,12,2,23,18),
  -- test_user normal purchase of "Test Otto Verified Course", plus refund:
  (13,'OpenCraft','D354B6A','','','Seat in Test Otto Verified Course with verified certificate (and ID verification)','',1,256.00,256.00,256.00,256.00,NULL,256.00,256.00,NULL,'Complete',NULL,13,2,13,10),
  -- test_user2 discounted purchase of "Test Otto Verified Course"
  (14,'OpenCraft','D354B6A','','','Seat in Test Otto Verified Course with verified certificate (and ID verification)','',1,192.00,192.00,256.00,256.00,NULL,256.00,256.00,NULL,'Complete',NULL,14,2,13,10),
  -- test_user enrollment code into professional course
  (15,'OpenCraft','CF9A708','','','Seat in Test Otto Professional Course with professional certificate','',1,0.00,0.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,15,2,15,11),
  -- test_user2 discounted purchase of professional course
  (16,'OpenCraft','CF9A708','','','Seat in Test Otto Professional Course with professional certificate','',1,800.00,800.00,1000.00,1000.00,NULL,1000.00,1000.00,NULL,'Complete',NULL,16,2,15,11),
  -- test_user discounted purchase of credit course
  (17,'OpenCraft','11A9662','','','Seat in Test Otto Credit Course with credit certificate (and ID verification)','',1,60.00,60.00,100.00,100.00,NULL,100.00,100.00,NULL,'Complete',NULL,17,2,19,14),
  -- test_user purchased a course entitlement
  (53260,'edX','clintonb','','','Verified Seat in Foundations of Computer Graphics','',1,100.00,100.00,100.00,100.00,NULL,100.00,100.00,NULL,'Complete',NULL,53148,1,29440,24908);
