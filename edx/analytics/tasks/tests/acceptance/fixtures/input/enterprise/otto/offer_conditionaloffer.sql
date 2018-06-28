--
-- Table structure for table `offer_conditionaloffer`
--

DROP TABLE IF EXISTS `offer_conditionaloffer`;
CREATE TABLE `offer_conditionaloffer` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `slug` varchar(128) NOT NULL,
  `description` longtext NOT NULL,
  `offer_type` varchar(128) NOT NULL,
  `status` varchar(64) NOT NULL,
  `priority` int(11) NOT NULL,
  `start_datetime` datetime DEFAULT NULL,
  `end_datetime` datetime DEFAULT NULL,
  `max_global_applications` int(10) unsigned DEFAULT NULL,
  `max_user_applications` int(10) unsigned DEFAULT NULL,
  `max_basket_applications` int(10) unsigned DEFAULT NULL,
  `max_discount` decimal(12,2) DEFAULT NULL,
  `total_discount` decimal(12,2) NOT NULL,
  `num_applications` int(10) unsigned NOT NULL,
  `num_orders` int(10) unsigned NOT NULL,
  `redirect_url` varchar(200) NOT NULL,
  `date_created` datetime NOT NULL,
  `benefit_id` int(11) NOT NULL,
  `condition_id` int(11) NOT NULL,
  `email_domains` varchar(255),
  `site_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `offer_conditionaloffer`
--

INSERT INTO `offer_conditionaloffer` VALUES
  (1,'Catalog [4]-Percentage-25','catalog-4-percentage-25','','Voucher','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',1,1,NULL,1),
  (2,'Catalog [5]-Percentage-100','catalog-5-percentage-100','','Voucher','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',2,2,NULL,1),
  (3,'Catalog [5]-Absolute-100','catalog-5-absolute-100','','Voucher','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',3,3,NULL,1),
  (4,'Catalog [5]-Absolute-50','catalog-5-absolute-50','','Voucher','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',4,4,NULL,1),
  (5,'Catalog [5]-Percentage-50','catalog-5-percentage-50','','Voucher','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',5,5,NULL,1),
  (6,'Discount 1','discount-1','','Site','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',6,6,NULL,1),
  (7,'Discount 2','discount-2','','Site','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',7,7,NULL,1),
  (8,'Discount 3','discount-3','','Site','Open',0,NULL,NULL,NULL,NULL,NULL,NULL,100.00,1,1,'','2017-05-31 19:18:14',8,8,NULL,1);
