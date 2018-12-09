--
-- Table structure for table `catalogue_productattribute`
--

DROP TABLE IF EXISTS `catalogue_productattribute`;
CREATE TABLE `catalogue_productattribute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `code` varchar(128) NOT NULL,
  `type` varchar(20) NOT NULL,
  `required` tinyint(1) NOT NULL,
  `option_group_id` int(11) DEFAULT NULL,
  `product_class_id` int(11),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `catalogue_productattribute`
--

INSERT INTO `catalogue_productattribute` VALUES
  (1,'course_key','course_key','text',1,NULL,1),
  (2,'id_verification_required','id_verification_required','boolean',0,NULL,1),
  (3,'certificate_type','certificate_type','text',0,NULL,1),
  (4,'credit_provider','credit_provider','text',0,NULL,1),
  (5,'credit_hours','credit_hours','integer',0,NULL,1),
  (10,'Coupon vouchers','coupon_vouchers','entity',1,NULL,3),
  (12,'Note','note','text',0,NULL,3),
  (13,'Course Key','course_key','text',1,NULL,4),
  (14,'Seat Type','seat_type','text',1,NULL,4),
  (15,'id_verification_required','id_verification_required','boolean',0,NULL,4),
  (20,'UUID','UUID','text',1,NULL,7),
  (21,'certificate_type','certificate_type','text',0,NULL,7);
