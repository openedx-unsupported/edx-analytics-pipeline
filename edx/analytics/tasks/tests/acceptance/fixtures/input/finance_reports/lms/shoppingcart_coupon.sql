--
-- Table structure for table `shoppingcart_coupon`
--

DROP TABLE IF EXISTS `shoppingcart_coupon`;
CREATE TABLE `shoppingcart_coupon` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `course_id` varchar(255) NOT NULL,
  `percentage_discount` int(11) NOT NULL,
  `created_at` datetime NOT NULL,
  `is_active` tinyint(1) NOT NULL,
  `expiration_date` datetime DEFAULT NULL,
  `created_by_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `shoppingcart_coupon`
--

INSERT INTO `shoppingcart_coupon` VALUES
  (2,'SC_VER_40_PCT_OFF','40% off the verified mode of the Shopping Cart Course','course-v1:AccTest+ShoppingCart+Verified',40,'2016-03-22 20:19:55.693594',1,NULL,5),
  (3,'SC_PAID_80_PCT_OFF','80% off the paid ShoppingCart course','course-v1:AccTest+ShoppingCart+Paid',80,'2016-03-22 20:20:39.722085',1,NULL,5);
