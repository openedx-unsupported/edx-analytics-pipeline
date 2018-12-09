--
-- Table structure for table `shoppingcart_couponredemption`
--

DROP TABLE IF EXISTS `shoppingcart_couponredemption`;
CREATE TABLE `shoppingcart_couponredemption` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `coupon_id` int(11) NOT NULL,
  `order_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `shoppingcart_couponredemption`
--

INSERT INTO `shoppingcart_couponredemption` VALUES
  (4,3,13,11);
