--
-- Table structure for table `voucher_couponvouchers`
--

DROP TABLE IF EXISTS `voucher_couponvouchers`;
CREATE TABLE `voucher_couponvouchers` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `coupon_id` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `voucher_couponvouchers`
--

INSERT INTO `voucher_couponvouchers` VALUES (5,20),(6,21),(7,22),(8,23);
