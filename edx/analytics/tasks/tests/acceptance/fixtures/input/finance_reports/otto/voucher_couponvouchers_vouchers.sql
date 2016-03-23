--
-- Table structure for table `voucher_couponvouchers_vouchers`
--

DROP TABLE IF EXISTS `voucher_couponvouchers_vouchers`;
CREATE TABLE `voucher_couponvouchers_vouchers` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `couponvouchers_id` int(11) NOT NULL,
  `voucher_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `couponvouchers_id` (`couponvouchers_id`,`voucher_id`),
  KEY `voucher_coupon_voucher_id_5db29962b448e2b0_fk_voucher_voucher_id` (`voucher_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `voucher_couponvouchers_vouchers`
--

INSERT INTO `voucher_couponvouchers_vouchers` VALUES (14,5,14),(15,6,15),(16,7,16),(17,7,17),(18,7,18),(19,7,19),(20,7,20),(21,7,21),(22,7,22),(23,7,23),(24,7,24),(25,7,25),(26,8,26);
