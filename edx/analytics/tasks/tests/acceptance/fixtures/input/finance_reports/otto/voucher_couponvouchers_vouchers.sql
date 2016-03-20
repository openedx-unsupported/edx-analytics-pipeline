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
