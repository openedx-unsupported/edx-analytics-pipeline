--
-- Table structure for table `refund_refundline`
--

DROP TABLE IF EXISTS `refund_refundline`;
CREATE TABLE `refund_refundline` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `line_credit_excl_tax` decimal(12,2) NOT NULL,
  `quantity` int(10) unsigned NOT NULL,
  `status` varchar(255) NOT NULL,
  `order_line_id` int(11) NOT NULL,
  `refund_id` int(11) NOT NULL,
  `created` datetime NOT NULL,
  `modified` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `refund_refundline`
--

INSERT INTO `refund_refundline` VALUES
  (1,199.00,1,'Complete',8,1,'2017-09-01 20:29:03','2017-09-01 20:29:03'),
  (2,300.00,1,'Open',1,2,'2017-03-22 20:58:06.345444','2017-03-22 20:58:06.345998'),
  (3,57.00,1,'Complete',13,1,'2017-09-01 20:29:03','2017-09-01 20:29:03');
