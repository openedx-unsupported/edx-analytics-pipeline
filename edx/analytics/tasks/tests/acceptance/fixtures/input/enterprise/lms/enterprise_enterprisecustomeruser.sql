--
-- Table structure for table `enterprise_enterprisecustomer`
--

DROP TABLE IF EXISTS `enterprise_enterprisecustomeruser`;
CREATE TABLE `enterprise_enterprisecustomeruser` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `enterprise_customer_id` char(32) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `enterprise_customer_user` (`enterprise_customer_id`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `enterprise_enterprisecustomeruser`
--
INSERT INTO `enterprise_enterprisecustomeruser` VALUES
  (1,'2017-05-23 19:50:12.480930','2018-01-11 17:18:10.743835',11,'0381d3cb033846d48a5cb1475b589d7f'),
  (2,'2017-05-30 13:20:05.314463','2018-01-11 17:18:10.793360',12,'0381d3cb033846d48a5cb1475b589d7f'),
  (3,'2017-06-07 19:53:04.516017','2018-01-11 17:18:10.840584',13,'03fc6c3a33d84580842576922275ca6f'),
  (4,'2017-05-23 19:50:12.480930','2018-01-11 17:18:10.743835',15,'0381d3cb033846d48a5cb1475b589d7f');
