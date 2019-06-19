--
-- Table structure for table `enterprise_enterprisecustomer`
--

DROP TABLE IF EXISTS `enterprise_enterprisecustomer`;
CREATE TABLE `enterprise_enterprisecustomer` (
  `created` datetime(6) NOT NULL,
  `modified` datetime(6) NOT NULL,
  `uuid` char(32) NOT NULL,
  `name` varchar(255) NOT NULL,
  `active` tinyint(1) NOT NULL,
  `site_id` int(11) NOT NULL,
  `enable_data_sharing_consent` tinyint(1) NOT NULL,
  `enforce_data_sharing_consent` varchar(25) NOT NULL,
  `enable_audit_enrollment` tinyint(1) NOT NULL,
  `enable_audit_data_reporting` tinyint(1) NOT NULL,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `enterprise_enterprisecustomer`
--

INSERT INTO `enterprise_enterprisecustomer` VALUES
  ('2017-08-07 16:15:18.806870','2018-01-26 19:17:08.511030','0381d3cb033846d48a5cb1475b589d7f','Enterprise 1',1,1,1,'at_enrollment',0,0),
  ('2017-12-13 22:30:05.926863','2017-12-13 22:30:10.361198','03fc6c3a33d84580842576922275ca6f','2nd Enterprise',1,1,1,'at_enrollment',0,0);
