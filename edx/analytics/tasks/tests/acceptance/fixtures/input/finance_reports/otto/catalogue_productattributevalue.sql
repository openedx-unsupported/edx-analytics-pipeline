--
-- Table structure for table `catalogue_productattributevalue`
--

DROP TABLE IF EXISTS `catalogue_productattributevalue`;
CREATE TABLE `catalogue_productattributevalue` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `value_text` longtext,
  `value_integer` int(11) DEFAULT NULL,
  `value_boolean` tinyint(1) DEFAULT NULL,
  `value_float` double DEFAULT NULL,
  `value_richtext` longtext,
  `value_date` date DEFAULT NULL,
  `value_file` varchar(255) DEFAULT NULL,
  `value_image` varchar(255) DEFAULT NULL,
  `entity_object_id` int(10) unsigned DEFAULT NULL,
  `attribute_id` int(11) NOT NULL,
  `entity_content_type_id` int(11) DEFAULT NULL,
  `product_id` int(11) NOT NULL,
  `value_option_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `catalogue_productattributevalue`
--

INSERT INTO `catalogue_productattributevalue` VALUES
  (9588,'verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,3,NULL,5334,NULL),
  (9589,'DemoX/DemoCourse/1T2015',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,5334,NULL),
  (9590,NULL,NULL,1,NULL,NULL,NULL,'','',NULL,2,NULL,5334,NULL),
  (18,'course-v1:AccTest+Otto+Verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,11,NULL),
  (19,'course-v1:AccTest+Otto+Verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,12,NULL),
  (20,NULL,NULL,0,NULL,NULL,NULL,'','',NULL,2,NULL,12,NULL),
  (21,'verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,3,NULL,13,NULL),
  (22,'course-v1:AccTest+Otto+Verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,13,NULL),
  (23,NULL,NULL,1,NULL,NULL,NULL,'','',NULL,2,NULL,13,NULL),
  (24,'course-v1:AccTest+Otto+Professional',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,14,NULL),
  (25,'professional',NULL,NULL,NULL,NULL,NULL,'','',NULL,3,NULL,15,NULL),
  (26,'course-v1:AccTest+Otto+Professional',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,15,NULL),
  (27,NULL,NULL,0,NULL,NULL,NULL,'','',NULL,2,NULL,15,NULL),
  (28,'course-v1:AccTest+Otto+Credit',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,16,NULL),
  (29,'course-v1:AccTest+Otto+Credit',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,17,NULL),
  (30,NULL,NULL,0,NULL,NULL,NULL,'','',NULL,2,NULL,17,NULL),
  (31,'verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,3,NULL,18,NULL),
  (32,'course-v1:AccTest+Otto+Credit',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,18,NULL),
  (33,NULL,NULL,1,NULL,NULL,NULL,'','',NULL,2,NULL,18,NULL),
  (34,'credit',NULL,NULL,NULL,NULL,NULL,'','',NULL,3,NULL,19,NULL),
  (35,'course-v1:AccTest+Otto+Credit',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,19,NULL),
  (36,NULL,100,NULL,NULL,NULL,NULL,'','',NULL,5,NULL,19,NULL),
  (37,'TestU',NULL,NULL,NULL,NULL,NULL,'','',NULL,4,NULL,19,NULL),
  (38,NULL,NULL,1,NULL,NULL,NULL,'','',NULL,2,NULL,19,NULL),
  (39,NULL,NULL,NULL,NULL,NULL,NULL,'','',5,10,113,20,NULL),
  (40,NULL,NULL,NULL,NULL,NULL,NULL,'','',6,10,113,21,NULL),
  (41,NULL,NULL,NULL,NULL,NULL,NULL,'','',7,10,113,22,NULL),
  (42,NULL,NULL,NULL,NULL,NULL,NULL,'','',8,10,113,23,NULL),
  (37497,'verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,21,NULL,29440,NULL),
  (37498,'38e5ea5533db4438a3846d90b4b31c33',NULL,NULL,NULL,NULL,NULL,'','',NULL,20,NULL,29440,NULL);
