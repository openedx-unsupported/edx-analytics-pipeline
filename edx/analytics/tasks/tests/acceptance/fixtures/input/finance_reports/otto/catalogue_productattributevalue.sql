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

INSERT INTO `catalogue_productattributevalue` VALUES (9588,'verified',NULL,NULL,NULL,NULL,NULL,'','',NULL,3,NULL,5334,NULL),(9589,'DemoX/DemoCourse/1T2015',NULL,NULL,NULL,NULL,NULL,'','',NULL,1,NULL,5334,NULL),(9590,NULL,NULL,1,NULL,NULL,NULL,'','',NULL,2,NULL,5334,NULL);
