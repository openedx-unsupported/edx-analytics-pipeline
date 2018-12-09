DROP TABLE IF EXISTS `course_modes_coursemode`;
CREATE TABLE `course_modes_coursemode` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `course_id` varchar(255) NOT NULL,
  `mode_slug` varchar(100) NOT NULL,
  `mode_display_name` varchar(255) NOT NULL,
  `min_price` int(11) NOT NULL,
  `currency` varchar(8) NOT NULL,
  `expiration_datetime` datetime DEFAULT NULL,
  `expiration_date` date DEFAULT NULL,
  `suggested_prices` varchar(255) NOT NULL,
  `description` longtext,
  `sku` varchar(255) DEFAULT NULL,
  `expiration_datetime_is_explicit` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `course_modes_coursemode`
--

INSERT INTO `course_modes_coursemode` VALUES
  (1,'eDemoX/DemoCourse/1T2015','verified','Verified Certificate',1,'usd',NULL,NULL,'5,20,100','','0F12F00',0),
  (9,'course-v1:AccTest+ShoppingCart+Verified','honor','Test SC Verified Option',0,'usd',NULL,NULL,'',NULL,'',0),
  (10,'course-v1:AccTest+ShoppingCart+Verified','verified','Test SC Verified Option',512,'usd',NULL,NULL,'',NULL,'',0),
  (11,'course-v1:AccTest+ShoppingCart+Paid','honor','Test SC Paid Course',128,'usd',NULL,NULL,'',NULL,'',0),
  (12,'course-v1:AccTest+Otto+Verified','verified','Verified Certificate',256,'usd',NULL,NULL,'',NULL,'D354B6A',0),
  (13,'course-v1:AccTest+Otto+Verified','audit','Audit',0,'usd',NULL,NULL,'',NULL,'5C89334',0),
  (14,'course-v1:AccTest+Otto+Professional','no-id-professional','Professional Education',1000,'usd',NULL,NULL,'',NULL,'CF9A708',0),
  (15,'course-v1:AccTest+Otto+Credit','credit','Credit',100,'usd',NULL,NULL,'',NULL,'11A9662',0),
  (16,'course-v1:AccTest+Otto+Credit','verified','Verified Certificate',100,'usd',NULL,NULL,'',NULL,'7F65F1F',0),
  (17,'course-v1:AccTest+Otto+Credit','audit','Audit',0,'usd',NULL,NULL,'',NULL,'AFF2417',0),
  (3850,'BerkeleyX/CS-184.1x/2013_October','verified','Verified Certificate',100,'usd',NULL,NULL,'',NULL,'FFA4773',0);
