-- MySQL dump 10.13  Distrib 5.6.19, for osx10.9 (x86_64)
--
-- Host: localhost    Database: analytics
-- ------------------------------------------------------
-- Server version	5.6.19
--
-- Table structure for table `courseware_studentmodule`
--
--
DROP TABLE IF EXISTS `courseware_studentmodule`;

CREATE TABLE `courseware_studentmodule` (`id` int(11) NOT NULL AUTO_INCREMENT, `module_type` varchar(32) NOT NULL DEFAULT 'problem', `module_id` varchar(255) NOT NULL, `student_id` int(11) NOT NULL, `state` longtext, `grade` double DEFAULT NULL, `created` datetime NOT NULL, `modified` datetime NOT NULL, `max_grade` double DEFAULT NULL,`done` varchar(8) NOT NULL,`course_id` varchar(255) NOT NULL, PRIMARY KEY (`id`),UNIQUE KEY `courseware_studentmodule_student_id_635d77aea1256de5_uniq` (`student_id`,`module_id`,`course_id`),KEY `courseware_studentmodule_42ff452e` (`student_id`), KEY `courseware_studentmodule_3216ff68` (`created`), KEY `courseware_studentmodule_6dff86b5` (`grade`),KEY `courseware_studentmodule_5436e97a` (`modified`),KEY `courseware_studentmodule_2d8768ff` (`module_type`),KEY `courseware_studentmodule_f53ed95e` (`module_id`),KEY `courseware_studentmodule_1923c03f` (`done`),KEY `courseware_studentmodule_ff48d8e5` (`course_id`) ) ENGINE=InnoDB AUTO_INCREMENT=97296 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `courseware_studentmodule`
--
INSERT INTO `courseware_studentmodule` VALUES (97281,'problem','i4x://stanford/analytics/problem/test1',1,'{}',0,'2014-08-22 20:36:43','2014-08-22 20:36:43',2,'na','stanford/analytics/tests'),(97283,'problem','i4x://stanford/analytics/problem/test1',2,'{}',1,'2014-08-22 20:36:43','2014-08-22 20:36:43',2,'na','stanford/analytics/tests'),(97284,'problem','i4x://stanford/analytics/problem/test1',3,'{}',2,'2014-08-22 20:36:43','2014-08-22 20:36:43',2,'na','stanford/analytics/tests'),(97285,'problem','i4x://stanford/analytics/problem/test1',4,'{}',0,'2014-08-22 20:36:43','2014-08-22 20:36:43',2,'na','stanford/analytics/tests'),(97286,'problem','i4x://stanford/analytics/problem/test1',5,'{}',0,'2014-08-22 20:36:43','2014-08-22 20:36:43',2,'na','stanford/analytics/tests'),(97288,'problem','i4x://stanford/analytics/problem/test1',6,'{}',1,'2014-08-22 20:36:43','2014-08-22 20:36:43',2,'na','stanford/analytics/tests'),(97289,'problem','i4x://stanford/analytics/problem/test2',6,'{}',1,'2014-08-22 20:36:43','2014-08-22 20:36:43',NULL,'na','stanford/analytics/tests'),(97290,'problem','i4x://stanford/analytics/problem/test2',7,'{}',1,'2014-08-22 20:36:43','2014-08-22 20:36:43',1,'na','stanford/analytics/tests'),(97291,'problem','i4x://stanford/analytics/problem/test2',8,'{}',0,'2014-08-22 20:36:43','2014-08-22 20:36:43',1,'na','stanford/analytics/tests'),(97292,'sequential','i4x://stanford/analytics/sequential/test2',8,'{}',NULL,'2014-08-22 20:36:43','2014-08-22 20:36:43',NULL,'na','stanford/analytics/tests'),(97293,'sequential','i4x://stanford/analytics/sequential/test1',9,'{}',NULL,'2014-08-22 20:36:43','2014-08-22 20:36:43',NULL,'na','stanford/analytics/tests'),(97295,'sequential','i4x://stanford/analytics/sequential/test2',9,'{}',NULL,'2014-08-22 20:36:43','2014-08-22 20:36:43',NULL,'na','stanford/analytics/tests');

-- Dump completed on 2014-08-22 13:59:58
