--
-- Table structure for table `shoppingcart_certificateitem`
--

DROP TABLE IF EXISTS `shoppingcart_certificateitem`;
CREATE TABLE `shoppingcart_certificateitem` (
  `orderitem_ptr_id` int(11) NOT NULL,
  `course_id` varchar(128) NOT NULL,
  `course_enrollment_id` int(11) NOT NULL,
  `mode` varchar(50) NOT NULL,
  PRIMARY KEY (`orderitem_ptr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `shoppingcart_certificateitem`
--

INSERT INTO `shoppingcart_certificateitem` VALUES 
  (4,'DemoX/DemoCourse/1T2015',1,'verified'),
  (31,'course-v1:AccTest+ShoppingCart+Verified',30,'verified'),
  (32,'course-v1:AccTest+ShoppingCart+Verified',30,'verified');
