DROP TABLE IF EXISTS `database_import_test_table`;

CREATE TABLE `database_import_test_table` (`id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(255) NOT NULL, `meta` longtext NOT NULL, `width` smallint(6), `allow_certificate` tinyint(1) NOT NULL, `user_id` bigint(20) unsigned NOT NULL, `profile_image_uploaded_at` datetime, `change_date` datetime(6), `total_amount` double NOT NULL, `expiration_date` date, `ip_address` varchar(39), `unit_cost` decimal(30,2) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=16061337 DEFAULT CHARSET=utf8;

INSERT INTO `database_import_test_table` VALUES (1, 'John Doe', '{"6002x_exit_response": {"rating": ["7 - Absolutely amazing. I learned a great deal."]}}', 1280, 0, 12487982, '2015-04-24 02:04:20', '2016-08-31 15:31:06.013344', 3106.5, '2013-11-09', '10.0.0.1', 100.00);

