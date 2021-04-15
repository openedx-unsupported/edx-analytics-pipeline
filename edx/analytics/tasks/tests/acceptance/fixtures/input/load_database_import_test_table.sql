DROP TABLE IF EXISTS `database_import_test_table`;

CREATE TABLE `database_import_test_table` (`id` int(11) NOT NULL AUTO_INCREMENT, `name` varchar(255) NOT NULL, `meta` longtext NOT NULL, `width` smallint(6), `user_id` bigint(20) unsigned NOT NULL, `field_to_exclude` varchar(255), `profile_image_uploaded_at` datetime, `change_date` datetime(6), `total_amount` double NOT NULL, `expiration_date` date, `ip_address` varchar(39), `unit_cost` decimal(30,2) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=16061337 DEFAULT CHARSET=utf8;

INSERT INTO `database_import_test_table` VALUES (1, 'John Doe', '{"6002x_exit_response": {"rating": ["7 - Absolutely amazing. I learned a great deal."]}}', 1280, 12487982, 'This should be excluded.', '2015-04-24 02:04:20', '2016-08-31 15:31:06.013344', 3106.5, '2013-11-09', '10.0.0.1', 100.00);
INSERT INTO `database_import_test_table` VALUES (2, '\\', '{"6002x_\\exit_response": {"rating": ["1 - Absolutely disappointing."]}}', 128, 12487983, 'This should also be excluded.', '2015-04-24 01:03:25', '2016-08-31 15:31:06.013344', 3106.49, '2015-11-09', '10.0.0.1', 500.00);

