--
-- Table structure for table `shoppingcart_order`
--

DROP TABLE IF EXISTS `shoppingcart_order`;
CREATE TABLE `shoppingcart_order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `currency` varchar(8) NOT NULL,
  `status` varchar(32) NOT NULL,
  `purchase_time` datetime DEFAULT NULL,
  `refunded_time` datetime DEFAULT NULL,
  `bill_to_first` varchar(64) NOT NULL,
  `bill_to_last` varchar(64) NOT NULL,
  `bill_to_street1` varchar(128) NOT NULL,
  `bill_to_street2` varchar(128) NOT NULL,
  `bill_to_city` varchar(64) NOT NULL,
  `bill_to_state` varchar(8) NOT NULL,
  `bill_to_postalcode` varchar(16) NOT NULL,
  `bill_to_country` varchar(64) NOT NULL,
  `bill_to_ccnum` varchar(8) NOT NULL,
  `bill_to_cardtype` varchar(32) NOT NULL,
  `processor_reply_dump` longtext NOT NULL,
  `company_name` varchar(255),
  `company_contact_name` varchar(255),
  `company_contact_email` varchar(255),
  `recipient_name` varchar(255),
  `recipient_email` varchar(255),
  `customer_reference_number` varchar(63),
  `order_type` varchar(32) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Dumping data for table `shoppingcart_order`
--

INSERT INTO `shoppingcart_order` VALUES
  (1,11,'usd','refunded','2013-09-12 21:24:38',NULL,'Test','User','','','Cambridge','MA','02140','us','','','',NULL,NULL,NULL,NULL,NULL,NULL,'personal'),
  (12,11,'usd','purchased','2016-03-22 21:18:46.395551',NULL,'John','Doe','123 Fake Street','','Boston','MA','02139','US','1111','Visa','{\"req_transaction_type\": \"sale\", \"req_bill_to_address_city\": \"Boston\", \"reason_code\": \"100\", \"req_card_expiry_date\": \"01-2018\", \"req_bill_to_surname\": \"Doe\", \"req_card_number\": \"xxxxxxxxxxxx1111\", \"message\": \"Request was processed successfully.\", \"auth_response\": \"100\", \"bill_trans_ref_no\": \"84997128QYI23CJT\", \"auth_amount\": \"512.00\", \"auth_avs_code\": \"X\", \"decision\": \"ACCEPT\", \"req_transaction_uuid\": \"318e67993d12423bbd1afb810829a5c5\", \"auth_avs_code_raw\": \"I1\", \"auth_code\": \"888888\", \"req_bill_to_address_country\": \"US\", \"signed_field_names\": \"transaction_id,decision,req_access_key,req_profile_id,req_transaction_uuid,req_transaction_type,req_reference_number,req_amount,req_currency,req_locale,req_payment_method,req_override_custom_receipt_page,req_bill_to_forename,req_bill_to_surname,req_bill_to_email,req_bill_to_address_line1,req_bill_to_address_city,req_bill_to_address_state,req_bill_to_address_country,req_bill_to_address_postal_code,req_card_number,req_card_type,req_card_expiry_date,message,reason_code,auth_avs_code,auth_avs_code_raw,auth_response,auth_amount,auth_code,auth_trans_ref_no,auth_time,bill_trans_ref_no,signed_field_names,signed_date_time\", \"transaction_id\": \"4083599817820176195662\", \"req_card_type\": \"001\", \"req_bill_to_address_postal_code\": \"02139\", \"utf8\": \"\\u2713\", \"req_locale\": \"en\", \"req_currency\": \"usd\", \"req_bill_to_forename\": \"John\", \"req_override_custom_receipt_page\": \"http://127.0.0.1:8000/shoppingcart/postpay_callback/\", \"req_amount\": \"512.00\", \"auth_time\": \"2014-08-18T110622Z\", \"auth_trans_ref_no\": \"84997128QYI23CJT\", \"req_bill_to_email\": \"john@example.com\", \"req_access_key\": \"abcd123\", \"req_reference_number\": \"12\", \"signed_date_time\": \"2016-03-22T21:18:43Z\", \"req_profile_id\": \"0000001\", \"req_bill_to_address_line1\": \"123 Fake Street\", \"signature\": \"yyXRpxBfYWvE8yJ7hbx0OJfkhUxUJJ2r2PY91IwjULw=\", \"req_bill_to_address_state\": \"MA\", \"req_payment_method\": \"card\"}',NULL,NULL,NULL,NULL,NULL,NULL,'personal'),
  (13,11,'usd','purchased','2016-03-22 21:27:03.266322',NULL,'John','Doe','123 Fake Street','','Boston','MA','02139','US','1111','Visa','{\"req_transaction_type\": \"sale\", \"req_bill_to_address_city\": \"Boston\", \"reason_code\": \"100\", \"req_card_expiry_date\": \"01-2018\", \"req_bill_to_surname\": \"Doe\", \"req_card_number\": \"xxxxxxxxxxxx1111\", \"message\": \"Request was processed successfully.\", \"auth_response\": \"100\", \"bill_trans_ref_no\": \"84997128QYI23CJT\", \"auth_amount\": \"102.40\", \"auth_avs_code\": \"X\", \"decision\": \"ACCEPT\", \"req_transaction_uuid\": \"3097f254d2d943388df4e654cdcc0876\", \"auth_avs_code_raw\": \"I1\", \"auth_code\": \"888888\", \"req_bill_to_address_country\": \"US\", \"signed_field_names\": \"transaction_id,decision,req_access_key,req_profile_id,req_transaction_uuid,req_transaction_type,req_reference_number,req_amount,req_currency,req_locale,req_payment_method,req_override_custom_receipt_page,req_bill_to_forename,req_bill_to_surname,req_bill_to_email,req_bill_to_address_line1,req_bill_to_address_city,req_bill_to_address_state,req_bill_to_address_country,req_bill_to_address_postal_code,req_card_number,req_card_type,req_card_expiry_date,message,reason_code,auth_avs_code,auth_avs_code_raw,auth_response,auth_amount,auth_code,auth_trans_ref_no,auth_time,bill_trans_ref_no,signed_field_names,signed_date_time\", \"transaction_id\": \"4083599817820176195662\", \"req_card_type\": \"001\", \"req_bill_to_address_postal_code\": \"02139\", \"utf8\": \"\\u2713\", \"req_locale\": \"en\", \"req_currency\": \"usd\", \"req_bill_to_forename\": \"John\", \"req_override_custom_receipt_page\": \"http://127.0.0.1:8000/shoppingcart/postpay_callback/\", \"req_amount\": \"102.40\", \"auth_time\": \"2014-08-18T110622Z\", \"auth_trans_ref_no\": \"84997128QYI23CJT\", \"req_bill_to_email\": \"john@example.com\", \"req_access_key\": \"abcd123\", \"req_reference_number\": \"13\", \"signed_date_time\": \"2016-03-22T21:26:48Z\", \"req_profile_id\": \"0000001\", \"req_bill_to_address_line1\": \"123 Fake Street\", \"signature\": \"OmaeIeemDtBeNngChg/xTQtOl27YzdfksjXLIQ9+6yE=\", \"req_bill_to_address_state\": \"MA\", \"req_payment_method\": \"card\"}','yada','yada','yada@example.com','','','','business');
