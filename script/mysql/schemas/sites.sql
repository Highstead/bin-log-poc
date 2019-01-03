DROP DATABASE IF EXISTS sales;
CREATE DATABASE sales;
USE sales;

CREATE TABLE sales (
  `id` bigint(20) AUTO_INCREMENT,
  `happened_at` datetime(6),
  `order_updated_at` TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
  `currency` varchar(255),
  `amount_displayed` decimal(20,2) DEFAULT '0.00',
  `tax_amount` decimal(20,2) DEFAULT '0.00',
  `discount_percent` decimal(20,2) DEFAULT '0.00',
  `created_at` datetime(6),
  `updated_at` TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
  `order_adjustment_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
