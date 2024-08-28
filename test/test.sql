
CREATE TABLE `test` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'primary id',
  `producer` varchar(64) NOT NULL,
  `resource` varchar(32) NOT NULL,
  `action` varchar(32) NOT NULL,
  `message` text,
  `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;