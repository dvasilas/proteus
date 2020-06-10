CREATE DATABASE IF NOT EXISTS proteus_lobsters_db;

USE proteus_lobsters_db;

CREATE TABLE IF NOT EXISTS`users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `stories` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint unsigned NOT NULL,
  `title` varchar(150) NOT NULL DEFAULT '',
  `description` mediumtext NOT NULL,
  `short_id` varchar(6) NOT NULL DEFAULT '',
  `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `short_id` (`short_id`),
  CONSTRAINT `stories_user_id_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `comments` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `story_id` bigint unsigned NOT NULL,
  `user_id` bigint unsigned NOT NULL,
  `comment` mediumtext NOT NULL,
  `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  CONSTRAINT `comments_story_id_fk` FOREIGN KEY (`story_id`) REFERENCES `stories` (`id`),
  CONSTRAINT `comments_user_id_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `votes` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint unsigned NOT NULL,
  `story_id` bigint unsigned NOT NULL,
  `comment_id` bigint unsigned DEFAULT NULL,
  `vote` tinyint NOT NULL,
  `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  CONSTRAINT `votes_comment_id_fk` FOREIGN KEY (`comment_id`) REFERENCES `comments` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `votes_story_id_fk` FOREIGN KEY (`story_id`) REFERENCES `stories` (`id`),
  CONSTRAINT `votes_user_id_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW `stories_votecount`
  AS SELECT `story_id`, SUM(`vote`) `vote_count`, MAX(`ts`) `ts`
  FROM `votes`
  WHERE `comment_id` IS NULL
  GROUP BY `story_id`;

CREATE OR REPLACE VIEW `comments_votecount`
  AS SELECT `story_id`, `comment_id`, SUM(`vote`) `vote_count`, MAX(`ts`) `ts`
  FROM `votes`
  WHERE `comment_id` IS NOT NULL
  GROUP BY `story_id`, `comment_id`;

CREATE OR REPLACE VIEW `stories_with_votecount`
  AS SELECT `story_id`, `user_id`, `title`, `description`, `short_id`, `vote_count`, `stories_votecount`.`ts`
  FROM `stories`
  JOIN `stories_votecount`
  ON `stories`.`id` = `stories_votecount`.`story_id`;

CREATE OR REPLACE VIEW `comments_with_votecount`
  AS SELECT `id`, `comments`.`story_id`, `user_id`, `comment`, `vote_count`, `comments_votecount`.`ts`
  FROM `comments`
  JOIN `comments_votecount`
  ON `comments`.`id` = `comments_votecount`.`comment_id`;