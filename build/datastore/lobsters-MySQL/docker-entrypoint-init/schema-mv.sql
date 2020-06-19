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
  `vote_count` bigint DEFAULT 0,
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
  `vote_count` bigint DEFAULT 0,
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

delimiter #
DROP TRIGGER IF EXISTS `stories_votecount`;
CREATE TRIGGER `stories_votecount`
AFTER INSERT ON `votes`
FOR EACH ROW
BEGIN
  DECLARE oldVC integer;
  DECLARE newVC integer;
  IF NEW.comment_id IS NULL THEN
    SET oldVC = (SELECT `vote_count` from `stories` where `id` = NEW.story_id);
    SET newVC = oldVC + NEW.vote;
    UPDATE `stories` SET `vote_count` = newVC  where `id` = NEW.story_id;
  ELSE
    SET oldVC = (SELECT `vote_count` from `comments` where `id` = NEW.comment_id);
    SET newVC = oldVC + NEW.vote;
    UPDATE `comments` SET `vote_count` = newVC  where `id` = NEW.comment_id;
  END IF;
END#
delimiter ;