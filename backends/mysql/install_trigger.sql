CREATE DATABASE IF NOT EXISTS db1;

USE db1;

CREATE TABLE IF NOT EXISTS votes (
    user_id int not null,
    story_id int not null
);

DROP FUNCTION IF EXISTS sys_exec;
CREATE FUNCTION sys_exec RETURNS INT SONAME 'lib_sys_exec.so';

DELIMITER $
DROP TRIGGER IF EXISTS `votes_insert` $
CREATE TRIGGER `votes_insert`
AFTER INSERT ON `votes`  FOR EACH ROW
BEGIN
  DECLARE cmd CHAR(255);
  DECLARE result int(10);
  SET cmd = CONCAT('python /opt/mysql_trigger/trigger.py ', NEW.user_id, ' ', NEW.story_id);
  SET result = sys_exec(cmd);
END;
$
DELIMITER ;