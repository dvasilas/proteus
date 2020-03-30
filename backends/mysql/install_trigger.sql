CREATE DATABASE IF NOT EXISTS db1;

USE db1;

CREATE TABLE IF NOT EXISTS trig_test (
    id int not null auto_increment PRIMARY KEY,
    random_data varchar(255) not null
);

DROP FUNCTION IF EXISTS sys_exec;
CREATE FUNCTION sys_exec RETURNS INT SONAME 'lib_sys_exec.so';

DELIMITER $
DROP TRIGGER IF EXISTS `test_after_insert` $
CREATE TRIGGER `test_after_insert`
AFTER INSERT ON `trig_test`  FOR EACH ROW
BEGIN
  DECLARE cmd CHAR(255);
  DECLARE result int(10);
  SET cmd = CONCAT('python /opt/mysql_trigger/trigger.py ', NEW.id, ' ', New.random_data);
  SET result = sys_exec(cmd);
END;
$
DELIMITER ;