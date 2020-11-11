 SELECT row_id, ts, ts_local FROM stateDB.write_log
INTO outfile 'write-log.txt'
FIELDS TERMINATED by ','
LINES TERMINATED by '\n' ;
