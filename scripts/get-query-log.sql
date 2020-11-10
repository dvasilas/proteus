SELECT row_ids, ts_local
FROM stateDB.query_log
INTO outfile 'query-log.txt'
FIELDS TERMINATED by ','
LINES TERMINATED by '\n' ;