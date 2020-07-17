SELECT row_ids, ts_local
FROM stateDB.stateTableJoin_query_ts
INTO outfile 'query-log.txt'
FIELDS TERMINATED by ','
LINES TERMINATED by '\n' ;