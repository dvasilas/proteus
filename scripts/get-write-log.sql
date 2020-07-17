SELECT row_id, ts, ts_local
FROM
(
  SELECT row_id, ts, ts_local FROM stateDB.stateTableJoin_ts WHERE ts_local >
  (
    SELECT ts_local FROM stateDB.stateTableJoin_ts where ts is null
  )
) as t
INTO outfile 'write-log.txt'
FIELDS TERMINATED by ','
LINES TERMINATED by '\n' ;