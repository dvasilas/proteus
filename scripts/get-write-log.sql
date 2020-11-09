SELECT row_id, ts, ts_local
FROM
(
  SELECT row_id, ts, ts_local FROM stateDB.stories50350_write_log WHERE ts_local >
  (
    SELECT ts_local FROM stateDB.stories50350_write_log where ts is null
  )
) as t
INTO outfile 'write-log.txt'
FIELDS TERMINATED by ','
LINES TERMINATED by '\n' ;