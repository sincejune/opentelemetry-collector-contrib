SELECT
  calls,
  datname,
  max_exec_time,
  max_plan_time,
  mean_exec_time,
  mean_plan_time,
  min_exec_time,
  min_plan_time,
  query,
  queryid::TEXT,
  rolname,
  rows::TEXT,
  total_exec_time,
  total_plan_time
FROM
  pg_stat_statements as pg_stat_statements
  LEFT JOIN pg_roles ON pg_stat_statements.userid = pg_roles.oid
  LEFT JOIN pg_database ON pg_stat_statements.dbid = pg_database.oid
WHERE
  query != '<insufficient privilege>'
LIMIT 31;