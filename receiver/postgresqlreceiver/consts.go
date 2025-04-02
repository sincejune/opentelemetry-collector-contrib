package postgresqlreceiver

const (
	DB_ATTRIBUTE_PREFIX         = "postgresql."
	QUERYID_COLUMN_NAME         = "queryid"
	TOTAL_EXEC_TIME_COLUMN_NAME = "total_exec_time"
	TOTAL_PLAN_TIME_COLUMN_NAME = "total_plan_time"
	CALLS_COLUMN_NAME           = "calls"
	ROWS_COLUMN_NAME            = "rows"
)

const (
	EXECUTION_TIME_SUFFIX = "-execution-time"
	PLAN_TIME_SUFFIX      = "-plan-time"
	CALLS_SUFFIX          = "-calls"
	ROWS_SUFFIX           = "-rows"
)
