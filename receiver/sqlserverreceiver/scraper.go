// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver"

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sqlquery"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver/internal/metadata"
)

const (
	computerNameKey = "computer_name"
	instanceNameKey = "sql_instance"
)

type sqlServerScraperHelper struct {
	id                 component.ID
	sqlQuery           string
	instanceName       string
	scrapeCfg          scraperhelper.ControllerConfig
	clientProviderFunc sqlquery.ClientProviderFunc
	dbProviderFunc     sqlquery.DbProviderFunc
	logger             *zap.Logger
	telemetry          sqlquery.TelemetryConfig
	client             sqlquery.DbClient
	db                 *sql.DB
	mb                 *metadata.MetricsBuilder
}

var _ scraperhelper.Scraper = (*sqlServerScraperHelper)(nil)

func newSQLServerScraper(id component.ID,
	query string,
	instanceName string,
	scrapeCfg scraperhelper.ControllerConfig,
	logger *zap.Logger,
	telemetry sqlquery.TelemetryConfig,
	dbProviderFunc sqlquery.DbProviderFunc,
	clientProviderFunc sqlquery.ClientProviderFunc,
	mb *metadata.MetricsBuilder) *sqlServerScraperHelper {

	return &sqlServerScraperHelper{
		id:                 id,
		sqlQuery:           query,
		instanceName:       instanceName,
		scrapeCfg:          scrapeCfg,
		logger:             logger,
		telemetry:          telemetry,
		dbProviderFunc:     dbProviderFunc,
		clientProviderFunc: clientProviderFunc,
		mb:                 mb,
	}
}

func (s *sqlServerScraperHelper) ID() component.ID {
	return s.id
}

func (s *sqlServerScraperHelper) Start(context.Context, component.Host) error {
	var err error
	s.db, err = s.dbProviderFunc()
	if err != nil {
		return fmt.Errorf("failed to open Db connection: %w", err)
	}
	s.client = s.clientProviderFunc(sqlquery.DbWrapper{Db: s.db}, s.sqlQuery, s.logger, s.telemetry)

	return nil
}

func (s *sqlServerScraperHelper) Scrape(ctx context.Context) (pmetric.Metrics, error) {
	var err error

	switch s.sqlQuery {
	case getSQLQuery(s.instanceName):
		err = s.recordSQL(ctx)
	case getQQueryPlan():
		err = s.recordQueryPlan(ctx)
	case getSQLServerDatabaseIOQuery(s.instanceName):
		err = s.recordDatabaseIOMetrics(ctx)
	case getSQLServerPerformanceCounterQuery(s.instanceName):
		err = s.recordDatabasePerfCounterMetrics(ctx)
	case getSQLServerPropertiesQuery(s.instanceName):
		err = s.recordDatabaseStatusMetrics(ctx)
	default:
		return pmetric.Metrics{}, fmt.Errorf("Attempted to get metrics from unsupported query: %s", s.sqlQuery)
	}

	if err != nil {
		return pmetric.Metrics{}, err
	}

	return s.mb.Emit(), nil
}

func (s *sqlServerScraperHelper) Shutdown(_ context.Context) error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *sqlServerScraperHelper) recordDatabaseIOMetrics(ctx context.Context) error {
	const databaseNameKey = "database_name"
	const physicalFilenameKey = "physical_filename"
	const logicalFilenameKey = "logical_filename"
	const fileTypeKey = "file_type"
	const readLatencyMsKey = "read_latency_ms"
	const writeLatencyMsKey = "write_latency_ms"
	const readCountKey = "reads"
	const writeCountKey = "writes"
	const readBytesKey = "read_bytes"
	const writeBytesKey = "write_bytes"

	rows, err := s.client.QueryRows(ctx)
	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return fmt.Errorf("sqlServerScraperHelper: %w", err)
		}
	}

	var errs []error
	now := pcommon.NewTimestampFromTime(time.Now())
	var val float64
	for i, row := range rows {
		rb := s.mb.NewResourceBuilder()
		rb.SetSqlserverComputerName(row[computerNameKey])
		rb.SetSqlserverDatabaseName(row[databaseNameKey])
		rb.SetSqlserverInstanceName(row[instanceNameKey])

		val, err = strconv.ParseFloat(row[readLatencyMsKey], 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		} else {
			s.mb.RecordSqlserverDatabaseLatencyDataPoint(now, val/1e3, row[physicalFilenameKey], row[logicalFilenameKey], row[fileTypeKey], metadata.AttributeDirectionRead)
		}

		val, err = strconv.ParseFloat(row[writeLatencyMsKey], 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		} else {
			s.mb.RecordSqlserverDatabaseLatencyDataPoint(now, val/1e3, row[physicalFilenameKey], row[logicalFilenameKey], row[fileTypeKey], metadata.AttributeDirectionWrite)
		}

		errs = append(errs, s.mb.RecordSqlserverDatabaseOperationsDataPoint(now, row[readCountKey], row[physicalFilenameKey], row[logicalFilenameKey], row[fileTypeKey], metadata.AttributeDirectionRead))
		errs = append(errs, s.mb.RecordSqlserverDatabaseOperationsDataPoint(now, row[writeCountKey], row[physicalFilenameKey], row[logicalFilenameKey], row[fileTypeKey], metadata.AttributeDirectionWrite))
		errs = append(errs, s.mb.RecordSqlserverDatabaseIoDataPoint(now, row[readBytesKey], row[physicalFilenameKey], row[logicalFilenameKey], row[fileTypeKey], metadata.AttributeDirectionRead))
		errs = append(errs, s.mb.RecordSqlserverDatabaseIoDataPoint(now, row[writeBytesKey], row[physicalFilenameKey], row[logicalFilenameKey], row[fileTypeKey], metadata.AttributeDirectionWrite))

		s.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}

	if len(rows) == 0 {
		s.logger.Info("SQLServerScraperHelper: No rows found by query")
	}

	return errors.Join(errs...)
}

func (s *sqlServerScraperHelper) recordDatabasePerfCounterMetrics(ctx context.Context) error {
	const counterKey = "counter"
	const valueKey = "value"
	// Constants are the columns for metrics from query
	const batchRequestRate = "Batch Requests/sec"
	const bufferCacheHitRatio = "Buffer cache hit ratio"
	const diskReadIOThrottled = "Disk Read IO Throttled/sec"
	const diskWriteIOThrottled = "Disk Write IO Throttled/sec"
	const lockWaits = "Lock Waits/sec"
	const processesBlocked = "Processes blocked"
	const sqlCompilationRate = "SQL Compilations/sec"
	const sqlReCompilationsRate = "SQL Re-Compilations/sec"
	const userConnCount = "User Connections"

	rows, err := s.client.QueryRows(ctx)

	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return fmt.Errorf("sqlServerScraperHelper: %w", err)
		}
	}

	var errs []error
	now := pcommon.NewTimestampFromTime(time.Now())
	for i, row := range rows {
		rb := s.mb.NewResourceBuilder()
		rb.SetSqlserverComputerName(row[computerNameKey])
		rb.SetSqlserverInstanceName(row[instanceNameKey])

		switch row[counterKey] {
		case batchRequestRate:
			val, err := strconv.ParseFloat(row[valueKey], 64)
			if err != nil {
				err = fmt.Errorf("row %d: %w", i, err)
				errs = append(errs, err)
			} else {
				s.mb.RecordSqlserverBatchRequestRateDataPoint(now, val)
			}
		case bufferCacheHitRatio:
			val, err := strconv.ParseFloat(row[valueKey], 64)
			if err != nil {
				err = fmt.Errorf("row %d: %w", i, err)
				errs = append(errs, err)
			} else {
				s.mb.RecordSqlserverPageBufferCacheHitRatioDataPoint(now, val)
			}
		case diskReadIOThrottled:
			errs = append(errs, s.mb.RecordSqlserverResourcePoolDiskThrottledReadRateDataPoint(now, row[valueKey]))
		case diskWriteIOThrottled:
			errs = append(errs, s.mb.RecordSqlserverResourcePoolDiskThrottledWriteRateDataPoint(now, row[valueKey]))
		case lockWaits:
			val, err := strconv.ParseFloat(row[valueKey], 64)
			if err != nil {
				err = fmt.Errorf("row %d: %w", i, err)
				errs = append(errs, err)
			} else {
				s.mb.RecordSqlserverLockWaitRateDataPoint(now, val)
			}
		case processesBlocked:
			errs = append(errs, s.mb.RecordSqlserverProcessesBlockedDataPoint(now, row[valueKey]))
		case sqlCompilationRate:
			val, err := strconv.ParseFloat(row[valueKey], 64)
			if err != nil {
				err = fmt.Errorf("row %d: %w", i, err)
				errs = append(errs, err)
			} else {
				s.mb.RecordSqlserverBatchSQLCompilationRateDataPoint(now, val)
			}
		case sqlReCompilationsRate:
			val, err := strconv.ParseFloat(row[valueKey], 64)
			if err != nil {
				err = fmt.Errorf("row %d: %w", i, err)
				errs = append(errs, err)
			} else {
				s.mb.RecordSqlserverBatchSQLRecompilationRateDataPoint(now, val)
			}
		case userConnCount:
			val, err := strconv.ParseInt(row[valueKey], 10, 64)
			if err != nil {
				err = fmt.Errorf("row %d: %w", i, err)
				errs = append(errs, err)
			} else {
				s.mb.RecordSqlserverUserConnectionCountDataPoint(now, val)
			}
		}

		s.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}

	return errors.Join(errs...)
}

func (s *sqlServerScraperHelper) recordDatabaseStatusMetrics(ctx context.Context) error {
	// Constants are the column names of the database status
	const dbOnline = "db_online"
	const dbRestoring = "db_restoring"
	const dbRecovering = "db_recovering"
	const dbPendingRecovery = "db_recoveryPending"
	const dbSuspect = "db_suspect"
	const dbOffline = "db_offline"

	rows, err := s.client.QueryRows(ctx)

	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return fmt.Errorf("sqlServerScraperHelper failed getting metric rows: %w", err)
		}
	}

	var errs []error
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, row := range rows {
		rb := s.mb.NewResourceBuilder()
		rb.SetSqlserverComputerName(row[computerNameKey])
		rb.SetSqlserverInstanceName(row[instanceNameKey])

		errs = append(errs, s.mb.RecordSqlserverDatabaseCountDataPoint(now, row[dbOnline], metadata.AttributeDatabaseStatusOnline))
		errs = append(errs, s.mb.RecordSqlserverDatabaseCountDataPoint(now, row[dbRestoring], metadata.AttributeDatabaseStatusRestoring))
		errs = append(errs, s.mb.RecordSqlserverDatabaseCountDataPoint(now, row[dbRecovering], metadata.AttributeDatabaseStatusRecovering))
		errs = append(errs, s.mb.RecordSqlserverDatabaseCountDataPoint(now, row[dbPendingRecovery], metadata.AttributeDatabaseStatusPendingRecovery))
		errs = append(errs, s.mb.RecordSqlserverDatabaseCountDataPoint(now, row[dbSuspect], metadata.AttributeDatabaseStatusSuspect))
		errs = append(errs, s.mb.RecordSqlserverDatabaseCountDataPoint(now, row[dbOffline], metadata.AttributeDatabaseStatusOffline))

		s.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}

	return errors.Join(errs...)
}

func (s *sqlServerScraperHelper) recordSQL(ctx context.Context) error {
	// Constants are the column names of the database status
	const now = "now"
	const queryStart = "query_start"
	const userName = "user_name"
	const lastRequestStartTime = "last_request_start_time"
	const databaseName = "database_name"
	const id = "id"
	const lastRequestEndTime = "last_request_end_time"
	const sessionStatus = "session_status"
	const requestStatus = "request_status"
	const statementText = "statement_text"
	const text = "text"
	const clientPort = "client_port"
	const clientAddress = "client_address"
	const hostName = "host_name"
	const programName = "program_name"
	const isUserProcess = "is_user_process"
	const command = "command"
	const blockingSessionId = "blocking_session_id"
	const waitType = "wait_type"
	const waitTime = "wait_time"
	const lastWaitTime = "last_wait_time"
	const waitResource = "wait_resource"
	const openTransactionCount = "open_transaction_count"
	const transactionId = "transaction_id"
	const percentComplete = "percent_complete"
	const estimatedCompletionTime = "estimated_completion_time"
	const cpuTime = "cpu_time"
	const totalElapsedTime = "total_elapsed_time"
	const reads = "reads"
	const writes = "writes"
	const logicalReads = "logical_reads"
	const transactionIsolationLevel = "transaction_isolation_level"
	const lockTimeout = "lock_timeout"
	const deadlockPriority = "deadlock_priority"
	const queryHash = "query_hash"
	const queryPlanHash = "query_plan_hash"
	const contextInfo = "context_info"

	rows, err := s.client.QueryRows(ctx)

	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			//s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return fmt.Errorf("sqlServerScraperHelper failed getting metric rows: %w", err)
		}
	}
	var errs []error
	for _, row := range rows {
		// TODO
		rb := s.mb.NewResourceBuilder()

		//s.logger.Info(fmt.Sprintf("%v: %v", now, row[now]))
		//s.logger.Info(fmt.Sprintf("%v: %v", queryStart, row[queryStart]))
		//s.logger.Info(fmt.Sprintf("%v: %v", userName, row[userName]))
		//s.logger.Info(fmt.Sprintf("%v: %v", lastRequestStartTime, row[lastRequestStartTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", databaseName, row[databaseName]))
		//s.logger.Info(fmt.Sprintf("%v: %v", id, row[id]))
		//s.logger.Info(fmt.Sprintf("%v: %v", lastRequestEndTime, row[lastRequestEndTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", sessionStatus, row[sessionStatus]))
		//s.logger.Info(fmt.Sprintf("%v: %v", requestStatus, row[requestStatus]))
		//s.logger.Info(fmt.Sprintf("%v: %v", statementText, row[statementText]))
		//s.logger.Info(fmt.Sprintf("%v: %v", text, row[text]))
		//s.logger.Info(fmt.Sprintf("%v: %v", clientPort, row[clientPort]))
		//s.logger.Info(fmt.Sprintf("%v: %v", clientAddress, row[clientAddress]))
		//s.logger.Info(fmt.Sprintf("%v: %v", hostName, row[hostName]))
		//s.logger.Info(fmt.Sprintf("%v: %v", programName, row[programName]))
		//s.logger.Info(fmt.Sprintf("%v: %v", isUserProcess, row[isUserProcess]))
		//s.logger.Info(fmt.Sprintf("%v: %v", command, row[command]))
		//s.logger.Info(fmt.Sprintf("%v: %v", blockingSessionId, row[blockingSessionId]))
		//s.logger.Info(fmt.Sprintf("%v: %v", waitType, row[waitType]))
		//s.logger.Info(fmt.Sprintf("%v: %v", waitTime, row[waitTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", lastWaitTime, row[lastWaitTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", waitResource, row[waitResource]))
		//s.logger.Info(fmt.Sprintf("%v: %v", openTransactionCount, row[openTransactionCount]))
		//s.logger.Info(fmt.Sprintf("%v: %v", transactionId, row[transactionId]))
		//s.logger.Info(fmt.Sprintf("%v: %v", percentComplete, row[percentComplete]))
		//s.logger.Info(fmt.Sprintf("%v: %v", estimatedCompletionTime, row[estimatedCompletionTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", cpuTime, row[cpuTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", totalElapsedTime, row[totalElapsedTime]))
		//s.logger.Info(fmt.Sprintf("%v: %v", reads, row[reads]))
		//s.logger.Info(fmt.Sprintf("%v: %v", writes, row[writes]))
		//s.logger.Info(fmt.Sprintf("%v: %v", logicalReads, row[logicalReads]))
		//s.logger.Info(fmt.Sprintf("%v: %v", transactionIsolationLevel, row[transactionIsolationLevel]))
		//s.logger.Info(fmt.Sprintf("%v: %v", lockTimeout, row[lockTimeout]))
		//s.logger.Info(fmt.Sprintf("%v: %v", deadlockPriority, row[deadlockPriority]))
		//s.logger.Info(fmt.Sprintf("%v: %v", queryHash, string(row[queryHash])[:]))
		s.logger.Info(fmt.Sprintf("%v: %v", queryPlanHash, hex.EncodeToString([]byte(row[queryPlanHash]))))
		//s.logger.Info(fmt.Sprintf("%v: %v", contextInfo, row[contextInfo]))
		//
		//s.mb.RecordSqlserverQuerySampleDataPoint(
		//	pcommon.NewTimestampFromTime(time.Now()),
		//	1,
		//	row[now], row[queryStart], row[userName], row[lastRequestStartTime], row[databaseName], row[id], row[lastRequestEndTime], row[sessionStatus], row[requestStatus], row[statementText], row[text], row[clientPort], row[clientAddress], row[hostName], row[programName], row[isUserProcess], row[command], row[blockingSessionId], row[waitType], row[waitTime], row[lastWaitTime], row[waitResource], row[openTransactionCount], row[transactionId], row[percentComplete], row[estimatedCompletionTime], row[cpuTime], row[totalElapsedTime], row[reads], row[writes], row[logicRead], row[transactionIsolationLevel], row[lockTimeout], row[deadlockPriority], row[queryHash], row[queryPlanHash], row[contextInfo],
		//)

		s.mb.RecordSqlserverQuerySample2DataPoint(
			pcommon.NewTimestampFromTime(time.Now()),
			1,
			row[statementText],
		)
		//s.logger.Info("Recorded metrics...")
		var resource = rb.Emit()
		var attributes = resource.Attributes()
		attributes.PutStr("statement", row[statementText])
		attributes.PutStr("query_hash", hex.EncodeToString([]byte(row[queryHash])))
		attributes.PutStr("query_plan_hash", hex.EncodeToString([]byte(row[queryPlanHash])))
		attributes.PutStr("isplanquery", "no")

		// Add metrics
		attributes.PutStr("wait_type", row[waitType])
		v, err := strconv.ParseInt(row[waitTime], 10, 64)
		if err == nil {
			attributes.PutInt("wait_time", v)
		}
		attributes.PutStr("wait_resource", row[waitResource])
		v, err = strconv.ParseInt(row[totalElapsedTime], 10, 64)
		if err == nil {
			attributes.PutInt("total_elapsed_time", v)
		}
		v, err = strconv.ParseInt(row[cpuTime], 10, 64)
		if err == nil {
			attributes.PutInt("cpu_time", v)
		}
		v, err = strconv.ParseInt(row[logicalReads], 10, 64)
		if err == nil {
			attributes.PutInt(logicalReads, v)
		} else {
			attributes.PutInt(logicalReads, 0)
		}
		// TODO
		attributes.PutInt("logical_writes", 0)
		s.mb.EmitForResource(metadata.WithResource(resource))
	}

	return errors.Join(errs...)
}

func (s *sqlServerScraperHelper) recordQueryPlan(ctx context.Context) error {
	// Constants are the column names of the database status

	const queryPlan = "query_plan"
	const queryHash = "query_hash"
	const queryPlanHash = "query_plan_hash"

	rows, err := s.client.QueryRows(ctx)

	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			//s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return fmt.Errorf("sqlServerScraperHelper failed getting metric rows: %w", err)
		}
	}

	var errs []error
	for _, row := range rows {
		// TODO
		rb := s.mb.NewResourceBuilder()
		s.mb.RecordSqlserverQuerySample3DataPoint(
			pcommon.NewTimestampFromTime(time.Now()),
			1,
			row[queryPlan],
		)
		//s.logger.Info("Recorded metrics...")
		var resource = rb.Emit()
		var attributes = resource.Attributes()
		attributes.PutStr("query_plan", row[queryPlan])
		attributes.PutStr("query_hash", hex.EncodeToString([]byte(row[queryHash])))
		attributes.PutStr("query_plan_hash", hex.EncodeToString([]byte(row[queryPlanHash])))
		attributes.PutStr("isplanquery", "yes")

		s.mb.EmitForResource(metadata.WithResource(resource))
	}

	return errors.Join(errs...)
}
