// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver"

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sqlquery"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver/internal/metadata"
)

const (
	computerNameKey = "computer_name"
	instanceNameKey = "sql_instance"
)

type sqlServerScraperHelper struct {
	id                  component.ID
	sqlQuery            string
	instanceName        string
	scrapeCfg           scraperhelper.ControllerConfig
	clientProviderFunc  sqlquery.ClientProviderFunc
	dbProviderFunc      sqlquery.DbProviderFunc
	logger              *zap.Logger
	telemetry           sqlquery.TelemetryConfig
	client              sqlquery.DbClient
	db                  *sql.DB
	mb                  *metadata.MetricsBuilder
	maxQuerySampleCount uint
	lookbackTime        uint
	topQueryCount       uint
	cache               *lru.Cache[string, int64]
}

var (
	_ scraper.Metrics = (*sqlServerScraperHelper)(nil)
	_ scraper.Logs    = (*sqlServerScraperHelper)(nil)
)

func newSQLServerScraper(id component.ID,
	query string,
	instanceName string,
	scrapeCfg scraperhelper.ControllerConfig,
	logger *zap.Logger,
	telemetry sqlquery.TelemetryConfig,
	dbProviderFunc sqlquery.DbProviderFunc,
	clientProviderFunc sqlquery.ClientProviderFunc,
	mb *metadata.MetricsBuilder,
	maxQuerySampleCount uint,
	lookbackTime uint,
	topQueryCount uint,
	cache *lru.Cache[string, int64],
) *sqlServerScraperHelper {
	return &sqlServerScraperHelper{
		id:                  id,
		sqlQuery:            query,
		instanceName:        instanceName,
		scrapeCfg:           scrapeCfg,
		logger:              logger,
		telemetry:           telemetry,
		dbProviderFunc:      dbProviderFunc,
		clientProviderFunc:  clientProviderFunc,
		mb:                  mb,
		maxQuerySampleCount: maxQuerySampleCount,
		lookbackTime:        lookbackTime,
		topQueryCount:       topQueryCount,
		cache:               cache,
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

func (s *sqlServerScraperHelper) ScrapeMetrics(ctx context.Context) (pmetric.Metrics, error) {
	var err error

	switch s.sqlQuery {
	case getSQLServerDatabaseIOQuery(s.instanceName):
		err = s.recordDatabaseIOMetrics(ctx)
	case getSQLServerPerformanceCounterQuery(s.instanceName):
		err = s.recordDatabasePerfCounterMetrics(ctx)
	case getSQLServerPropertiesQuery(s.instanceName):
		err = s.recordDatabaseStatusMetrics(ctx)
	case getSQLServerQueryMetricsQuery(s.instanceName, s.maxQuerySampleCount, s.lookbackTime):
		err = s.recordDatabaseQueryMetrics(ctx, s.topQueryCount)
	default:
		return pmetric.Metrics{}, fmt.Errorf("Attempted to get metrics from unsupported query: %s", s.sqlQuery)
	}

	if err != nil {
		return pmetric.Metrics{}, err
	}

	return s.mb.Emit(), nil
}

func (s *sqlServerScraperHelper) ScrapeLogs(ctx context.Context) (plog.Logs, error) {
	switch s.sqlQuery {
	case getSQLServerQueryTextAndPlanQuery(s.instanceName, s.maxQuerySampleCount, s.lookbackTime):
		// TODO: Add a logs builder for that
		return s.recordDatabaseQueryTextAndPlan(ctx, s.topQueryCount)
	case getSQLServerQuerySamplesQuery():
		return s.recordDatabaseSampleQuery(ctx)
	default:
		return plog.Logs{}, fmt.Errorf("Attempted to get logs from unsupported query: %s", s.sqlQuery)
	}
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

func (s *sqlServerScraperHelper) recordDatabaseQueryMetrics(ctx context.Context, topQueryCount uint) error {
	// Constants are the column names of the database status
	const totalElapsedTime = "total_elapsed_time"
	const rowsReturned = "total_rows"
	const totalWorkerTime = "total_worker_time"
	const queryHash = "query_hash"
	const queryPlanHash = "query_plan_hash"
	const logicalReads = "total_logical_reads"
	const logicalWrites = "total_logical_writes"
	const physicalReads = "total_physical_reads"
	const executionCount = "execution_count"
	const totalGrant = "total_grant_kb"
	rows, err := s.client.QueryRows(ctx)
	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return fmt.Errorf("sqlServerScraperHelper failed getting metric rows: %w", err)
		}
	}
	var errs []error

	totalElapsedTimeDiffs := make([]int64, len(rows))

	for i, row := range rows {
		// human-readable query hash and query plan hash
		queryHashVal := hex.EncodeToString([]byte(row[queryHash]))
		queryPlanHashVal := hex.EncodeToString([]byte(row[queryPlanHash]))

		elapsedTime, err := strconv.ParseInt(row[totalElapsedTime], 10, 64)
		if err != nil {
			s.logger.Info(fmt.Sprintf("sqlServerScraperHelper failed getting metric rows: %s", err))
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, totalElapsedTime, elapsedTime/1000); cached && diff > 0 {
				totalElapsedTimeDiffs[i] = diff
			}
		}
	}

	rows = sortRows(rows, totalElapsedTimeDiffs)

	sort.Slice(totalElapsedTimeDiffs, func(i, j int) bool {
		return totalElapsedTimeDiffs[i] > totalElapsedTimeDiffs[j]
	})

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for i, row := range rows {
		if i >= int(topQueryCount) {
			break
		}

		// skipping as not cached
		if totalElapsedTimeDiffs[i] == 0 {
			continue
		}

		queryHashVal := hex.EncodeToString([]byte(row[queryHash]))
		queryPlanHashVal := hex.EncodeToString([]byte(row[queryPlanHash]))

		rb := s.mb.NewResourceBuilder()
		rb.SetSqlserverComputerName(row[computerNameKey])
		rb.SetSqlserverInstanceName(row[instanceNameKey])
		rb.SetSqlserverQueryHash(queryHashVal)
		rb.SetSqlserverQueryPlanHash(queryPlanHashVal)
		s.logger.Debug(fmt.Sprintf("DataRow: %v, PlanHash: %v, Hash: %v", row, queryPlanHashVal, queryHashVal))

		s.mb.RecordSqlserverQueryTotalElapsedTimeDataPoint(timestamp, totalElapsedTimeDiffs[i])

		rowsReturnVal, err := strconv.ParseInt(row[rowsReturned], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, rowsReturned, rowsReturnVal); cached && diff > 0 {
			s.mb.RecordSqlserverQueryReturnedRowsDataPoint(timestamp, diff)
		}

		logicalReadsVal, err := strconv.ParseInt(row[logicalReads], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, logicalReads, logicalReadsVal); cached && diff > 0 {
			s.mb.RecordSqlserverQueryTotalLogicalReadsDataPoint(timestamp, diff)
		}

		logicalWritesVal, err := strconv.ParseInt(row[logicalWrites], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, logicalWrites, logicalWritesVal); cached && diff > 0 {
			s.mb.RecordSqlserverQueryTotalLogicalWritesDataPoint(timestamp, diff)
		}

		physicalReadsVal, err := strconv.ParseInt(row[physicalReads], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, physicalReads, physicalReadsVal); cached && diff > 0 {
			s.mb.RecordSqlserverQueryTotalPhysicalReadsDataPoint(timestamp, diff)
		}

		totalExecutionCount, err := strconv.ParseInt(row[executionCount], 10, 64)
		if err != nil {
			s.logger.Info(fmt.Sprintf("sqlServerScraperHelper failed getting metric rows: %s", err))
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, executionCount, totalExecutionCount); cached && diff > 0 {
				s.mb.RecordSqlserverQueryExecutionCountDataPoint(timestamp, diff)
			}
		}

		workerTime, err := strconv.ParseInt(row[totalWorkerTime], 10, 64)
		if err != nil {
			s.logger.Info(fmt.Sprintf("sqlServerScraperHelper failed parsing metric total_worker_time: %s", err))
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, totalWorkerTime, workerTime/1000); cached && diff > 0 {
				s.mb.RecordSqlserverQueryTotalWorkerTimeDataPoint(timestamp, diff)
			}
		}

		memoryGranted, err := strconv.ParseInt(row[totalGrant], 10, 64)
		if err != nil {
			s.logger.Info(fmt.Sprintf("sqlServerScraperHelper failed parsing metric total_grant_kb: %s", err))
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, totalGrant, memoryGranted); cached && diff > 0 {
				s.mb.RecordSqlserverQueryTotalGrantKbDataPoint(timestamp, diff)
			}
		}

		s.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}
	return errors.Join(errs...)
}

func (s *sqlServerScraperHelper) recordDatabaseQueryTextAndPlan(ctx context.Context, topQueryCount uint) (plog.Logs, error) {
	// Constants are the column names of the database status
	const DBPrefix = "db."
	const totalElapsedTime = "total_elapsed_time"
	const rowsReturned = "total_rows"
	const totalWorkerTime = "total_worker_time"
	const queryHash = "query_hash"
	const queryPlanHash = "query_plan_hash"
	const logicalReads = "total_logical_reads"
	const logicalWrites = "total_logical_writes"
	const physicalReads = "total_physical_reads"
	const executionCount = "execution_count"
	const totalGrant = "total_grant_kb"
	const queryText = "query_text"
	const queryPlan = "query_plan"
	rows, err := s.client.QueryRows(ctx)
	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			s.logger.Warn("problems encountered getting metric rows", zap.Error(err))
		} else {
			return plog.Logs{}, fmt.Errorf("sqlServerScraperHelper failed getting rows: %w", err)
		}
	}
	var errs []error

	totalElapsedTimeDiffs := make([]int64, len(rows))

	for i, row := range rows {
		queryHashVal := hex.EncodeToString([]byte(row[queryHash]))
		queryPlanHashVal := hex.EncodeToString([]byte(row[queryPlanHash]))

		elapsedTime, err := strconv.ParseInt(row[totalElapsedTime], 10, 64)
		if err != nil {
			s.logger.Info(fmt.Sprintf("sqlServerScraperHelper failed getting rows: %s", err))
			errs = append(errs, err)
		} else {
			// we're trying to get the queries that used the most time.
			// caching the total elapsed time (in millisecond) and compare in the next scrape.
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, totalElapsedTime, elapsedTime/1000); cached && diff > 0 {
				totalElapsedTimeDiffs[i] = diff
			}
		}
	}

	// sort the rows based on the totalElapsedTimeDiffs in descending order
	rows = sortRows(rows, totalElapsedTimeDiffs)

	// sort the totalElapsedTimeDiffs in descending order as well
	sort.Slice(totalElapsedTimeDiffs, func(i, j int) bool { return totalElapsedTimeDiffs[i] > totalElapsedTimeDiffs[j] })

	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()
	resourceLog.Resource().Attributes().PutStr("db.system.type", "microsoft.sql_server")

	scopedLog := resourceLog.ScopeLogs().AppendEmpty()
	scopedLog.Scope().SetName("github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver")
	scopedLog.Scope().SetVersion("development")

	timestamp := pcommon.NewTimestampFromTime(time.Now())
	for i, row := range rows {
		// we skip the query if we already export enough queries in this run.
		if i > int(topQueryCount) {
			continue
		}

		// reporting human-readable query hash and query hash plan
		queryHashVal := hex.EncodeToString([]byte(row[queryHash]))
		queryPlanHashVal := hex.EncodeToString([]byte(row[queryPlanHash]))

		record := scopedLog.LogRecords().AppendEmpty()
		record.SetTimestamp(timestamp)

		record.Attributes().PutStr(computerNameKey, row[computerNameKey])
		record.Attributes().PutStr(instanceNameKey, row[instanceNameKey])

		record.Attributes().PutStr(DBPrefix+queryHash, queryHashVal)
		record.Attributes().PutStr(DBPrefix+queryPlanHash, queryPlanHashVal)

		s.logger.Debug(fmt.Sprintf("QueryHash: %v, PlanHash: %v, DataRow: %v", queryHashVal, queryPlanHashVal, row))

		record.Attributes().PutInt(DBPrefix+totalElapsedTime, totalElapsedTimeDiffs[i])

		// handling `total_rows`
		rowsReturnVal, err := strconv.ParseInt(row[rowsReturned], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, rowsReturned, rowsReturnVal); cached {
			record.Attributes().PutInt(DBPrefix+rowsReturned, diff)
		}

		// handling `total_logical_reads`
		logicalReadsVal, err := strconv.ParseInt(row[logicalReads], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, logicalReads, logicalReadsVal); cached {
			record.Attributes().PutInt(DBPrefix+logicalReads, diff)
		}

		// handling `total_logical_writes`
		logicalWritesVal, err := strconv.ParseInt(row[logicalWrites], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, logicalWrites, logicalWritesVal); cached {
			record.Attributes().PutInt(DBPrefix+logicalWrites, diff)
		}

		// handling `physical_reads`
		physicalReadsVal, err := strconv.ParseInt(row[physicalReads], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		}
		if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, physicalReads, physicalReadsVal); cached {
			record.Attributes().PutInt(DBPrefix+physicalReads, diff)
		}

		// handling `execution_count`
		totalExecutionCount, err := strconv.ParseInt(row[executionCount], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, executionCount, totalExecutionCount); cached {
				record.Attributes().PutInt(DBPrefix+executionCount, diff)
			}
		}

		// handle `total_worker_time`, storing milliseconds
		workerTime, err := strconv.ParseInt(row[totalWorkerTime], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, totalWorkerTime, workerTime/1000); cached {
				record.Attributes().PutInt(DBPrefix+totalWorkerTime, diff)
			}
		}

		// handle `total_grant_kb`
		memoryGranted, err := strconv.ParseInt(row[totalGrant], 10, 64)
		if err != nil {
			err = fmt.Errorf("row %d: %w", i, err)
			errs = append(errs, err)
		} else {
			if cached, diff := s.cacheAndDiff(queryHashVal, queryPlanHashVal, totalGrant, memoryGranted); cached {
				record.Attributes().PutInt(DBPrefix+totalGrant, diff)
			}
		}

		// handling `query_text`
		obfuscatedSQL, err := obfuscateSQL(row[queryText])
		if err != nil {
			s.logger.Error("failed to obfuscate query text", zap.Error(err))
			errs = append(errs, err)
		}
		record.Attributes().PutStr(DBPrefix+queryText, obfuscatedSQL)

		// handling `query_plan`
		obfuscatedQueryPlan, err := obfuscateXMLPlan(row[queryPlan])
		if err != nil {
			s.logger.Error("failed to obfuscate query plan", zap.Error(err))
			errs = append(errs, err)
		}
		record.Attributes().PutStr(DBPrefix+queryPlan, obfuscatedQueryPlan)
	}

	return logs, errors.Join(errs...)
}

func (s *sqlServerScraperHelper) recordDatabaseSampleQuery(ctx context.Context) (plog.Logs, error) {
	const DBName = "db_name"
	const clientAddress = "client_address"
	const clientPort = "client_port"
	const queryStart = "query_start"
	const sessionID = "session_id"
	const sessionStatus = "session_status"
	const hostname = "host_name"
	const command = "command"
	const statementText = "statement_text"
	const blockingSessionID = "blocking_session_id"
	const waitType = "wait_type"
	const waitTime = "wait_time"
	const waitResource = "wait_resource"
	const openTransactionCount = "open_transaction_count"
	const transactionID = "transaction_id"
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
	const rowCount = "row_count"
	const queryHash = "query_hash"
	const queryPlanHash = "query_plan_hash"
	const contextInfo = "context_info"

	const username = "username"
	rows, err := s.client.QueryRows(ctx)
	if err != nil {
		if errors.Is(err, sqlquery.ErrNullValueWarning) {
			// TODO: ignore this for now.
			s.logger.Warn("problems encountered getting log rows", zap.Error(err))
		} else {
			return plog.Logs{}, fmt.Errorf("sqlServerScraperHelper failed getting log rows: %w", err)
		}
	}

	var errs []error
	logs := plog.NewLogs()

	for _, row := range rows {
		queryHashVal := hex.EncodeToString([]byte(row[queryHash]))
		queryPlanHashVal := hex.EncodeToString([]byte(row[queryPlanHash]))
		contextInfoVal := hex.EncodeToString([]byte(row[contextInfo]))
		// clientPort could be null, and it will be converted to empty string with ISNULL in our query. when it is
		// an empty string, clientPortNumber would be 0.
		clientPortNumber := 0
		if row[clientPort] != "" {
			clientPortNumber, err = strconv.Atoi(row[clientPort])
			if err != nil {
				s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing client port number. original value: %s, err: %s", row[clientPort], err))
			}
		}

		sessionIDNumber, err := strconv.Atoi(row[sessionID])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing session id number. original value: %s, err: %s", row[sessionID], err))
		}
		blockingSessionIDNumber, err := strconv.Atoi(row[blockingSessionID])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing blocking session id number. value: %s, err: %s", row[blockingSessionID], err))
		}
		waitTimeVal, err := strconv.Atoi(row[waitTime])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing wait time number. original value: %s, err: %s", row[waitTime], err))
		}
		openTransactionCountVal, err := strconv.Atoi(row[openTransactionCount])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing open transaction count. original value: %s, err: %s", row[openTransactionCount], err))
		}
		transactionIDVal, err := strconv.Atoi(row[transactionID])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing transaction id number. original value: %s, err: %s", row[transactionID], err))
		}
		// percent complete and estimated completion time is a real value in mssql
		percentCompleteVal, err := strconv.ParseFloat(row[percentComplete], 32)
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing percent complete. original value: %s, err: %s", row[percentComplete], err))
		}
		estimatedCompletionTimeVal, err := strconv.ParseFloat(row[estimatedCompletionTime], 32)
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing estimated completion time number. original value: %s, err: %s", row[estimatedCompletionTime], err))
		}
		cpuTimeVal, err := strconv.Atoi(row[cpuTime])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing cpu time number. original value: %s, err: %s", row[cpuTime], err))
		}
		totalElapsedTimeVal, err := strconv.Atoi(row[totalElapsedTime])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing total elapsed time. original value: %s, err: %s", row[totalElapsedTime], err))
		}
		readsVal, err := strconv.Atoi(row[reads])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing read count. original value: %s, err: %s", row[reads], err))
		}
		writesVal, err := strconv.Atoi(row[writes])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing write count. original value: %s, err: %s", row[writes], err))
		}
		logicalReadsVal, err := strconv.Atoi(row[logicalReads])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing logical read count. original value: %s, err: %s", row[logicalReads], err))
		}
		transactionIsolationLevelVal, err := strconv.Atoi(row[transactionIsolationLevel])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing transaction isolation level. original value: %s, err: %s", row[transactionIsolationLevel], err))
		}
		lockTimeoutVal := 0
		if row[lockTimeout] != "" {
			lockTimeoutVal, err = strconv.Atoi(row[lockTimeout])
			if err != nil {
				s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing lock timeout. original value: %s, err: %s", row[lockTimeout], err))
			}
		}

		deadlockPriorityVal := 0
		if row[deadlockPriority] != "" {
			deadlockPriorityVal, err = strconv.Atoi(row[deadlockPriority])
			if err != nil {
				s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing deadlock priority. original value: %s, err: %s", row[deadlockPriority], err))
			}
		}

		rowCountVal, err := strconv.Atoi(row[rowCount])
		if err != nil {
			s.logger.Error(fmt.Sprintf("sqlServerScraperHelper failed parsing row count. original value: %s, err: %s", row[rowCount], err))
		}

		obfuscatedStatement, err := obfuscateSQL(row[statementText])
		if err != nil {
			s.logger.Error(fmt.Sprintf("failed to obfuscate SQL statement value: %s err: %s", row[statementText], err))
		}
		cacheKey := queryHashVal + "-" + queryPlanHashVal

		if _, ok := s.cache.Get(cacheKey); !ok {
			// TODO: report this value
			record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			record.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			record.Attributes().PutStr(DBName, row[DBName])
			record.Attributes().PutStr(clientAddress, row[clientAddress])
			record.Attributes().PutInt(clientPort, int64(clientPortNumber))
			record.Attributes().PutStr(queryStart, row[queryStart])
			record.Attributes().PutInt(sessionID, int64(sessionIDNumber))
			record.Attributes().PutStr(sessionStatus, row[sessionStatus])
			record.Attributes().PutStr(hostname, row[hostname])
			record.Attributes().PutStr(command, row[command])
			record.Attributes().PutStr(statementText, obfuscatedStatement)
			record.Attributes().PutInt(blockingSessionID, int64(blockingSessionIDNumber))
			record.Attributes().PutStr(waitType, row[waitType])
			record.Attributes().PutInt(waitTime, int64(waitTimeVal))
			record.Attributes().PutStr(waitResource, row[waitResource])
			record.Attributes().PutInt(openTransactionCount, int64(openTransactionCountVal))
			record.Attributes().PutInt(transactionID, int64(transactionIDVal))
			record.Attributes().PutDouble(percentComplete, percentCompleteVal)
			record.Attributes().PutDouble(estimatedCompletionTime, estimatedCompletionTimeVal)
			record.Attributes().PutInt(cpuTime, int64(cpuTimeVal))
			record.Attributes().PutInt(totalElapsedTime, int64(totalElapsedTimeVal))
			record.Attributes().PutInt(reads, int64(readsVal))
			record.Attributes().PutInt(writes, int64(writesVal))
			record.Attributes().PutInt(logicalReads, int64(logicalReadsVal))
			record.Attributes().PutInt(transactionIsolationLevel, int64(transactionIsolationLevelVal))
			record.Attributes().PutInt(lockTimeout, int64(lockTimeoutVal))
			record.Attributes().PutInt(deadlockPriority, int64(deadlockPriorityVal))
			record.Attributes().PutInt(rowCount, int64(rowCountVal))
			record.Attributes().PutStr(queryHash, queryHashVal)
			record.Attributes().PutStr(queryPlanHash, queryPlanHashVal)
			record.Attributes().PutStr(contextInfo, contextInfoVal)

			record.Attributes().PutStr(username, row[username])

			waitCode, waitCategory := getWaitCategory(row[waitType])
			record.Attributes().PutInt("wait_code", int64(waitCode))
			record.Attributes().PutStr("wait_category", waitCategory)
			record.Body().SetStr("sample")
		} else {
			s.cache.Add(cacheKey, 1)
		}
	}

	return logs, errors.Join(errs...)
}

// cacheAndDiff store row(in int) with query hash and query plan hash variables
// (1) returns true if the key is cached before
// (2) returns positive value if the value is larger than the cached value
func (s *sqlServerScraperHelper) cacheAndDiff(queryHash string, queryPlanHash string, column string, val int64) (bool, int64) {
	if s.cache == nil {
		s.logger.Error("LRU cache is not successfully initialized, skipping caching and diffing")
		return false, 0
	}

	if val < 0 {
		return false, 0
	}

	key := queryHash + "-" + queryPlanHash + "-" + column

	cached, ok := s.cache.Get(key)
	if !ok {
		s.cache.Add(key, val)
		return false, val
	}

	if val > cached {
		s.cache.Add(key, val)
		return true, val - cached
	}

	return true, 0
}

// sortRows sorts the rows based on the `values` slice in descending order
// Input: (row: [row1, row2, row3], values: [100, 10, 1000]
// Expected Output: (row: [row3, row1, row2]
func sortRows(rows []sqlquery.StringMap, values []int64) []sqlquery.StringMap {
	// Create an index slice to track the original indices of rows
	indices := make([]int, len(values))
	for i := range indices {
		indices[i] = i
	}

	// Sort the indices based on the values slice
	sort.Slice(indices, func(i, j int) bool {
		return values[indices[i]] > values[indices[j]]
	})

	// Create a new sorted slice for rows based on the sorted indices
	sorted := make([]sqlquery.StringMap, len(rows))
	for i, idx := range indices {
		sorted[i] = rows[idx]
	}

	return sorted
}

func anyOf(s string, f func(a string, b string) bool, vals ...string) bool {
	if len(vals) == 0 {
		return false
	}

	for _, v := range vals {
		if f(s, v) {
			return true
		}
	}
	return false
}

func getWaitCategory(s string) (uint, string) {
	if code, exists := detailedWaitTypes[s]; exists {
		return code, waitTypes[code]
	}

	switch {
	case strings.HasPrefix(s, "LOCK_M_"):
		return 3, "Lock"
	case strings.HasPrefix(s, "LATCH_"):
		return 4, "Latch"
	case strings.HasPrefix(s, "PAGELATCH_"):
		return 5, "Buffer Latch"
	case strings.HasPrefix(s, "PAGEIOLATCH_"):
		return 6, "Buffer IO"
	case anyOf(s, strings.HasPrefix, "CLR", "SQLCLR"):
		return 8, "SQL CLR"
	case strings.HasPrefix(s, "DBMIRROR"):
		return 9, "Mirroring"
	case anyOf(s, strings.HasPrefix, "XACT", "DTC", "TRAN_MARKLATCH_", "MSQL_XACT_"):
		return 10, "Transaction"
	case strings.HasPrefix(s, "SLEEP_"):
		return 11, "Idle"
	case strings.HasPrefix(s, "PREEMPTIVE_"):
		return 12, "Preemptive"
	case strings.HasPrefix(s, "BROKER_") && s != "BROKER_RECEIVE_WAITFOR":
		return 13, "Service Broker"
	case anyOf(s, strings.HasPrefix, "HT", "BMP", "BP"):
		return 16, "Parallelism"
	case anyOf(s, strings.HasPrefix, "SE_REPL_", "REPL_", "PWAIT_HADR_"),
		strings.HasPrefix(s, "HADR_") && s != "HADR_THROTTLE_LOG_RATE_GOVERNOR":
		return 22, "Replication"
	case strings.HasPrefix(s, "RBIO_RG_"):
		return 23, "Log Rate Governor"
	default:
		return 0, "Unknown"
	}
}
