// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package postgresqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/postgresqlreceiver"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/postgresqlreceiver/internal/metadata"
)

const (
	readmeURL            = "https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.88.0/receiver/postgresqlreceiver/README.md"
	separateSchemaAttrID = "receiver.postgresql.separateSchemaAttr"

	defaultPostgreSQLDatabase = "postgres"
)

var separateSchemaAttrGate = featuregate.GlobalRegistry().MustRegister(
	separateSchemaAttrID,
	featuregate.StageAlpha,
	featuregate.WithRegisterDescription("Moves Schema Names into dedicated Attribute"),
	featuregate.WithRegisterReferenceURL("https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/29559"),
)

type postgreSQLScraper struct {
	logger        *zap.Logger
	config        *Config
	clientFactory postgreSQLClientFactory
	mb            *metadata.MetricsBuilder
	excludes      map[string]struct{}
	cache         *lru.Cache[string, float64]
	// if enabled, uses a separated attribute for the schema
	separateSchemaAttr bool
}

type errsMux struct {
	sync.RWMutex
	errs scrapererror.ScrapeErrors
}

func (e *errsMux) add(err error) {
	e.Lock()
	defer e.Unlock()
	e.errs.Add(err)
}

func (e *errsMux) addPartial(err error) {
	e.Lock()
	defer e.Unlock()
	e.errs.AddPartial(1, err)
}

func (e *errsMux) combine() error {
	e.Lock()
	defer e.Unlock()
	return e.errs.Combine()
}

func newPostgreSQLScraper(
	settings receiver.Settings,
	config *Config,
	clientFactory postgreSQLClientFactory,
	cache *lru.Cache[string, float64],
) *postgreSQLScraper {
	excludes := make(map[string]struct{})
	for _, db := range config.ExcludeDatabases {
		excludes[db] = struct{}{}
	}
	separateSchemaAttr := separateSchemaAttrGate.IsEnabled()

	if !separateSchemaAttr {
		settings.Logger.Warn(
			fmt.Sprintf("Feature gate %s is not enabled. Please see the README for more information: %s", separateSchemaAttrID, readmeURL),
		)
	}

	return &postgreSQLScraper{
		logger:        settings.Logger,
		config:        config,
		clientFactory: clientFactory,
		mb:            metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings),
		excludes:      excludes,
		cache:         cache,

		separateSchemaAttr: separateSchemaAttr,
	}
}

type dbRetrieval struct {
	sync.RWMutex
	activityMap map[databaseName]int64
	dbSizeMap   map[databaseName]int64
	dbStats     map[databaseName]databaseStats
}

// scrape scrapes the metric stats, transforms them and attributes them into a metric slices.
func (p *postgreSQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	databases := p.config.Databases
	listClient, err := p.clientFactory.getClient(defaultPostgreSQLDatabase)
	if err != nil {
		p.logger.Error("Failed to initialize connection to postgres", zap.Error(err))
		return pmetric.NewMetrics(), err
	}
	defer listClient.Close()

	if len(databases) == 0 {
		dbList, dbErr := listClient.listDatabases(ctx)
		if dbErr != nil {
			p.logger.Error("Failed to request list of databases from postgres", zap.Error(dbErr))
			return pmetric.NewMetrics(), dbErr
		}
		databases = dbList
	}
	var filteredDatabases []string
	for _, db := range databases {
		if _, ok := p.excludes[db]; !ok {
			filteredDatabases = append(filteredDatabases, db)
		}
	}
	databases = filteredDatabases

	now := pcommon.NewTimestampFromTime(time.Now())

	var errs errsMux
	r := &dbRetrieval{
		activityMap: make(map[databaseName]int64),
		dbSizeMap:   make(map[databaseName]int64),
		dbStats:     make(map[databaseName]databaseStats),
	}
	p.retrieveDBMetrics(ctx, listClient, databases, r, &errs)

	for _, database := range databases {
		dbClient, dbErr := p.clientFactory.getClient(database)
		if dbErr != nil {
			errs.add(dbErr)
			p.logger.Error("Failed to initialize connection to postgres", zap.String("database", database), zap.Error(dbErr))
			continue
		}
		defer dbClient.Close()
		numTables := p.collectTables(ctx, now, dbClient, database, &errs)

		p.recordDatabase(now, database, r, numTables)
		p.collectIndexes(ctx, now, dbClient, database, &errs)
	}

	p.mb.RecordPostgresqlDatabaseCountDataPoint(now, int64(len(databases)))
	p.collectBGWriterStats(ctx, now, listClient, &errs)
	p.collectWalAge(ctx, now, listClient, &errs)
	p.collectReplicationStats(ctx, now, listClient, &errs)
	p.collectMaxConnections(ctx, now, listClient, &errs)
	p.collectDatabaseLocks(ctx, now, listClient, &errs)

	return p.mb.Emit(), errs.combine()
}

func (p *postgreSQLScraper) scrapeQuerySamples(ctx context.Context, maxRowsPerQuery int64) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()

	scopedLog := resourceLog.ScopeLogs().AppendEmpty()
	scopedLog.Scope().SetName(metadata.ScopeName)
	scopedLog.Scope().SetVersion("0.0.1")

	dbClient, err := p.clientFactory.getClient(defaultPostgreSQLDatabase)
	if err != nil {
		p.logger.Error("Failed to initialize connection to postgres", zap.Error(err))
		return logs, err
	}

	var errs errsMux

	logRecords := scopedLog.LogRecords()

	p.collectQuerySamples(ctx, dbClient, &logRecords, maxRowsPerQuery, &errs, p.logger)

	defer dbClient.Close()

	return logs, nil
}

func (p *postgreSQLScraper) scrapeTopQuery(ctx context.Context, maxRowsPerQuery int64) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLog := logs.ResourceLogs().AppendEmpty()

	scopedLog := resourceLog.ScopeLogs().AppendEmpty()
	scopedLog.Scope().SetName(metadata.ScopeName)
	scopedLog.Scope().SetVersion("0.0.1")

	dbClient, err := p.clientFactory.getClient(defaultPostgreSQLDatabase)
	if err != nil {
		p.logger.Error("Failed to initialize connection to postgres", zap.Error(err))
		return logs, err
	}

	var errs errsMux

	logRecords := scopedLog.LogRecords()

	p.collectTopQuery(ctx, dbClient, &logRecords, maxRowsPerQuery, &errs, p.logger)

	defer dbClient.Close()

	return logs, nil
}

func (p *postgreSQLScraper) collectQuerySamples(ctx context.Context, dbClient client, logRecords *plog.LogRecordSlice, limit int64, mux *errsMux, logger *zap.Logger) {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	attributes, err := dbClient.getQuerySamples(ctx, limit, logger)
	if err != nil {
		mux.addPartial(err)
		return
	}
	for _, atts := range attributes {
		record := logRecords.AppendEmpty()
		record.SetTimestamp(timestamp)
		record.SetEventName("query sample")
		if err := record.Attributes().FromRaw(atts); err != nil {
			mux.addPartial(err)
			logger.Error("failed to read attributes from row", zap.Error(err))
		}
		record.Body().SetStr("sample")
	}
}

func (p *postgreSQLScraper) collectTopQuery(ctx context.Context, dbClient client, logRecords *plog.LogRecordSlice, limit int64, mux *errsMux, logger *zap.Logger) {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	attributes, err := dbClient.getTopQuery(ctx, limit, logger)
	if err != nil {
		logger.Error("failed to get top query", zap.Error(err))
		mux.addPartial(err)
		return
	}

	type updatedOnlyInfo struct {
		suffix         string
		finalConverter func(float64) any
	}

	updatedOnly := map[string]updatedOnlyInfo{
		TOTAL_EXEC_TIME_COLUMN_NAME: {
			suffix: EXECUTION_TIME_SUFFIX,
		},
		TOTAL_PLAN_TIME_COLUMN_NAME: {
			suffix: PLAN_TIME_SUFFIX,
		},
		ROWS_COLUMN_NAME: {
			suffix: ROWS_SUFFIX,
			finalConverter: func(f float64) any {
				return int64(f)
			},
		},
		CALLS_COLUMN_NAME: {
			suffix: CALLS_SUFFIX,
			finalConverter: func(f float64) any {
				return int64(f)
			},
		},
	}

	for _, atts := range attributes {
		queryId := atts[DB_ATTRIBUTE_PREFIX+QUERYID_COLUMN_NAME]

		if queryId == nil {
			// this should not happen, but in case
			logger.Error("queryid is nil", zap.Any("atts", atts))
			mux.addPartial(fmt.Errorf("queryid is nil"))
			continue
		}

		for columnName, info := range updatedOnly {
			var valInAtts float64
			_val := atts[DB_ATTRIBUTE_PREFIX+columnName]
			if i, ok := _val.(int64); ok {
				valInAtts = float64(i)
			} else {
				valInAtts = _val.(float64)
			}
			valInCache, exist := p.cache.Get(queryId.(string) + info.suffix)
			valDelta := valInAtts
			if exist {
				valDelta = valInAtts - valInCache
			}
			finalValue := float64(0)
			if valDelta > 0 {
				p.cache.Add(queryId.(string)+info.suffix, valDelta)
				finalValue = valDelta
			}
			if info.finalConverter != nil {
				atts[DB_ATTRIBUTE_PREFIX+columnName] = info.finalConverter(finalValue)
			} else {
				atts[DB_ATTRIBUTE_PREFIX+columnName] = finalValue
			}
		}

		// totalExecTime := atts[DB_ATTRIBUTE_PREFIX+TOTAL_EXEC_TIME_COLUMN_NAME]
		// execTimeInCache, exist := p.cache.Get(queryId.(string) + EXECUTION_TIME_SUFFIX)
		// execTimeDelta := totalExecTime.(float64)
		// if exist {
		// 	execTimeDelta = totalExecTime.(float64) - execTimeInCache
		// }
		// if execTimeDelta > 0 {
		// 	p.cache.Add(queryId.(string)+EXECUTION_TIME_SUFFIX, totalExecTime.(float64))
		// 	atts[DB_ATTRIBUTE_PREFIX+TOTAL_EXEC_TIME_COLUMN_NAME] = execTimeDelta
		// } else {
		// 	atts[DB_ATTRIBUTE_PREFIX+TOTAL_EXEC_TIME_COLUMN_NAME] = 0.0
		// }

		// totalPlanTime := atts[DB_ATTRIBUTE_PREFIX+TOTAL_PLAN_TIME_COLUMN_NAME]
		// if totalPlanTime != nil {
		// 	// in theory it would always be non-nil value.
		// 	planTimeInCache, exist := p.cache.Get(queryId.(string) + PLAN_TIME_SUFFIX)
		// 	planTimeDelta := totalPlanTime.(float64)
		// 	if exist {
		// 		planTimeDelta = totalPlanTime.(float64) - planTimeInCache
		// 	}
		// 	if planTimeDelta > 0 {
		// 		p.cache.Add(queryId.(string)+PLAN_TIME_SUFFIX, planTimeDelta)
		// 		atts[DB_ATTRIBUTE_PREFIX+TOTAL_PLAN_TIME_COLUMN_NAME] = planTimeDelta
		// 	} else {
		// 		atts[DB_ATTRIBUTE_PREFIX+TOTAL_PLAN_TIME_COLUMN_NAME] = 0.0
		// 	}
		// }

		// calls := atts[DB_ATTRIBUTE_PREFIX+CALLS_COLUMN_NAME]
		// if calls != nil {
		// 	// in theory it would always be non-nil value.
		// 	callsInRowCastedToFloat := float64(calls.(int64))
		// 	callsInCache, exist := p.cache.Get(queryId.(string) + CALLS_SUFFIX)
		// 	callsDelta := callsInRowCastedToFloat
		// 	if exist {
		// 		callsDelta = callsInRowCastedToFloat - callsInCache
		// 	}
		// 	if callsDelta > 0 {
		// 		p.cache.Add(queryId.(string)+CALLS_SUFFIX, callsInRowCastedToFloat)
		// 		atts[DB_ATTRIBUTE_PREFIX+CALLS_COLUMN_NAME] = int64(callsDelta)
		// 	} else {
		// 		atts[DB_ATTRIBUTE_PREFIX+CALLS_COLUMN_NAME] = int64(0)
		// 	}
		// }

		// rows := atts[DB_ATTRIBUTE_PREFIX+ROWS_COLUMN_NAME]
		// if rows != nil {
		// 	// in theory it would always be non-nil value.
		// 	rowsInRowCastedToFloat := float64(rows.(int64))
		// 	rowsInCache, exist := p.cache.Get(queryId.(string) + ROWS_SUFFIX)
		// 	rowsDelta := rowsInRowCastedToFloat
		// 	if exist {
		// 		rowsDelta = rowsInRowCastedToFloat - rowsInCache
		// 	}
		// 	if rowsDelta > 0 {
		// 		p.cache.Add(queryId.(string)+ROWS_SUFFIX, rowsInRowCastedToFloat)
		// 		atts[DB_ATTRIBUTE_PREFIX+ROWS_COLUMN_NAME] = int64(rowsDelta)
		// 	} else {
		// 		atts[DB_ATTRIBUTE_PREFIX+ROWS_COLUMN_NAME] = int64(0)
		// 	}
		// }
		record := logRecords.AppendEmpty()
		record.SetTimestamp(timestamp)
		record.SetEventName("top query")
		if err := record.Attributes().FromRaw(atts); err != nil {
			mux.addPartial(err)
			logger.Error("failed to read attributes from row", zap.Error(err))
		}
		record.Body().SetStr("top query")
	}
}

func (p *postgreSQLScraper) shutdown(_ context.Context) error {
	if p.clientFactory != nil {
		p.clientFactory.close()
	}
	return nil
}

func (p *postgreSQLScraper) retrieveDBMetrics(
	ctx context.Context,
	listClient client,
	databases []string,
	r *dbRetrieval,
	errs *errsMux,
) {
	wg := &sync.WaitGroup{}

	wg.Add(3)
	go p.retrieveBackends(ctx, wg, listClient, databases, r, errs)
	go p.retrieveDatabaseSize(ctx, wg, listClient, databases, r, errs)
	go p.retrieveDatabaseStats(ctx, wg, listClient, databases, r, errs)

	wg.Wait()
}

func (p *postgreSQLScraper) recordDatabase(now pcommon.Timestamp, db string, r *dbRetrieval, numTables int64) {
	dbName := databaseName(db)
	p.mb.RecordPostgresqlTableCountDataPoint(now, numTables)
	if activeConnections, ok := r.activityMap[dbName]; ok {
		p.mb.RecordPostgresqlBackendsDataPoint(now, activeConnections)
	}
	if size, ok := r.dbSizeMap[dbName]; ok {
		p.mb.RecordPostgresqlDbSizeDataPoint(now, size)
	}
	if stats, ok := r.dbStats[dbName]; ok {
		p.mb.RecordPostgresqlCommitsDataPoint(now, stats.transactionCommitted)
		p.mb.RecordPostgresqlRollbacksDataPoint(now, stats.transactionRollback)
		p.mb.RecordPostgresqlDeadlocksDataPoint(now, stats.deadlocks)
		p.mb.RecordPostgresqlTempFilesDataPoint(now, stats.tempFiles)
		p.mb.RecordPostgresqlTupUpdatedDataPoint(now, stats.tupUpdated)
		p.mb.RecordPostgresqlTupReturnedDataPoint(now, stats.tupReturned)
		p.mb.RecordPostgresqlTupFetchedDataPoint(now, stats.tupFetched)
		p.mb.RecordPostgresqlTupInsertedDataPoint(now, stats.tupInserted)
		p.mb.RecordPostgresqlTupDeletedDataPoint(now, stats.tupDeleted)
		p.mb.RecordPostgresqlBlksHitDataPoint(now, stats.blksHit)
		p.mb.RecordPostgresqlBlksReadDataPoint(now, stats.blksRead)
	}
	rb := p.mb.NewResourceBuilder()
	rb.SetPostgresqlDatabaseName(db)
	p.mb.EmitForResource(metadata.WithResource(rb.Emit()))
}

func (p *postgreSQLScraper) collectTables(ctx context.Context, now pcommon.Timestamp, dbClient client, db string, errs *errsMux) (numTables int64) {
	blockReads, err := dbClient.getBlocksReadByTable(ctx, db)
	if err != nil {
		errs.addPartial(err)
	}

	tableMetrics, err := dbClient.getDatabaseTableMetrics(ctx, db)
	if err != nil {
		errs.addPartial(err)
	}

	for tableKey, tm := range tableMetrics {
		p.mb.RecordPostgresqlRowsDataPoint(now, tm.dead, metadata.AttributeStateDead)
		p.mb.RecordPostgresqlRowsDataPoint(now, tm.live, metadata.AttributeStateLive)
		p.mb.RecordPostgresqlOperationsDataPoint(now, tm.inserts, metadata.AttributeOperationIns)
		p.mb.RecordPostgresqlOperationsDataPoint(now, tm.del, metadata.AttributeOperationDel)
		p.mb.RecordPostgresqlOperationsDataPoint(now, tm.upd, metadata.AttributeOperationUpd)
		p.mb.RecordPostgresqlOperationsDataPoint(now, tm.hotUpd, metadata.AttributeOperationHotUpd)
		p.mb.RecordPostgresqlTableSizeDataPoint(now, tm.size)
		p.mb.RecordPostgresqlTableVacuumCountDataPoint(now, tm.vacuumCount)
		p.mb.RecordPostgresqlSequentialScansDataPoint(now, tm.seqScans)

		br, ok := blockReads[tableKey]
		if ok {
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.heapRead, metadata.AttributeSourceHeapRead)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.heapHit, metadata.AttributeSourceHeapHit)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.idxRead, metadata.AttributeSourceIdxRead)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.idxHit, metadata.AttributeSourceIdxHit)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.toastHit, metadata.AttributeSourceToastHit)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.toastRead, metadata.AttributeSourceToastRead)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.tidxRead, metadata.AttributeSourceTidxRead)
			p.mb.RecordPostgresqlBlocksReadDataPoint(now, br.tidxHit, metadata.AttributeSourceTidxHit)
		}
		rb := p.mb.NewResourceBuilder()
		rb.SetPostgresqlDatabaseName(db)
		if p.separateSchemaAttr {
			rb.SetPostgresqlSchemaName(tm.schema)
			rb.SetPostgresqlTableName(tm.table)
		} else {
			rb.SetPostgresqlTableName(fmt.Sprintf("%s.%s", tm.schema, tm.table))
		}
		p.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}
	return int64(len(tableMetrics))
}

func (p *postgreSQLScraper) collectIndexes(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	database string,
	errs *errsMux,
) {
	idxStats, err := client.getIndexStats(ctx, database)
	if err != nil {
		errs.addPartial(err)
		return
	}

	for _, stat := range idxStats {
		p.mb.RecordPostgresqlIndexScansDataPoint(now, stat.scans)
		p.mb.RecordPostgresqlIndexSizeDataPoint(now, stat.size)
		rb := p.mb.NewResourceBuilder()
		rb.SetPostgresqlDatabaseName(database)
		if p.separateSchemaAttr {
			rb.SetPostgresqlSchemaName(stat.schema)
			rb.SetPostgresqlTableName(stat.table)
		} else {
			rb.SetPostgresqlTableName(stat.table)
		}
		rb.SetPostgresqlIndexName(stat.index)
		p.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}
}

func (p *postgreSQLScraper) collectBGWriterStats(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	bgStats, err := client.getBGWriterStats(ctx)
	if err != nil {
		errs.addPartial(err)
		return
	}

	p.mb.RecordPostgresqlBgwriterBuffersAllocatedDataPoint(now, bgStats.buffersAllocated)

	p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bgWrites, metadata.AttributeBgBufferSourceBgwriter)
	if bgStats.bufferBackendWrites >= 0 {
		p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bufferBackendWrites, metadata.AttributeBgBufferSourceBackend)
	}
	p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bufferCheckpoints, metadata.AttributeBgBufferSourceCheckpoints)
	if bgStats.bufferFsyncWrites >= 0 {
		p.mb.RecordPostgresqlBgwriterBuffersWritesDataPoint(now, bgStats.bufferFsyncWrites, metadata.AttributeBgBufferSourceBackendFsync)
	}

	p.mb.RecordPostgresqlBgwriterCheckpointCountDataPoint(now, bgStats.checkpointsReq, metadata.AttributeBgCheckpointTypeRequested)
	p.mb.RecordPostgresqlBgwriterCheckpointCountDataPoint(now, bgStats.checkpointsScheduled, metadata.AttributeBgCheckpointTypeScheduled)

	p.mb.RecordPostgresqlBgwriterDurationDataPoint(now, bgStats.checkpointSyncTime, metadata.AttributeBgDurationTypeSync)
	p.mb.RecordPostgresqlBgwriterDurationDataPoint(now, bgStats.checkpointWriteTime, metadata.AttributeBgDurationTypeWrite)

	p.mb.RecordPostgresqlBgwriterMaxwrittenDataPoint(now, bgStats.maxWritten)
}

func (p *postgreSQLScraper) collectDatabaseLocks(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	dbLocks, err := client.getDatabaseLocks(ctx)
	if err != nil {
		p.logger.Error("Errors encountered while fetching database locks", zap.Error(err))
		errs.addPartial(err)
		return
	}
	for _, dbLock := range dbLocks {
		p.mb.RecordPostgresqlDatabaseLocksDataPoint(now, dbLock.locks, dbLock.relation, dbLock.mode, dbLock.lockType)
	}
}

func (p *postgreSQLScraper) collectMaxConnections(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	mc, err := client.getMaxConnections(ctx)
	if err != nil {
		errs.addPartial(err)
		return
	}
	p.mb.RecordPostgresqlConnectionMaxDataPoint(now, mc)
}

func (p *postgreSQLScraper) collectReplicationStats(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	rss, err := client.getReplicationStats(ctx)
	if err != nil {
		errs.addPartial(err)
		return
	}
	for _, rs := range rss {
		if rs.pendingBytes >= 0 {
			p.mb.RecordPostgresqlReplicationDataDelayDataPoint(now, rs.pendingBytes, rs.clientAddr)
		}
		if preciseLagMetricsFg.IsEnabled() {
			if rs.writeLag >= 0 {
				p.mb.RecordPostgresqlWalDelayDataPoint(now, rs.writeLag, metadata.AttributeWalOperationLagWrite, rs.clientAddr)
			}
			if rs.replayLag >= 0 {
				p.mb.RecordPostgresqlWalDelayDataPoint(now, rs.replayLag, metadata.AttributeWalOperationLagReplay, rs.clientAddr)
			}
			if rs.flushLag >= 0 {
				p.mb.RecordPostgresqlWalDelayDataPoint(now, rs.flushLag, metadata.AttributeWalOperationLagFlush, rs.clientAddr)
			}
		} else {
			if rs.writeLagInt >= 0 {
				p.mb.RecordPostgresqlWalLagDataPoint(now, rs.writeLagInt, metadata.AttributeWalOperationLagWrite, rs.clientAddr)
			}
			if rs.replayLagInt >= 0 {
				p.mb.RecordPostgresqlWalLagDataPoint(now, rs.replayLagInt, metadata.AttributeWalOperationLagReplay, rs.clientAddr)
			}
			if rs.flushLagInt >= 0 {
				p.mb.RecordPostgresqlWalLagDataPoint(now, rs.flushLagInt, metadata.AttributeWalOperationLagFlush, rs.clientAddr)
			}
		}
	}
}

func (p *postgreSQLScraper) collectWalAge(
	ctx context.Context,
	now pcommon.Timestamp,
	client client,
	errs *errsMux,
) {
	walAge, err := client.getLatestWalAgeSeconds(ctx)
	if errors.Is(err, errNoLastArchive) {
		// return no error as there is no last archive to derive the value from
		return
	}
	if err != nil {
		errs.addPartial(fmt.Errorf("unable to determine latest WAL age: %w", err))
		return
	}
	p.mb.RecordPostgresqlWalAgeDataPoint(now, walAge)
}

func (p *postgreSQLScraper) retrieveDatabaseStats(
	ctx context.Context,
	wg *sync.WaitGroup,
	client client,
	databases []string,
	r *dbRetrieval,
	errs *errsMux,
) {
	defer wg.Done()
	dbStats, err := client.getDatabaseStats(ctx, databases)
	if err != nil {
		p.logger.Error("Errors encountered while fetching commits and rollbacks", zap.Error(err))
		errs.addPartial(err)
		return
	}
	r.Lock()
	r.dbStats = dbStats
	r.Unlock()
}

func (p *postgreSQLScraper) retrieveDatabaseSize(
	ctx context.Context,
	wg *sync.WaitGroup,
	client client,
	databases []string,
	r *dbRetrieval,
	errs *errsMux,
) {
	defer wg.Done()
	databaseSizeMetrics, err := client.getDatabaseSize(ctx, databases)
	if err != nil {
		p.logger.Error("Errors encountered while fetching database size", zap.Error(err))
		errs.addPartial(err)
		return
	}
	r.Lock()
	r.dbSizeMap = databaseSizeMetrics
	r.Unlock()
}

func (p *postgreSQLScraper) retrieveBackends(
	ctx context.Context,
	wg *sync.WaitGroup,
	client client,
	databases []string,
	r *dbRetrieval,
	errs *errsMux,
) {
	defer wg.Done()
	activityByDB, err := client.getBackends(ctx, databases)
	if err != nil {
		errs.addPartial(err)
		return
	}
	r.Lock()
	r.activityMap = activityByDB
	r.Unlock()
}
