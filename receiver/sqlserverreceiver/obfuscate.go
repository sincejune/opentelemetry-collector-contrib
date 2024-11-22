// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package sqlserverreceiver

import (
	"encoding/json"
	"github.com/DataDog/datadog-agent/pkg/obfuscate"
	"sync"
)

var (
	// one obfuscator instance is shared across all python checks. It is not threadsafe but that is ok because
	// the GIL is always locked when calling c code from python which means that the exported functions in this file
	// will only ever be called by one goroutine at a time
	obfuscator       *obfuscate.Obfuscator
	obfuscatorLoader sync.Once
)

// lazyInitObfuscator initializes the obfuscator the first time it is used. We can't initialize during the package init
// because the obfuscator depends on pkgconfigsetup.Datadog and it isn't guaranteed to be initialized during package init, but
// will definitely be initialized by the time one of the python checks runs
func lazyInitObfuscator() *obfuscate.Obfuscator {
	obfuscatorLoader.Do(func() {
		var cfg = obfuscate.Config{
			SQL:        obfuscate.SQLConfig{},
			ES:         obfuscate.JSONConfig{},
			OpenSearch: obfuscate.JSONConfig{},
			Mongo:      obfuscate.JSONConfig{},
			SQLExecPlan: obfuscate.JSONConfig{
				Enabled: true,
			},
			SQLExecPlanNormalize: obfuscate.JSONConfig{},
			HTTP:                 obfuscate.HTTPConfig{},
			Redis:                obfuscate.RedisConfig{},
			Memcached:            obfuscate.MemcachedConfig{},
			CreditCard:           obfuscate.CreditCardsConfig{},
			Statsd:               nil,
			Logger:               nil,
		}
		if !cfg.SQLExecPlan.Enabled {
			cfg.SQLExecPlan = defaultSQLPlanObfuscateSettings
		}
		if !cfg.SQLExecPlanNormalize.Enabled {
			cfg.SQLExecPlanNormalize = defaultSQLPlanNormalizeSettings
		}
		//if !cfg.Mongo.Enabled {
		//	cfg.Mongo = defaultMongoObfuscateSettings
		//}
		obfuscator = obfuscate.NewObfuscator(cfg)
	})
	return obfuscator
}

// sqlConfig holds the config for the python SQL obfuscator.
type sqlConfig struct {
	// DBMS identifies the type of database management system (e.g. MySQL, Postgres, and SQL Server).
	DBMS string `json:"dbms"`
	// TableNames specifies whether the obfuscator should extract and return table names as SQL metadata when obfuscating.
	TableNames bool `json:"table_names"`
	// CollectCommands specifies whether the obfuscator should extract and return commands as SQL metadata when obfuscating.
	CollectCommands bool `json:"collect_commands"`
	// CollectComments specifies whether the obfuscator should extract and return comments as SQL metadata when obfuscating.
	CollectComments bool `json:"collect_comments"`
	// CollectProcedures specifies whether the obfuscator should extract and return procedure names as SQL metadata when obfuscating.
	CollectProcedures bool `json:"collect_procedures"`
	// ReplaceDigits specifies whether digits in table names and identifiers should be obfuscated.
	ReplaceDigits bool `json:"replace_digits"`
	// KeepSQLAlias specifies whether or not to strip sql aliases while obfuscating.
	KeepSQLAlias bool `json:"keep_sql_alias"`
	// DollarQuotedFunc specifies whether or not to remove $func$ strings in postgres.
	DollarQuotedFunc bool `json:"dollar_quoted_func"`
	// ReturnJSONMetadata specifies whether the stub will return metadata as JSON.
	ReturnJSONMetadata bool `json:"return_json_metadata"`

	// ObfuscationMode specifies the obfuscation mode to use for go-sqllexer pkg.
	// When specified, obfuscator will attempt to use go-sqllexer pkg to obfuscate (and normalize) SQL queries.
	// Valid values are "obfuscate_only", "obfuscate_and_normalize"
	ObfuscationMode obfuscate.ObfuscationMode `json:"obfuscation_mode"`

	// RemoveSpaceBetweenParentheses specifies whether to remove spaces between parentheses.
	// By default, spaces are inserted between parentheses during normalization.
	// This option is only valid when ObfuscationMode is "normalize_only" or "obfuscate_and_normalize".
	RemoveSpaceBetweenParentheses bool `json:"remove_space_between_parentheses"`

	// KeepNull specifies whether to disable obfuscate NULL value with ?.
	// This option is only valid when ObfuscationMode is "obfuscate_only" or "obfuscate_and_normalize".
	KeepNull bool `json:"keep_null"`

	// KeepBoolean specifies whether to disable obfuscate boolean value with ?.
	// This option is only valid when ObfuscationMode is "obfuscate_only" or "obfuscate_and_normalize".
	KeepBoolean bool `json:"keep_boolean"`

	// KeepPositionalParameter specifies whether to disable obfuscate positional parameter with ?.
	// This option is only valid when ObfuscationMode is "obfuscate_only" or "obfuscate_and_normalize".
	KeepPositionalParameter bool `json:"keep_positional_parameter"`

	// KeepTrailingSemicolon specifies whether to keep trailing semicolon.
	// By default, trailing semicolon is removed during normalization.
	// This option is only valid when ObfuscationMode is "normalize_only" or "obfuscate_and_normalize".
	KeepTrailingSemicolon bool `json:"keep_trailing_semicolon"`

	// KeepIdentifierQuotation specifies whether to keep identifier quotation, e.g. "my_table" or [my_table].
	// By default, identifier quotation is removed during normalization.
	// This option is only valid when ObfuscationMode is "normalize_only" or "obfuscate_and_normalize".
	KeepIdentifierQuotation bool `json:"keep_identifier_quotation"`

	// KeepJSONPath specifies whether to keep JSON paths following JSON operators in SQL statements in obfuscation.
	// By default, JSON paths are treated as literals and are obfuscated to ?, e.g. "data::jsonb -> 'name'" -> "data::jsonb -> ?".
	// This option is only valid when ObfuscationMode is "normalize_only" or "obfuscate_and_normalize".
	KeepJSONPath bool `json:"keep_json_path" yaml:"keep_json_path"`
}

// ObfuscateSQL obfuscates & normalizes the provided SQL query, writing the error into errResult if the operation
// fails. An optional configuration may be passed to change the behavior of the obfuscator.
//
//export ObfuscateSQL
func TrackedCString(str string) string {

	// TODO(memory-tracking): track the origin of the string (for example check name)
	//if pkgconfigsetup.Datadog().GetBool("memtrack_enabled") {
	//	MemoryTracker(unsafe.Pointer(cstr), C.size_t(len(str)+1), C.DATADOG_AGENT_RTLOADER_ALLOCATION)
	//}

	return str
}

func ObfuscateSQL(rawQuery, optStr string) string {
	if optStr == "" {
		// ensure we have a valid JSON string before unmarshalling
		optStr = "{}"
	}
	var sqlOpts sqlConfig
	if err := json.Unmarshal([]byte(optStr), &sqlOpts); err != nil {
		println("Failed to unmarshal obfuscation options: %s", err.Error())
		//*errResult = TrackedCString(err.Error())
	}
	s := rawQuery
	obfuscatedQuery, err := lazyInitObfuscator().ObfuscateSQLStringWithOptions(s, &obfuscate.SQLConfig{
		DBMS:                          sqlOpts.DBMS,
		TableNames:                    sqlOpts.TableNames,
		CollectCommands:               sqlOpts.CollectCommands,
		CollectComments:               sqlOpts.CollectComments,
		CollectProcedures:             sqlOpts.CollectProcedures,
		ReplaceDigits:                 sqlOpts.ReplaceDigits,
		KeepSQLAlias:                  sqlOpts.KeepSQLAlias,
		DollarQuotedFunc:              sqlOpts.DollarQuotedFunc,
		ObfuscationMode:               sqlOpts.ObfuscationMode,
		RemoveSpaceBetweenParentheses: sqlOpts.RemoveSpaceBetweenParentheses,
		KeepNull:                      sqlOpts.KeepNull,
		KeepBoolean:                   sqlOpts.KeepBoolean,
		KeepPositionalParameter:       sqlOpts.KeepPositionalParameter,
		KeepTrailingSemicolon:         sqlOpts.KeepTrailingSemicolon,
		KeepIdentifierQuotation:       sqlOpts.KeepIdentifierQuotation,
		KeepJSONPath:                  sqlOpts.KeepJSONPath,
	})
	if err != nil {
		// memory will be freed by caller
		//*errResult = TrackedCString(err.Error())
		return ""
	}
	if sqlOpts.ReturnJSONMetadata {
		out, err := json.Marshal(obfuscatedQuery)
		if err != nil {
			// memory will be freed by caller
			//*errResult = TrackedCString(err.Error())
			return ""
		}
		// memory will be freed by caller
		return TrackedCString(string(out))
	}
	// memory will be freed by caller
	return TrackedCString(obfuscatedQuery.Query)
}

// ObfuscateSQLExecPlan obfuscates the provided json query execution plan, writing the error into errResult if the
// operation fails
//
//export ObfuscateSQLExecPlan
func ObfuscateSQLExecPlan(jsonPlan string, normalize bool, errResult *string) string {
	obfuscatedJSONPlan, err := lazyInitObfuscator().ObfuscateSQLExecPlan(
		jsonPlan,
		bool(normalize),
	)
	if err != nil {
		// memory will be freed by caller
		*errResult = TrackedCString(err.Error())
		return ""
	}
	// memory will be freed by caller
	return TrackedCString(obfuscatedJSONPlan)
}

// defaultSQLPlanNormalizeSettings are the default JSON obfuscator settings for both obfuscating and normalizing SQL
// execution plans
var defaultSQLPlanNormalizeSettings = obfuscate.JSONConfig{
	Enabled: true,
	ObfuscateSQLValues: []string{
		// mysql
		"attached_condition",
		// postgres
		"Cache Key",
		"Conflict Filter",
		"Function Call",
		"Filter",
		"Hash Cond",
		"Index Cond",
		"Join Filter",
		"Merge Cond",
		"Output",
		"Recheck Cond",
		"Repeatable Seed",
		"Sampling Parameters",
		"TID Cond",
	},
	KeepValues: []string{
		// mysql
		"access_type",
		"backward_index_scan",
		"cacheable",
		"delete",
		"dependent",
		"first_match",
		"key",
		"key_length",
		"possible_keys",
		"ref",
		"select_id",
		"table_name",
		"update",
		"used_columns",
		"used_key_parts",
		"using_MRR",
		"using_filesort",
		"using_index",
		"using_join_buffer",
		"using_temporary_table",
		// postgres
		"Actual Loops",
		"Actual Rows",
		"Actual Startup Time",
		"Actual Total Time",
		"Alias",
		"Async Capable",
		"Average Sort Space Used",
		"Cache Evictions",
		"Cache Hits",
		"Cache Misses",
		"Cache Overflows",
		"Calls",
		"Command",
		"Conflict Arbiter Indexes",
		"Conflict Resolution",
		"Conflicting Tuples",
		"Constraint Name",
		"CTE Name",
		"Custom Plan Provider",
		"Deforming",
		"Emission",
		"Exact Heap Blocks",
		"Execution Time",
		"Expressions",
		"Foreign Delete",
		"Foreign Insert",
		"Foreign Update",
		"Full-sort Group",
		"Function Name",
		"Generation",
		"Group Count",
		"Grouping Sets",
		"Group Key",
		"HashAgg Batches",
		"Hash Batches",
		"Hash Buckets",
		"Heap Fetches",
		"I/O Read Time",
		"I/O Write Time",
		"Index Name",
		"Inlining",
		"Join Type",
		"Local Dirtied Blocks",
		"Local Hit Blocks",
		"Local Read Blocks",
		"Local Written Blocks",
		"Lossy Heap Blocks",
		"Node Type",
		"Optimization",
		"Original Hash Batches",
		"Original Hash Buckets",
		"Parallel Aware",
		"Parent Relationship",
		"Partial Mode",
		"Peak Memory Usage",
		"Peak Sort Space Used",
		"Planned Partitions",
		"Planning Time",
		"Pre-sorted Groups",
		"Presorted Key",
		"Query Identifier",
		"Relation Name",
		"Rows Removed by Conflict Filter",
		"Rows Removed by Filter",
		"Rows Removed by Index Recheck",
		"Rows Removed by Join Filter",
		"Sampling Method",
		"Scan Direction",
		"Schema",
		"Settings",
		"Shared Dirtied Blocks",
		"Shared Hit Blocks",
		"Shared Read Blocks",
		"Shared Written Blocks",
		"Single Copy",
		"Sort Key",
		"Sort Method",
		"Sort Methods Used",
		"Sort Space Type",
		"Sort Space Used",
		"Strategy",
		"Subplan Name",
		"Subplans Removed",
		"Target Tables",
		"Temp Read Blocks",
		"Temp Written Blocks",
		"Time",
		"Timing",
		"Total",
		"Trigger",
		"Trigger Name",
		"Triggers",
		"Tuples Inserted",
		"Tuplestore Name",
		"WAL Bytes",
		"WAL FPI",
		"WAL Records",
		"Worker",
		"Worker Number",
		"Workers",
		"Workers Launched",
		"Workers Planned",
	},
}

// defaultSQLPlanObfuscateSettings builds upon sqlPlanNormalizeSettings by including cost & row estimates in the keep
// list
var defaultSQLPlanObfuscateSettings = obfuscate.JSONConfig{
	Enabled: true,
	KeepValues: append([]string{
		// mysql
		"cost_info",
		"filtered",
		"rows_examined_per_join",
		"rows_examined_per_scan",
		"rows_produced_per_join",
		// postgres
		"Plan Rows",
		"Plan Width",
		"Startup Cost",
		"Total Cost",
	}, defaultSQLPlanNormalizeSettings.KeepValues...),
	ObfuscateSQLValues: defaultSQLPlanNormalizeSettings.ObfuscateSQLValues,
}
