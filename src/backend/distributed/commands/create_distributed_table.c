/*-------------------------------------------------------------------------
 *
 * create_distributed_relation.c
 *	  Routines relation to the creation of distributed relations.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/distribution_column.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_utility.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parser.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/inval.h"


/* Replication model to use when creating distributed tables */
int ReplicationModel = REPLICATION_MODEL_COORDINATOR;


/* local function forward declarations */
static void CreateDistributedTable(Oid relationId, Var *distributionColumn,
								   char distributionMethod, char replicationModel,
								   uint32 colocationId);
static void CreateDistributedTableMetadata(Oid relationId, Var *distributionColumn,
										   char distributionMethod, char replicationModel,
										   uint32 colocationId);
static void CreateHashDistributedTableShards(Oid relationId, uint32 colocationId);
static void CreateReferenceTable(Oid distributedRelationId);
static void ConvertToDistributedTable(Oid relationId, char *distributionColumnName,
									  char distributionMethod, char replicationModel,
									  uint32 colocationId);
static char LookupDistributionMethod(Oid distributionMethodOid);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);
static bool LocalTableEmpty(Oid tableId);
static void CopyLocalDataIntoShards(Oid relationId);
static List * TupleDescColumnNameList(TupleDesc tupleDescriptor);
#if (PG_VERSION_NUM >= 100000)
static bool RelationUsesIdentityColumns(TupleDesc relationDesc);
#endif

static void EnsureNoUnsupportedFeatureUsed(Oid relationId, Var *distributionColumn,
										   char distributionMethod, char replicationModel,
										   uint32 colocationId);
static void EnsureSchemaExistsOnAllNodes(Oid relationId);
static void EnsureLocalTableEmpty(Oid relationId);
static void EnsureTableNotDistributed(Oid relationId);
static void EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod);
static void EnsureIsTableId(Oid relationId);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(create_distributed_table);
PG_FUNCTION_INFO_V1(create_reference_table);


/*
 * master_create_distributed_table accepts a table, distribution column and
 * method and performs the corresponding catalog changes.
 *
 * Note that this UDF is deprecated and cannot create colocated tables, so we
 * always use INVALID_COLOCATION_ID.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *distributionColumnText = PG_GETARG_TEXT_P(1);
	Oid distributionMethodOid = PG_GETARG_OID(2);

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	Relation relation = relation_open(relationId, ExclusiveLock);

	char *distributionColumnName = text_to_cstring(distributionColumnText);
	Var *distributionColumn = BuildDistributionKeyFromColumnName(relation,
																 distributionColumnName);
	char distributionMethod = LookupDistributionMethod(distributionMethodOid);

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	if (ReplicationModel != REPLICATION_MODEL_COORDINATOR)
	{
		ereport(NOTICE, (errmsg("using statement-based replication"),
						 errdetail("The current replication_model setting is "
								   "'streaming', which is not supported by "
								   "master_create_distributed_table."),
						 errhint("Use create_distributed_table to use the streaming "
								 "replication model.")));
	}

	if (PartitionedTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("distributing partitioned tables in only supported "
							   "with create_distributed_table UDF")));
	}

	EnsureNoUnsupportedFeatureUsed(relationId, distributionColumn, distributionMethod,
								   REPLICATION_MODEL_COORDINATOR, INVALID_COLOCATION_ID);
	EnsureLocalTableEmpty(relationId);

	CreateDistributedTableMetadata(relationId, distributionColumn, distributionMethod,
								   REPLICATION_MODEL_COORDINATOR, INVALID_COLOCATION_ID);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


/*
 * create_distributed_table gets a table name, distribution column,
 * distribution method and colocate_with option, then it creates a
 * distributed table.
 */
Datum
create_distributed_table(PG_FUNCTION_ARGS)
{
	Oid relationId = InvalidOid;
	text *distributionColumnText = NULL;
	Oid distributionMethodOid = InvalidOid;
	text *colocateWithTableNameText = NULL;

	Relation relation = NULL;
	char *distributionColumnName = NULL;
	Var *distributionColumn = NULL;
	char distributionMethod = 0;

	char *colocateWithTableName = NULL;
	uint32 colocationId = INVALID_COLOCATION_ID;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	relationId = PG_GETARG_OID(0);
	distributionColumnText = PG_GETARG_TEXT_P(1);
	distributionMethodOid = PG_GETARG_OID(2);
	colocateWithTableNameText = PG_GETARG_TEXT_P(3);

	/*
	 * Lock target relation with an exclusive lock - there's no way to make
	 * sense of this table until we've committed, and we don't want multiple
	 * backends manipulating this relation.
	 */
	relation = relation_open(relationId, ExclusiveLock);

	distributionColumnName = text_to_cstring(distributionColumnText);
	distributionColumn = BuildDistributionKeyFromColumnName(relation,
															distributionColumnName);
	distributionMethod = LookupDistributionMethod(distributionMethodOid);

	/*
	 * ColocationIdForNewTable will return a colocation id according to given
	 * constraints. If there is no such colocation group, it will create a one.
	 */
	colocateWithTableName = text_to_cstring(colocateWithTableNameText);
	colocationId = ColocationIdForNewTable(relationId, distributionColumnName,
										   distributionMethod, colocateWithTableName);

	CreateDistributedTable(relationId, distributionColumn, distributionMethod,
						   ReplicationModel, colocationId);

	relation_close(relation, NoLock);

	PG_RETURN_VOID();
}


static void
CreateDistributedTable(Oid relationId, Var *distributionColumn,
					   char distributionMethod, char replicationModel,
					   uint32 colocationId)
{
	EnsureNoUnsupportedFeatureUsed(relationId, distributionColumn, distributionMethod,
								   replicationModel, colocationId);

	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		CreateHashDistributedTableShards(relationId, colocationId);
	}
	else if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		CreateReferenceTableShard(relationId);
	}

	CreateDistributedTableMetadata(relationId, distributionColumn, distributionMethod,
								   replicationModel, colocationId);

	if (distributionMethod == DISTRIBUTE_BY_HASH && PartitionedTable(relationId))
	{
		List *partitionList = PartitionList(relationId);
		ListCell *partitionCell = NULL;

		foreach(partitionCell, partitionList)
		{
			Oid partitionRelationId = lfirst_oid(partitionCell);
			CreateDistributedTable(partitionRelationId, distributionColumn,
								   distributionMethod, replicationModel, colocationId);
		}
	}
}


static void
CreateDistributedTableMetadata(Oid relationId, Var *distributionColumn,
							   char distributionMethod, char replicationModel,
							   uint32 colocationId)
{
	char relationKind = get_rel_relkind(relationId);

	InsertIntoPgDistPartition(relationId, distributionMethod, distributionColumn,
							  colocationId, replicationModel);

	/*
	 * PostgreSQL supports truncate trigger for regular relations only.
	 * Truncate on foreign tables is not supported.
	 */
	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
	{
		CreateTruncateTrigger(relationId);
	}

	if (ShouldSyncTableMetadata(relationId))
	{
		CreateTableMetadataOnWorkers(relationId);
	}
}


static void
EnsureNoUnsupportedFeatureUsed(Oid relationId, Var *distributionColumn, char
							   distributionMethod, char replicationModel, uint32
							   colocationId)
{
	Relation relation = NULL;
	TupleDesc relationDesc = NULL;
	char *relationName = NULL;

	relation = relation_open(relationId, ExclusiveLock);

	EnsureRelationKindSupported(relationId);
	EnsureTableOwner(relationId);
	EnsureTableNotDistributed(relationId);
	EnsureLocalTableEmptyIfNecessary(relationId, distributionMethod);

	relationDesc = RelationGetDescr(relation);
	relationName = RelationGetRelationName(relation);

	/* replication model checks */
	if (distributionMethod != DISTRIBUTE_BY_HASH)
	{
		if (ReplicationModel != REPLICATION_MODEL_COORDINATOR)
		{
			ereport(NOTICE, (errmsg("using statement-based replication"),
							 errdetail("Streaming replication is supported only for "
									   "hash-distributed tables.")));
		}
	}
	else
	{
		EnsureReplicationSettings(InvalidOid, replicationModel);
	}

	/* verify target relation does not use WITH (OIDS) PostgreSQL feature */
	if (relationDesc->tdhasoid)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not specify the WITH "
								  "(OIDS) option in their definitions.")));
	}


#if (PG_VERSION_NUM >= 100000)
	if (RelationUsesIdentityColumns(relationDesc))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot distribute relation: %s", relationName),
						errdetail("Distributed relations must not use GENERATED "
								  "... AS IDENTITY.")));
	}
#endif

	if (PartitionedTable(relationId))
	{
		/* distributing partitioned tables in only supported for hash-distribution */
		if (distributionMethod != DISTRIBUTE_BY_HASH)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributing partitioned tables in only supported "),
							"for hash-distributed tables"));
		}

		/* we cannot distribute tables with multi-level partitioning */
		if (PartitionTable(relationId))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Citus does not support distributing multi-level "),
							"partitioned tables"));
		}
	}

	/* partitions cannot be distributed directly */
	if (PartitionTable(relationId) && !IsDistributedTable(PartitionParentOid(relationId)))
	{
		char *relationName = get_rel_name(relationId);
		char *parentRelationName = get_rel_name(PartitionParentOid(relationId));

		ereport(ERROR, (errmsg("cannot distribute relation \"%s\" which is partition of "
							   "\"%s\"", relationName, parentRelationName),
						errdetail("Citus does not support distributing partitions "
								  "directly."),
						errhint("Distribute the partitioned table \"%s\" instead",
								parentRelationName)));
	}

	/* check for support function needed by specified partition method */
	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(distributionColumn,
														   HASH_AM_OID, HASHPROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(distributionColumn->vartype)),
							errdatatype(distributionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
		}
	}
	else if (distributionMethod == DISTRIBUTE_BY_RANGE)
	{
		Oid btreeSupportFunction = SupportFunctionForColumn(distributionColumn,
															BTREE_AM_OID, BTORDER_PROC);
		if (btreeSupportFunction == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify a comparison function for type %s",
							format_type_be(distributionColumn->vartype)),
					 errdatatype(distributionColumn->vartype),
					 errdetail("Partition column types must have a comparison function "
							   "defined to use range partitioning.")));
		}
	}

	ErrorIfUnsupportedConstraint(relation, distributionMethod, distributionColumn,
								 colocationId);
}


/*
 * CreateHashDistributedTableShards creates shards of given hash distributed table.
 */
static void
CreateHashDistributedTableShards(Oid relationId, uint32 colocationId)
{
	Oid sourceRelationId = ColocatedTableId(colocationId);
	bool useExclusiveConnection = false;
	char relationKind = get_rel_relkind(relationId);

	/*
	 * Ensure schema exists on each worker node. We can not run this function
	 * transactionally, since we may create shards over separate sessions and
	 * shard creation depends on the schema being present and visible from all
	 * sessions.
	 */
	EnsureSchemaExistsOnAllNodes(relationId);

	if (relationKind == RELKIND_RELATION || relationKind == RELKIND_PARTITIONED_TABLE)
	{
		useExclusiveConnection = IsTransactionBlock() || !LocalTableEmpty(relationId);
	}

	if (sourceRelationId != InvalidOid)
	{
		CreateColocatedShards(relationId, sourceRelationId, useExclusiveConnection);
	}
	else
	{
		CreateShardsWithRoundRobinPolicy(relationId, ShardCount, ShardReplicationFactor,
										 useExclusiveConnection);
	}

	/* copy over data for regular relations */
	if (relationKind == RELKIND_RELATION)
	{
		CopyLocalDataIntoShards(relationId);
	}
}


/*
 * create_reference_table accepts a table and then it creates a distributed
 * table which has one shard and replication factor is set to
 * the worker count.
 */
Datum
create_reference_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	CreateReferenceTable(relationId);

	PG_RETURN_VOID();
}


/*
 * CreateReferenceTable creates a distributed table with the given relationId. The
 * created table has one shard and replication factor is set to the active worker
 * count. In fact, the above is the definition of a reference table in Citus.
 */
static void
CreateReferenceTable(Oid relationId)
{
	uint32 colocationId = INVALID_COLOCATION_ID;
	List *workerNodeList = NIL;
	int replicationFactor = 0;
	char *distributionColumnName = NULL;
	char relationKind = 0;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	workerNodeList = ActiveWorkerNodeList();
	replicationFactor = list_length(workerNodeList);

	/* if there are no workers, error out */
	if (replicationFactor == 0)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cannot create reference table \"%s\"", relationName),
						errdetail("There are no active worker nodes.")));
	}


	/* first, convert the relation into distributed relation */
	ConvertToDistributedTable(relationId, distributionColumnName,
							  DISTRIBUTE_BY_NONE, REPLICATION_MODEL_2PC, colocationId);

	/* now, create the single shard replicated to all nodes */
	CreateReferenceTableShard(relationId);

	CreateTableMetadataOnWorkers(relationId);

	/* copy over data for regular relations */
	if (relationKind == RELKIND_RELATION)
	{
		CopyLocalDataIntoShards(relationId);
	}
}


/*
 * ConvertToDistributedTable converts the given regular PostgreSQL table into a
 * distributed table. First, it checks if the given table can be distributed,
 * then it creates related tuple in pg_dist_partition.
 *
 * XXX: We should perform more checks here to see if this table is fit for
 * partitioning. At a minimum, we should validate the following: (i) this node
 * runs as the master node, (ii) table does not make use of the inheritance
 * mechanism and (iii) table does not have collated columns.
 */
static void
ConvertToDistributedTable(Oid relationId, char *distributionColumnName,
						  char distributionMethod, char replicationModel,
						  uint32 colocationId)
{ }


/*
 * LookupDistributionMethod maps the oids of citus.distribution_type enum
 * values to pg_dist_partition.partmethod values.
 *
 * The passed in oid has to belong to a value of citus.distribution_type.
 */
static char
LookupDistributionMethod(Oid distributionMethodOid)
{
	HeapTuple enumTuple = NULL;
	Form_pg_enum enumForm = NULL;
	char distributionMethod = 0;
	const char *enumLabel = NULL;

	enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(distributionMethodOid));
	if (!HeapTupleIsValid(enumTuple))
	{
		ereport(ERROR, (errmsg("invalid internal value for enum: %u",
							   distributionMethodOid)));
	}

	enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
	enumLabel = NameStr(enumForm->enumlabel);

	if (strncmp(enumLabel, "append", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_APPEND;
	}
	else if (strncmp(enumLabel, "hash", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_HASH;
	}
	else if (strncmp(enumLabel, "range", NAMEDATALEN) == 0)
	{
		distributionMethod = DISTRIBUTE_BY_RANGE;
	}
	else
	{
		ereport(ERROR, (errmsg("invalid label for enum: %s", enumLabel)));
	}

	ReleaseSysCache(enumTuple);

	return distributionMethod;
}


/*
 *	SupportFunctionForColumn locates a support function given a column, an access method,
 *	and and id of a support function. This function returns InvalidOid if there is no
 *	support function for the operator class family of the column, but if the data type
 *	of the column has no default operator class whatsoever, this function errors out.
 */
static Oid
SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
						 int16 supportFunctionNumber)
{
	Oid operatorFamilyId = InvalidOid;
	Oid supportFunctionOid = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
	Oid columnOid = partitionColumn->vartype;
	Oid operatorClassId = GetDefaultOpClass(columnOid, accessMethodId);

	/* currently only support using the default operator class */
	if (operatorClassId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("data type %s has no default operator class for specified"
							   " partition method", format_type_be(columnOid)),
						errdatatype(columnOid),
						errdetail("Partition column types must have a default operator"
								  " class defined.")));
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	operatorClassInputType = get_opclass_input_type(operatorClassId);
	supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
										   operatorClassInputType,
										   supportFunctionNumber);

	return supportFunctionOid;
}


/*
 * LocalTableEmpty function checks whether given local table contains any row and
 * returns false if there is any data. This function is only for local tables and
 * should not be called for distributed tables.
 */
static bool
LocalTableEmpty(Oid tableId)
{
	Oid schemaId = get_rel_namespace(tableId);
	char *schemaName = get_namespace_name(schemaId);
	char *tableName = get_rel_name(tableId);
	char *tableQualifiedName = quote_qualified_identifier(schemaName, tableName);

	int spiConnectionResult = 0;
	int spiQueryResult = 0;
	StringInfo selectExistQueryString = makeStringInfo();

	HeapTuple tuple = NULL;
	Datum hasDataDatum = 0;
	bool localTableEmpty = false;
	bool columnNull = false;
	bool readOnly = true;

	int rowId = 0;
	int attributeId = 1;

	AssertArg(!IsDistributedTable(tableId));

	spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	appendStringInfo(selectExistQueryString, SELECT_EXIST_QUERY, tableQualifiedName);

	spiQueryResult = SPI_execute(selectExistQueryString->data, readOnly, 0);
	if (spiQueryResult != SPI_OK_SELECT)
	{
		ereport(ERROR, (errmsg("execution was not successful \"%s\"",
							   selectExistQueryString->data)));
	}

	/* we expect that SELECT EXISTS query will return single value in a single row */
	Assert(SPI_processed == 1);

	tuple = SPI_tuptable->vals[rowId];
	hasDataDatum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, attributeId, &columnNull);
	localTableEmpty = !DatumGetBool(hasDataDatum);

	SPI_finish();

	return localTableEmpty;
}


/*
 * CreateTruncateTrigger creates a truncate trigger on table identified by relationId
 * and assigns citus_truncate_trigger() as handler.
 */
void
CreateTruncateTrigger(Oid relationId)
{
	CreateTrigStmt *trigger = NULL;
	StringInfo triggerName = makeStringInfo();
	bool internal = true;

	appendStringInfo(triggerName, "truncate_trigger");

	trigger = makeNode(CreateTrigStmt);
	trigger->trigname = triggerName->data;
	trigger->relation = NULL;
	trigger->funcname = SystemFuncName("citus_truncate_trigger");
	trigger->args = NIL;
	trigger->row = false;
	trigger->timing = TRIGGER_TYPE_BEFORE;
	trigger->events = TRIGGER_TYPE_TRUNCATE;
	trigger->columns = NIL;
	trigger->whenClause = NULL;
	trigger->isconstraint = false;

	CreateTrigger(trigger, NULL, relationId, InvalidOid, InvalidOid, InvalidOid,
				  internal);
}


/*
 * EnsureSchemaExistsOnAllNodes connects to all nodes with citus extension user
 * and creates the schema of the given relationId. The function errors out if the
 * command cannot be executed in any of the worker nodes.
 */
static void
EnsureSchemaExistsOnAllNodes(Oid relationId)
{
	List *workerNodeList = ActiveWorkerNodeList();
	ListCell *workerNodeCell = NULL;
	StringInfo applySchemaCreationDDL = makeStringInfo();

	Oid schemaId = get_rel_namespace(relationId);
	const char *createSchemaDDL = CreateSchemaDDLCommand(schemaId);
	uint64 connectionFlag = FORCE_NEW_CONNECTION;

	if (createSchemaDDL == NULL)
	{
		return;
	}

	appendStringInfo(applySchemaCreationDDL, "%s", createSchemaDDL);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		uint32 nodePort = workerNode->workerPort;
		MultiConnection *connection =
			GetNodeUserDatabaseConnection(connectionFlag, nodeName, nodePort, NULL,
										  NULL);

		ExecuteCriticalRemoteCommand(connection, applySchemaCreationDDL->data);
	}
}


/*
 * EnsureLocalTableEmpty errors out if the local table is not empty.
 */
static void
EnsureLocalTableEmpty(Oid relationId)
{
	bool localTableEmpty = false;
	char *relationName = get_rel_name(relationId);

	localTableEmpty = LocalTableEmpty(relationId);

	if (!localTableEmpty)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("cannot distribute relation \"%s\"", relationName),
						errdetail("Relation \"%s\" contains data.", relationName),
						errhint("Empty your table before distributing it.")));
	}
}


/*
 * EnsureIsTableId errors out if the id is not belong to a regular of foreign table.
 */
static void
EnsureIsTableId(Oid relationId)
{
	Relation relation = relation_open(relationId, AccessShareLock);
	char *relationName = get_rel_name(relationId);
	char relationKind = 0;

	/* verify target relation is either regular or foreign table */
	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table",
							   relationName)));
	}

	relation_close(relation, NoLock);
}


/*
 * EnsureTableNotDistributed errors out if the relationId doesn't belong to regular or foreign table
 * or the table is distributed.
 */
static void
EnsureTableNotDistributed(Oid relationId)
{
	char *relationName = get_rel_name(relationId);
	bool isDistributedTable = false;

	EnsureIsTableId(relationId);
	isDistributedTable = IsDistributedTable(relationId);

	if (isDistributedTable)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("table \"%s\" is already distributed",
							   relationName)));
	}
}


static void
EnsureLocalTableEmptyIfNecessary(Oid relationId, char distributionMethod)
{
	char relationKind = get_rel_relkind(relationId);

	if (distributionMethod == DISTRIBUTE_BY_HASH)
	{
		EnsureLocalTableEmpty(relationId);
	}
	else if (relationKind != RELKIND_RELATION &&
			 relationKind != RELKIND_PARTITIONED_TABLE)
	{
		EnsureLocalTableEmpty(relationId);
	}
}


/*
 * Check that the current replication factor setting is compatible with the
 * replication model of relationId, if valid. If InvalidOid, check that the
 * global replication model setting instead. Errors out if an invalid state
 * is detected.
 */
void
EnsureReplicationSettings(Oid relationId, char replicationModel)
{
	char *msgSuffix = "the streaming replication model";
	char *extraHint = " or setting \"citus.replication_model\" to \"statement\"";

	if (relationId != InvalidOid)
	{
		msgSuffix = "tables which use the streaming replication model";
		extraHint = "";
	}

	if (replicationModel == REPLICATION_MODEL_STREAMING && ShardReplicationFactor != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication factors above one are incompatible with %s",
							   msgSuffix),
						errhint("Try again after reducing \"citus.shard_replication_"
								"factor\" to one%s.", extraHint)));
	}
}


/*
 * CopyLocalDataIntoShards copies data from the local table, which is hidden
 * after converting it to a distributed table, into the shards of the distributed
 * table.
 *
 * This function uses CitusCopyDestReceiver to invoke the distributed COPY logic.
 * We cannot use a regular COPY here since that cannot read from a table. Instead
 * we read from the table and pass each tuple to the CitusCopyDestReceiver which
 * opens a connection and starts a COPY for each shard placement that will have
 * data.
 *
 * We could call the planner and executor here and send the output to the
 * DestReceiver, but we are in a tricky spot here since Citus is already
 * intercepting queries on this table in the planner and executor hooks and we
 * want to read from the local table. To keep it simple, we perform a heap scan
 * directly on the table.
 *
 * Any writes on the table that are started during this operation will be handled
 * as distributed queries once the current transaction commits. SELECTs will
 * continue to read from the local table until the current transaction commits,
 * after which new SELECTs will be handled as distributed queries.
 *
 * After copying local data into the distributed table, the local data remains
 * in place and should be truncated at a later time.
 */
static void
CopyLocalDataIntoShards(Oid distributedRelationId)
{
	DestReceiver *copyDest = NULL;
	List *columnNameList = NIL;
	Relation distributedRelation = NULL;
	TupleDesc tupleDescriptor = NULL;
	bool stopOnFailure = true;

	EState *estate = NULL;
	HeapScanDesc scan = NULL;
	HeapTuple tuple = NULL;
	ExprContext *econtext = NULL;
	MemoryContext oldContext = NULL;
	TupleTableSlot *slot = NULL;
	uint64 rowsCopied = 0;

	/* take an ExclusiveLock to block all operations except SELECT */
	distributedRelation = heap_open(distributedRelationId, ExclusiveLock);

	/*
	 * Skip copying data to partitioned tables, we will copy the data from
	 * partition to partition's shards.
	 */
	if (PartitionedTable(distributedRelationId))
	{
		return;
	}

	/*
	 * All writes have finished, make sure that we can see them by using the
	 * latest snapshot. We use GetLatestSnapshot instead of
	 * GetTransactionSnapshot since the latter would not reveal all writes
	 * in serializable or repeatable read mode. Note that subsequent reads
	 * from the distributed table would reveal those writes, temporarily
	 * violating the isolation level. However, this seems preferable over
	 * dropping the writes entirely.
	 */
	PushActiveSnapshot(GetLatestSnapshot());

	/* get the table columns */
	tupleDescriptor = RelationGetDescr(distributedRelation);
	slot = MakeSingleTupleTableSlot(tupleDescriptor);
	columnNameList = TupleDescColumnNameList(tupleDescriptor);

	/* initialise per-tuple memory context */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;

	copyDest =
		(DestReceiver *) CreateCitusCopyDestReceiver(distributedRelationId,
													 columnNameList, estate,
													 stopOnFailure);

	/* initialise state for writing to shards, we'll open connections on demand */
	copyDest->rStartup(copyDest, 0, tupleDescriptor);

	/* begin reading from local table */
	scan = heap_beginscan(distributedRelation, GetActiveSnapshot(), 0, NULL);

	oldContext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		/* materialize tuple and send it to a shard */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		copyDest->receiveSlot(slot, copyDest);

		/* clear tuple memory */
		ResetPerTupleExprContext(estate);

		/* make sure we roll back on cancellation */
		CHECK_FOR_INTERRUPTS();

		if (rowsCopied == 0)
		{
			ereport(NOTICE, (errmsg("Copying data from local table...")));
		}

		rowsCopied++;

		if (rowsCopied % 1000000 == 0)
		{
			ereport(DEBUG1, (errmsg("Copied %ld rows", rowsCopied)));
		}
	}

	if (rowsCopied % 1000000 != 0)
	{
		ereport(DEBUG1, (errmsg("Copied %ld rows", rowsCopied)));
	}

	MemoryContextSwitchTo(oldContext);

	/* finish reading from the local table */
	heap_endscan(scan);

	/* finish writing into the shards */
	copyDest->rShutdown(copyDest);

	/* free memory and close the relation */
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	heap_close(distributedRelation, NoLock);

	PopActiveSnapshot();
}


/*
 * TupleDescColumnNameList returns a list of column names for the given tuple
 * descriptor as plain strings.
 */
static List *
TupleDescColumnNameList(TupleDesc tupleDescriptor)
{
	List *columnNameList = NIL;
	int columnIndex = 0;

	for (columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = tupleDescriptor->attrs[columnIndex];
		char *columnName = NameStr(currentColumn->attname);

		if (currentColumn->attisdropped)
		{
			continue;
		}

		columnNameList = lappend(columnNameList, columnName);
	}

	return columnNameList;
}


/*
 * RelationUsesIdentityColumns returns whether a given relation uses the SQL
 * GENERATED ... AS IDENTITY features supported as of PostgreSQL 10.
 */
#if (PG_VERSION_NUM >= 100000)
static bool
RelationUsesIdentityColumns(TupleDesc relationDesc)
{
	int attributeIndex = 0;

	for (attributeIndex = 0; attributeIndex < relationDesc->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = relationDesc->attrs[attributeIndex];

		if (attributeForm->attidentity != '\0')
		{
			return true;
		}
	}

	return false;
}


#endif
