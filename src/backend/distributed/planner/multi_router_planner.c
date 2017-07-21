/*-------------------------------------------------------------------------
 *
 * multi_router_planner.c
 *
 * This file contains functions to plan single shard queries
 * including distributed table modifications.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stddef.h>

#include "access/stratnum.h"
#include "access/xact.h"
#include "catalog/pg_opfamily.h"
#include "distributed/citus_clauses.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distribution_column.h"
#include "distributed/errormessage.h"
#include "distributed/insert_select_planner.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_pruning.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "catalog/pg_proc.h"
#include "optimizer/planmain.h"


typedef struct WalkerState
{
	bool containsVar;
	bool varArgument;
	bool badCoalesce;
} WalkerState;

bool EnableRouterExecution = true;


/* planner functions forward declarations */
static MultiPlan * CreateSingleTaskRouterPlan(Query *originalQuery,
											  Query *query,
											  RelationRestrictionContext *
											  restrictionContext);
static bool MasterIrreducibleExpression(Node *expression, bool *varArgument,
										bool *badCoalesce);
static bool MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state);
static char MostPermissiveVolatileFlag(char left, char right);
static bool TargetEntryChangesValue(TargetEntry *targetEntry, Var *column,
									FromExpr *joinTree);
static Job * RouterModifyJob(Query *originalQuery, Query *query,
							 DeferredErrorMessage **planningError);
static void ErrorIfNoShardsExist(DistTableCacheEntry *cacheEntry);
static bool CanShardPrune(Oid distributedTableId, Query *query);
static List * RouterInsertTaskList(Query *query, DistTableCacheEntry *cacheEntry,
								   DeferredErrorMessage **planningError);
static List * RouterUpdateOrDeleteTaskList(Query *query, DistTableCacheEntry *cacheEntry,
										   DeferredErrorMessage **planningError);
static Job * CreateJob(Query *query);
static Task * CreateTask(TaskType taskType);
static ShardInterval * FindShardForInsert(Query *query, DistTableCacheEntry *cacheEntry,
										  DeferredErrorMessage **planningError);
static ShardInterval * FindShardForUpdateOrDelete(Query *query,
												  DistTableCacheEntry *cacheEntry,
												  DeferredErrorMessage **planningError);
static List * QueryRestrictList(Query *query, char partitionMethod);
static Expr * ExtractInsertPartitionValue(Query *query, Var *partitionColumn);
static Job * RouterSelectJob(Query *originalQuery,
							 RelationRestrictionContext *restrictionContext,
							 DeferredErrorMessage **planningError);
static bool RelationPrunesToMultipleShards(List *relationShardList);
static List * TargetShardIntervalsForRouter(Query *query,
											RelationRestrictionContext *restrictionContext);
static List * WorkersContainingAllShards(List *prunedShardIntervalsList);
static bool MultiRouterPlannableQuery(Query *query,
									  RelationRestrictionContext *restrictionContext);
static DeferredErrorMessage * ErrorIfQueryHasModifyingCTE(Query *queryTree);
#if (PG_VERSION_NUM >= 100000)
static List * get_all_actual_clauses(List *restrictinfo_list);
#endif


/*
 * CreateRouterPlan attempts to create a router executor plan for the given
 * SELECT statement.  If planning fails either NULL is returned, or
 * ->planningError is set to a description of the failure.
 */
MultiPlan *
CreateRouterPlan(Query *originalQuery, Query *query,
				 RelationRestrictionContext *restrictionContext)
{
	Assert(EnableRouterExecution);

	if (MultiRouterPlannableQuery(query, restrictionContext))
	{
		return CreateSingleTaskRouterPlan(originalQuery, query,
										  restrictionContext);
	}

	/*
	 * TODO: Instead have MultiRouterPlannableQuery set an error describing
	 * why router cannot support the query.
	 */
	return NULL;
}


/*
 * CreateModifyPlan attempts to create a plan the given modification
 * statement.  If planning fails ->planningError is set to a description of
 * the failure.
 */
MultiPlan *
CreateModifyPlan(Query *originalQuery, Query *query,
				 PlannerRestrictionContext *plannerRestrictionContext)
{
	Job *job = NULL;
	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);
	bool onlySimpleUpdates = false;

	multiPlan->operation = query->commandType;

	multiPlan->planningError = ModifyQuerySupported(query, onlySimpleUpdates);
	if (multiPlan->planningError != NULL)
	{
		return multiPlan;
	}

	if (multiPlan->operation == CMD_UPDATE)
	{
		RelationRestrictionContext *restrictionContext =
			plannerRestrictionContext->relationRestrictionContext;
		job = RouterSelectJob(originalQuery, restrictionContext,
							  &multiPlan->planningError);
	}
	else
	{
		job = RouterModifyJob(originalQuery, query, &multiPlan->planningError);
	}

	if (multiPlan->planningError != NULL)
	{
		return multiPlan;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->routerExecutable = true;
	multiPlan->hasReturning = false;

	if (list_length(originalQuery->returningList) > 0)
	{
		multiPlan->hasReturning = true;
	}

	return multiPlan;
}


/*
 * CreateSingleTaskRouterPlan creates a physical plan for given query. The created plan is
 * either a modify task that changes a single shard, or a router task that returns
 * query results from a single worker. Supported modify queries (insert/update/delete)
 * are router plannable by default. If query is not router plannable then either NULL is
 * returned, or the returned plan has planningError set to a description of the problem.
 */
static MultiPlan *
CreateSingleTaskRouterPlan(Query *originalQuery, Query *query,
						   RelationRestrictionContext *restrictionContext)
{
	Job *job = NULL;
	MultiPlan *multiPlan = CitusMakeNode(MultiPlan);

	multiPlan->operation = query->commandType;

	/* FIXME: this should probably rather be inlined into CreateRouterPlan */
	multiPlan->planningError = ErrorIfQueryHasModifyingCTE(query);
	if (multiPlan->planningError)
	{
		return multiPlan;
	}

	job = RouterSelectJob(originalQuery, restrictionContext, &multiPlan->planningError);
	if (multiPlan->planningError)
	{
		/* query cannot be handled by this planner */
		return NULL;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->routerExecutable = true;
	multiPlan->hasReturning = false;

	return multiPlan;
}


/*
 * ShardIntervalOpExpressions returns a list of OpExprs with exactly two
 * items in it. The list consists of shard interval ranges with partition columns
 * such as (partitionColumn >= shardMinValue) and (partitionColumn <= shardMaxValue).
 *
 * The function returns hashed columns generated by MakeInt4Column() for the hash
 * partitioned tables in place of partition columns.
 *
 * The function errors out if the given shard interval does not belong to a hash,
 * range and append distributed tables.
 *
 * NB: If you update this, also look at PrunableExpressionsWalker().
 */
List *
ShardIntervalOpExpressions(ShardInterval *shardInterval, Index rteIndex)
{
	Oid relationId = shardInterval->relationId;
	char partitionMethod = PartitionMethod(shardInterval->relationId);
	Var *partitionColumn = NULL;
	Node *baseConstraint = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		partitionColumn = MakeInt4Column();
	}
	else if (partitionMethod == DISTRIBUTE_BY_RANGE || partitionMethod ==
			 DISTRIBUTE_BY_APPEND)
	{
		Assert(rteIndex > 0);
		partitionColumn = PartitionColumn(relationId, rteIndex);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot create shard interval operator expression for "
							   "distributed relations other than hash, range and append distributed "
							   "relations")));
	}

	/* build the base expression for constraint */
	baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	if (shardInterval->minValueExists && shardInterval->maxValueExists)
	{
		UpdateConstraint(baseConstraint, shardInterval);
	}

	return list_make1(baseConstraint);
}


/*
 * AddShardIntervalRestrictionToSelect adds the following range boundaries
 * with the given subquery and shardInterval:
 *
 *    hashfunc(partitionColumn) >= $lower_bound AND
 *    hashfunc(partitionColumn) <= $upper_bound
 *
 * The function expects and asserts that subquery's target list contains a partition
 * column value. Thus, this function should never be called with reference tables.
 */
void
AddShardIntervalRestrictionToSelect(Query *subqery, ShardInterval *shardInterval)
{
	List *targetList = subqery->targetList;
	ListCell *targetEntryCell = NULL;
	Var *targetPartitionColumnVar = NULL;
	Oid integer4GEoperatorId = InvalidOid;
	Oid integer4LEoperatorId = InvalidOid;
	TypeCacheEntry *typeEntry = NULL;
	FuncExpr *hashFunctionExpr = NULL;
	OpExpr *greaterThanAndEqualsBoundExpr = NULL;
	OpExpr *lessThanAndEqualsBoundExpr = NULL;
	List *boundExpressionList = NIL;
	Expr *andedBoundExpressions = NULL;

	/* iterate through the target entries */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		if (IsPartitionColumn(targetEntry->expr, subqery) &&
			IsA(targetEntry->expr, Var))
		{
			targetPartitionColumnVar = (Var *) targetEntry->expr;
			break;
		}
	}

	/* we should have found target partition column */
	Assert(targetPartitionColumnVar != NULL);

	integer4GEoperatorId = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID,
											   INT4OID,
											   BTGreaterEqualStrategyNumber);
	integer4LEoperatorId = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID,
											   INT4OID,
											   BTLessEqualStrategyNumber);

	/* ensure that we find the correct operators */
	Assert(integer4GEoperatorId != InvalidOid);
	Assert(integer4LEoperatorId != InvalidOid);

	/* look up the type cache */
	typeEntry = lookup_type_cache(targetPartitionColumnVar->vartype,
								  TYPECACHE_HASH_PROC_FINFO);

	/* probable never possible given that the tables are already hash partitioned */
	if (!OidIsValid(typeEntry->hash_proc_finfo.fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify a hash function for type %s",
							   format_type_be(targetPartitionColumnVar->vartype))));
	}

	/* generate hashfunc(partCol) expression */
	hashFunctionExpr = makeNode(FuncExpr);
	hashFunctionExpr->funcid = CitusWorkerHashFunctionId();
	hashFunctionExpr->args = list_make1(targetPartitionColumnVar);

	/* hash functions always return INT4 */
	hashFunctionExpr->funcresulttype = INT4OID;

	/* generate hashfunc(partCol) >= shardMinValue OpExpr */
	greaterThanAndEqualsBoundExpr =
		(OpExpr *) make_opclause(integer4GEoperatorId,
								 InvalidOid, false,
								 (Expr *) hashFunctionExpr,
								 (Expr *) MakeInt4Constant(shardInterval->minValue),
								 targetPartitionColumnVar->varcollid,
								 targetPartitionColumnVar->varcollid);

	/* update the operators with correct operator numbers and function ids */
	greaterThanAndEqualsBoundExpr->opfuncid =
		get_opcode(greaterThanAndEqualsBoundExpr->opno);
	greaterThanAndEqualsBoundExpr->opresulttype =
		get_func_rettype(greaterThanAndEqualsBoundExpr->opfuncid);

	/* generate hashfunc(partCol) <= shardMinValue OpExpr */
	lessThanAndEqualsBoundExpr =
		(OpExpr *) make_opclause(integer4LEoperatorId,
								 InvalidOid, false,
								 (Expr *) hashFunctionExpr,
								 (Expr *) MakeInt4Constant(shardInterval->maxValue),
								 targetPartitionColumnVar->varcollid,
								 targetPartitionColumnVar->varcollid);

	/* update the operators with correct operator numbers and function ids */
	lessThanAndEqualsBoundExpr->opfuncid = get_opcode(lessThanAndEqualsBoundExpr->opno);
	lessThanAndEqualsBoundExpr->opresulttype =
		get_func_rettype(lessThanAndEqualsBoundExpr->opfuncid);

	/* finally add the operators to a list and make them explicitly anded */
	boundExpressionList = lappend(boundExpressionList, greaterThanAndEqualsBoundExpr);
	boundExpressionList = lappend(boundExpressionList, lessThanAndEqualsBoundExpr);

	andedBoundExpressions = make_ands_explicit(boundExpressionList);

	/* finally add the quals */
	if (subqery->jointree->quals == NULL)
	{
		subqery->jointree->quals = (Node *) andedBoundExpressions;
	}
	else
	{
		subqery->jointree->quals = make_and_qual(subqery->jointree->quals,
												 (Node *) andedBoundExpressions);
	}
}


/*
 * ExtractSelectRangeTableEntry returns the range table entry of the subquery.
 * Note that the function expects and asserts that the input query be
 * an INSERT...SELECT query.
 */
RangeTblEntry *
ExtractSelectRangeTableEntry(Query *query)
{
	List *fromList = NULL;
	RangeTblRef *reference = NULL;
	RangeTblEntry *subqueryRte = NULL;

	Assert(InsertSelectIntoDistributedTable(query));

	/*
	 * Since we already asserted InsertSelectIntoDistributedTable() it is safe to access
	 * both lists
	 */
	fromList = query->jointree->fromlist;
	reference = linitial(fromList);
	subqueryRte = rt_fetch(reference->rtindex, query->rtable);

	return subqueryRte;
}


/*
 * ExtractInsertRangeTableEntry returns the INSERT'ed table's range table entry.
 * Note that the function expects and asserts that the input query be
 * an INSERT...SELECT query.
 */
RangeTblEntry *
ExtractInsertRangeTableEntry(Query *query)
{
	int resultRelation = query->resultRelation;
	List *rangeTableList = query->rtable;
	RangeTblEntry *insertRTE = NULL;

	insertRTE = rt_fetch(resultRelation, rangeTableList);

	return insertRTE;
}


/*
 * ModifyQuerySupported returns NULL if the query only contains supported
 * features, otherwise it returns an error description.
 */
DeferredErrorMessage *
ModifyQuerySupported(Query *queryTree, bool onlySimpleUpdates)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	bool isCoordinator = IsCoordinator();
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool specifiesPartitionValue = false;
	ListCell *setTargetCell = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;

	CmdType commandType = queryTree->commandType;

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		/* we have an exception for UPDATE queries if onlySimpleUpdates is set false */
		if (commandType != CMD_UPDATE || onlySimpleUpdates)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modifications",
								 "Subqueries are not supported in distributed "
								 "modifications.", NULL);
		}
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "common table expressions are not supported in distributed "
							 "modifications",
							 NULL, NULL);
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		bool referenceTable = false;

		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			/*
			 * We are sure that the table should be distributed, therefore no need to
			 * call IsDistributedTable() here and DistributedTableCacheEntry will
			 * error out if the table is not distributed
			 */
			DistTableCacheEntry *distTableEntry =
				DistributedTableCacheEntry(rangeTableEntry->relid);

			if (distTableEntry->partitionMethod == DISTRIBUTE_BY_NONE)
			{
				referenceTable = true;
			}

			if (referenceTable && !isCoordinator)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot perform distributed planning for the given"
									 " modification",
									 "Modifications to reference tables are "
									 "supported only from the coordinator.",
									 NULL);
			}

			queryTableCount++;

			/* we do not expect to see a view in modify query */
			if (rangeTableEntry->relkind == RELKIND_VIEW)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot modify views over distributed tables",
									 NULL, NULL);
			}
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else
		{
			char *rangeTableEntryErrorDetail = NULL;

			/* support updates with subqueries */
			if (commandType == CMD_UPDATE && !onlySimpleUpdates)
			{
				continue;
			}

			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 * We do not need to check for RTE_CTE because all common table expressions
			 * are rejected above with queryTree->cteList check.
			 */
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				rangeTableEntryErrorDetail = "Subqueries are not supported in"
											 " distributed modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_JOIN)
			{
				rangeTableEntryErrorDetail = "Joins are not supported in distributed"
											 " modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_FUNCTION)
			{
				rangeTableEntryErrorDetail = "Functions must not appear in the FROM"
											 " clause of a distributed modifications.";
			}
			else
			{
				rangeTableEntryErrorDetail = "Unrecognized range table entry.";
			}

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modifications",
								 rangeTableEntryErrorDetail,
								 NULL);
		}
	}

	/*
	 * Reject queries which involve joins. Note that UPSERTs are exceptional for this case.
	 * Queries like "INSERT INTO table_name ON CONFLICT DO UPDATE (col) SET other_col = ''"
	 * contains two range table entries, and we have to allow them.
	 */
	if (commandType != CMD_INSERT && queryTableCount != 1)
	{
		/* UPDATEs with subqueries are also an exceptional case */
		if (commandType != CMD_UPDATE || onlySimpleUpdates)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modification",
								 "Joins are not supported in distributed "
								 "modifications.",
								 NULL);
		}
	}

	/* reject queries which involve multi-row inserts */
	if (hasValuesScan)
	{
		/*
		 * NB: If you remove this check you must also change the checks further in this
		 * method and ensure that VOLATILE function calls aren't allowed in INSERT
		 * statements. Currently they're allowed but the function call is replaced
		 * with a constant, and if you're inserting multiple rows at once the function
		 * should return a different value for each row.
		 */
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot perform distributed planning for the given"
							 " modification",
							 "Multi-row INSERTs to distributed tables are not "
							 "supported.",
							 NULL);
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
		bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
		FromExpr *joinTree = queryTree->jointree;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
			bool targetEntryPartitionColumn = false;

			/* reference tables do not have partition column */
			if (partitionColumn == NULL)
			{
				targetEntryPartitionColumn = false;
			}
			else if (targetEntry->resno == partitionColumn->varattno)
			{
				targetEntryPartitionColumn = true;
			}

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (commandType == CMD_UPDATE &&
				contain_volatile_functions((Node *) targetEntry->expr))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in UPDATE queries on distributed "
									 "tables must not be VOLATILE",
									 NULL, NULL);
			}

			if (commandType == CMD_UPDATE && targetEntryPartitionColumn &&
				TargetEntryChangesValue(targetEntry, partitionColumn,
										queryTree->jointree))
			{
				specifiesPartitionValue = true;
			}

			if (commandType == CMD_UPDATE &&
				MasterIrreducibleExpression((Node *) targetEntry->expr,
											&hasVarArgument, &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (joinTree != NULL)
		{
			if (contain_volatile_functions(joinTree->quals))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in the WHERE clause of modification "
									 "queries on distributed tables must not be VOLATILE",
									 NULL, NULL);
			}
			else if (MasterIrreducibleExpression(joinTree->quals, &hasVarArgument,
												 &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (hasVarArgument)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "STABLE functions used in UPDATE queries "
								 "cannot be called with column references",
								 NULL, NULL);
		}

		if (hasBadCoalesce)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "non-IMMUTABLE functions are not allowed in CASE or "
								 "COALESCE statements",
								 NULL, NULL);
		}

		if (contain_mutable_functions((Node *) queryTree->returningList))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "non-IMMUTABLE functions are not allowed in the "
								 "RETURNING clause",
								 NULL, NULL);
		}
	}

	if (commandType == CMD_INSERT && queryTree->onConflict != NULL)
	{
		onConflictSet = queryTree->onConflict->onConflictSet;
		arbiterWhere = queryTree->onConflict->arbiterWhere;
		onConflictWhere = queryTree->onConflict->onConflictWhere;
	}

	/*
	 * onConflictSet is expanded via expand_targetlist() on the standard planner.
	 * This ends up adding all the columns to the onConflictSet even if the user
	 * does not explicitly state the columns in the query.
	 *
	 * The following loop simply allows "DO UPDATE SET part_col = table.part_col"
	 * types of elements in the target list, which are added by expand_targetlist().
	 * Any other attempt to update partition column value is forbidden.
	 */
	foreach(setTargetCell, onConflictSet)
	{
		TargetEntry *setTargetEntry = (TargetEntry *) lfirst(setTargetCell);
		bool setTargetEntryPartitionColumn = false;

		/* reference tables do not have partition column */
		if (partitionColumn == NULL)
		{
			setTargetEntryPartitionColumn = false;
		}
		else if (setTargetEntry->resno == partitionColumn->varattno)
		{
			setTargetEntryPartitionColumn = true;
		}

		if (setTargetEntryPartitionColumn)
		{
			Expr *setExpr = setTargetEntry->expr;
			if (IsA(setExpr, Var) &&
				((Var *) setExpr)->varattno == partitionColumn->varattno)
			{
				specifiesPartitionValue = false;
			}
			else
			{
				specifiesPartitionValue = true;
			}
		}
		else
		{
			/*
			 * Similarly, allow  "DO UPDATE SET col_1 = table.col_1" types of
			 * target list elements. Note that, the following check allows
			 * "DO UPDATE SET col_1 = table.col_2", which is not harmful.
			 */
			if (IsA(setTargetEntry->expr, Var))
			{
				continue;
			}
			else if (contain_mutable_functions((Node *) setTargetEntry->expr))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in the DO UPDATE SET clause of "
									 "INSERTs on distributed tables must be marked "
									 "IMMUTABLE",
									 NULL, NULL);
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "functions used in the WHERE clause of the "
							 "ON CONFLICT clause of INSERTs on distributed "
							 "tables must be marked IMMUTABLE",
							 NULL, NULL);
	}

	if (specifiesPartitionValue)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "modifying the partition value of rows is not "
							 "allowed",
							 NULL, NULL);
	}

	return NULL;
}


/*
 * If the expression contains STABLE functions which accept any parameters derived from a
 * Var returns true and sets varArgument.
 *
 * If the expression contains a CASE or COALESCE which invoke non-IMMUTABLE functions
 * returns true and sets badCoalesce.
 *
 * Assumes the expression contains no VOLATILE functions.
 *
 * Var's are allowed, but only if they are passed solely to IMMUTABLE functions
 *
 * We special-case CASE/COALESCE because those are evaluated lazily. We could evaluate
 * CASE/COALESCE expressions which don't reference Vars, or partially evaluate some
 * which do, but for now we just error out. That makes both the code and user-education
 * easier.
 */
static bool
MasterIrreducibleExpression(Node *expression, bool *varArgument, bool *badCoalesce)
{
	bool result;
	WalkerState data;
	data.containsVar = data.varArgument = data.badCoalesce = false;

	result = MasterIrreducibleExpressionWalker(expression, &data);

	*varArgument |= data.varArgument;
	*badCoalesce |= data.badCoalesce;
	return result;
}


static bool
MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state)
{
	char volatileFlag = 0;
	WalkerState childState = { false, false, false };
	bool containsDisallowedFunction = false;

	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, CoalesceExpr))
	{
		CoalesceExpr *expr = (CoalesceExpr *) expression;

		if (contain_mutable_functions((Node *) (expr->args)))
		{
			state->badCoalesce = true;
			return true;
		}
		else
		{
			/*
			 * There's no need to recurse. Since there are no STABLE functions
			 * varArgument will never be set.
			 */
			return false;
		}
	}

	if (IsA(expression, CaseExpr))
	{
		if (contain_mutable_functions(expression))
		{
			state->badCoalesce = true;
			return true;
		}

		return false;
	}

	if (IsA(expression, Var))
	{
		state->containsVar = true;
		return false;
	}

	/*
	 * In order for statement replication to give us consistent results it's important
	 * that we either disallow or evaluate on the master anything which has a volatility
	 * category above IMMUTABLE. Newer versions of postgres might add node types which
	 * should be checked in this function.
	 *
	 * Look through contain_mutable_functions_walker or future PG's equivalent for new
	 * node types before bumping this version number to fix compilation; e.g. for any
	 * PostgreSQL after 9.5, see check_functions_in_node.
	 *
	 * Once you've added them to this check, make sure you also evaluate them in the
	 * executor!
	 */
	StaticAssertStmt(PG_VERSION_NUM < 100100, "When porting to a newer PG this section"
											  " needs to be reviewed.");
	if (IsA(expression, Aggref))
	{
		Aggref *expr = (Aggref *) expression;

		volatileFlag = func_volatile(expr->aggfnoid);
	}
	else if (IsA(expression, WindowFunc))
	{
		WindowFunc *expr = (WindowFunc *) expression;

		volatileFlag = func_volatile(expr->winfnoid);
	}
	else if (IsA(expression, OpExpr))
	{
		OpExpr *expr = (OpExpr *) expression;

		set_opfuncid(expr);
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) expression;

		volatileFlag = func_volatile(expr->funcid);
	}
	else if (IsA(expression, DistinctExpr))
	{
		/*
		 * to exercise this, you need to create a custom type for which the '=' operator
		 * is STABLE/VOLATILE
		 */
		DistinctExpr *expr = (DistinctExpr *) expression;

		set_opfuncid((OpExpr *) expr);  /* rely on struct equivalence */
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, NullIfExpr))
	{
		/*
		 * same as above, exercising this requires a STABLE/VOLATILE '=' operator
		 */
		NullIfExpr *expr = (NullIfExpr *) expression;

		set_opfuncid((OpExpr *) expr);  /* rely on struct equivalence */
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, ScalarArrayOpExpr))
	{
		/*
		 * to exercise this you need to CREATE OPERATOR with a binary predicate
		 * and use it within an ANY/ALL clause.
		 */
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) expression;

		set_sa_opfuncid(expr);
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, CoerceViaIO))
	{
		/*
		 * to exercise this you need to use a type with a STABLE/VOLATILE intype or
		 * outtype.
		 */
		CoerceViaIO *expr = (CoerceViaIO *) expression;
		Oid iofunc;
		Oid typioparam;
		bool typisvarlena;

		/* check the result type's input function */
		getTypeInputInfo(expr->resulttype,
						 &iofunc, &typioparam);
		volatileFlag = MostPermissiveVolatileFlag(volatileFlag, func_volatile(iofunc));

		/* check the input type's output function */
		getTypeOutputInfo(exprType((Node *) expr->arg),
						  &iofunc, &typisvarlena);
		volatileFlag = MostPermissiveVolatileFlag(volatileFlag, func_volatile(iofunc));
	}
	else if (IsA(expression, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) expression;

		if (OidIsValid(expr->elemfuncid))
		{
			volatileFlag = func_volatile(expr->elemfuncid);
		}
	}
	else if (IsA(expression, RowCompareExpr))
	{
		RowCompareExpr *rcexpr = (RowCompareExpr *) expression;
		ListCell *opid;

		foreach(opid, rcexpr->opnos)
		{
			volatileFlag = MostPermissiveVolatileFlag(volatileFlag,
													  op_volatile(lfirst_oid(opid)));
		}
	}
	else if (IsA(expression, Query))
	{
		/* subqueries aren't allowed and fail before control reaches this point */
		Assert(false);
	}

	if (volatileFlag == PROVOLATILE_VOLATILE)
	{
		/* the caller should have already checked for this */
		Assert(false);
	}
	else if (volatileFlag == PROVOLATILE_STABLE)
	{
		containsDisallowedFunction =
			expression_tree_walker(expression,
								   MasterIrreducibleExpressionWalker,
								   &childState);

		if (childState.containsVar)
		{
			state->varArgument = true;
		}

		state->badCoalesce |= childState.badCoalesce;
		state->varArgument |= childState.varArgument;

		return (containsDisallowedFunction || childState.containsVar);
	}

	/* keep traversing */
	return expression_tree_walker(expression,
								  MasterIrreducibleExpressionWalker,
								  state);
}


/*
 * Return the most-pessimistic volatility flag of the two params.
 *
 * for example: given two flags, if one is stable and one is volatile, an expression
 * involving both is volatile.
 */
char
MostPermissiveVolatileFlag(char left, char right)
{
	if (left == PROVOLATILE_VOLATILE || right == PROVOLATILE_VOLATILE)
	{
		return PROVOLATILE_VOLATILE;
	}
	else if (left == PROVOLATILE_STABLE || right == PROVOLATILE_STABLE)
	{
		return PROVOLATILE_STABLE;
	}
	else
	{
		return PROVOLATILE_IMMUTABLE;
	}
}


/*
 * TargetEntryChangesValue determines whether the given target entry may
 * change the value in a given column, given a join tree. The result is
 * true unless the expression refers directly to the column, or the
 * expression is a value that is implied by the qualifiers of the join
 * tree, or the target entry sets a different column.
 */
static bool
TargetEntryChangesValue(TargetEntry *targetEntry, Var *column, FromExpr *joinTree)
{
	bool isColumnValueChanged = true;
	Expr *setExpr = targetEntry->expr;

	if (targetEntry->resno != column->varattno)
	{
		/* target entry of the form SET some_other_col = <x> */
		isColumnValueChanged = false;
	}
	else if (IsA(setExpr, Var))
	{
		Var *newValue = (Var *) setExpr;
		if (newValue->varattno == column->varattno)
		{
			/* target entry of the form SET col = table.col */
			isColumnValueChanged = false;
		}
	}
	else if (IsA(setExpr, Const))
	{
		Const *newValue = (Const *) setExpr;
		List *restrictClauseList = WhereClauseList(joinTree);
		OpExpr *equalityExpr = MakeOpExpression(column, BTEqualStrategyNumber);
		Const *rightConst = (Const *) get_rightop((Expr *) equalityExpr);
		bool predicateIsImplied = false;

		rightConst->constvalue = newValue->constvalue;
		rightConst->constisnull = newValue->constisnull;
		rightConst->constbyval = newValue->constbyval;

#if (PG_VERSION_NUM >= 100000)
		predicateIsImplied = predicate_implied_by(list_make1(equalityExpr),
												  restrictClauseList, false);
#else
		predicateIsImplied = predicate_implied_by(list_make1(equalityExpr),
												  restrictClauseList);
#endif
		if (predicateIsImplied)
		{
			/* target entry of the form SET col = <x> WHERE col = <x> AND ... */
			isColumnValueChanged = false;
		}
	}

	return isColumnValueChanged;
}


/*
 * RouterModifyJob builds a Job to represent a modification performed by
 * the provided query against the provided shard interval. This task contains
 * shard-extended deparsed SQL to be run during execution.
 */
static Job *
RouterModifyJob(Query *originalQuery, Query *query, DeferredErrorMessage **planningError)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	List *taskList = NIL;
	Job *job = NULL;
	bool requiresMasterEvaluation = false;
	bool deferredPruning = false;

	if (!CanShardPrune(distributedTableId, query))
	{
		/* there is a non-constant in the partition column, cannot prune yet */
		taskList = NIL;
		deferredPruning = true;

		/* must evaluate the non-constant in the partition column */
		requiresMasterEvaluation = true;
	}
	else
	{
		taskList = RouterModifyTaskList(query, planningError);
		if (*planningError)
		{
			return NULL;
		}

		/* determine whether there are function calls to evaluate */
		requiresMasterEvaluation = RequiresMasterEvaluation(originalQuery);
	}

	if (!requiresMasterEvaluation)
	{
		/* no functions or parameters, build the query strings upfront */
		RebuildQueryStrings(originalQuery, taskList);
	}

	job = CreateJob(originalQuery);
	job->taskList = taskList;
	job->requiresMasterEvaluation = requiresMasterEvaluation;
	job->deferredPruning = deferredPruning;

	return job;
}


/*
 * CreateJob returns a new Job for the given query.
 */
static Job *
CreateJob(Query *query)
{
	Job *job = NULL;

	job = CitusMakeNode(Job);
	job->dependedJobList = NIL;
	job->dependedJobList = NIL;
	job->jobId = INVALID_JOB_ID;
	job->subqueryPushdown = false;
	job->jobQuery = query;
	job->taskList = NIL;
	job->requiresMasterEvaluation = false;

	return job;
}


/*
 * CanShardPrune determines whether a query is ready for shard pruning
 * by checking whether there is a constant value in the partition column.
 */
static bool
CanShardPrune(Oid distributedTableId, Query *query)
{
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	Expr *partitionValueExpr = NULL;

	if (query->commandType != CMD_INSERT)
	{
		/* we assume UPDATE/DELETE is always prunable */
		return true;
	}

	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	if (partitionColumn == NULL)
	{
		/* can always do shard pruning for reference tables */
		return true;
	}

	partitionValueExpr = ExtractInsertPartitionValue(query, partitionColumn);
	if (IsA(partitionValueExpr, Const))
	{
		/* can do shard pruning if the partition column is constant */
		return true;
	}

	return false;
}


/*
 * RouterModifyTaskList builds a list of tasks for a given query.
 */
List *
RouterModifyTaskList(Query *query, DeferredErrorMessage **planningError)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	CmdType commandType = query->commandType;
	List *taskList = NIL;

	ErrorIfNoShardsExist(cacheEntry);

	if (commandType == CMD_INSERT)
	{
		taskList = RouterInsertTaskList(query, cacheEntry, planningError);
	}
	else
	{
		taskList = RouterUpdateOrDeleteTaskList(query, cacheEntry, planningError);
		if (*planningError)
		{
			return NIL;
		}
	}

	return taskList;
}


/*
 * ErrorIfNoShardsExist throws an error if the given table has no shards.
 */
static void
ErrorIfNoShardsExist(DistTableCacheEntry *cacheEntry)
{
	int shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		Oid distributedTableId = cacheEntry->relationId;
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}
}


/*
 * RouterInsertTaskList generates a list of tasks for performing an INSERT on
 * a distributed table via the router executor.
 */
static List *
RouterInsertTaskList(Query *query, DistTableCacheEntry *cacheEntry,
					 DeferredErrorMessage **planningError)
{
	ShardInterval *shardInterval = NULL;
	Task *modifyTask = NULL;

	Assert(query->commandType == CMD_INSERT);

	shardInterval = FindShardForInsert(query, cacheEntry, planningError);

	if (*planningError != NULL)
	{
		return NIL;
	}

	/* an INSERT always routes to exactly one shard */
	Assert(shardInterval != NULL);

	modifyTask = CreateTask(MODIFY_TASK);
	modifyTask->anchorShardId = shardInterval->shardId;
	modifyTask->replicationModel = cacheEntry->replicationModel;

	if (query->onConflict != NULL)
	{
		modifyTask->upsertQuery = true;
	}

	return list_make1(modifyTask);
}


/*
 * RouterUpdateOrDeleteTaskList returns a list of tasks for executing an UPDATE
 * or DELETE command on a distributed table via the router executor.
 */
static List *
RouterUpdateOrDeleteTaskList(Query *query, DistTableCacheEntry *cacheEntry,
							 DeferredErrorMessage **planningError)
{
	ShardInterval *shardInterval = NULL;
	List *taskList = NIL;

	Assert(query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE);

	shardInterval = FindShardForUpdateOrDelete(query, cacheEntry, planningError);

	if (*planningError != NULL)
	{
		return NIL;
	}

	if (shardInterval != NULL)
	{
		Task *modifyTask = NULL;

		modifyTask = CreateTask(MODIFY_TASK);
		modifyTask->anchorShardId = shardInterval->shardId;
		modifyTask->replicationModel = cacheEntry->replicationModel;

		taskList = lappend(taskList, modifyTask);
	}

	return taskList;
}


/*
 * CreateTask returns a new Task with the given type.
 */
static Task *
CreateTask(TaskType taskType)
{
	Task *task = NULL;

	task = CitusMakeNode(Task);
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	task->taskType = taskType;
	task->queryString = NULL;
	task->anchorShardId = INVALID_SHARD_ID;
	task->dependedTaskList = NIL;
	task->replicationModel = REPLICATION_MODEL_INVALID;
	task->upsertQuery = false;
	task->relationShardList = NIL;

	return task;
}


/*
 * FindShardForInsert returns the shard interval for an INSERT query or NULL if
 * the partition column value is defined as an expression that still needs to be
 * evaluated. If the partition column value falls within 0 or multiple
 * (overlapping) shards, the planningError is set.
 */
static ShardInterval *
FindShardForInsert(Query *query, DistTableCacheEntry *cacheEntry,
				   DeferredErrorMessage **planningError)
{
	Oid distributedTableId = cacheEntry->relationId;
	char partitionMethod = cacheEntry->partitionMethod;
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	Expr *partitionValueExpr = NULL;
	Const *partitionValueConst = NULL;
	int prunedShardCount = 0;
	List *prunedShardList = NIL;

	Assert(query->commandType == CMD_INSERT);

	/* reference tables do not have a partition column, but can only have one shard */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		int shardCount = cacheEntry->shardIntervalArrayLength;
		if (shardCount != 1)
		{
			ereport(ERROR, (errmsg("reference table cannot have %d shards", shardCount)));
		}

		return cacheEntry->sortedShardIntervalArray[0];
	}

	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	partitionValueExpr = ExtractInsertPartitionValue(query, partitionColumn);

	/* non-constants should have been caught by CanShardPrune */
	if (!IsA(partitionValueExpr, Const))
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot perform an INSERT with a non-constant in the "
							   "partition column")));
	}

	partitionValueConst = (Const *) partitionValueExpr;
	if (partitionValueConst->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot perform an INSERT with NULL in the partition "
							   "column")));
	}

	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_RANGE)
	{
		Datum partitionValue = partitionValueConst->constvalue;
		ShardInterval *shardInterval = FindShardInterval(partitionValue, cacheEntry);

		if (shardInterval != NULL)
		{
			prunedShardList = list_make1(shardInterval);
		}
	}
	else
	{
		List *restrictClauseList = NIL;
		Index tableId = 1;
		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);
		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;

		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValueConst->constvalue;
		rightConst->constisnull = partitionValueConst->constisnull;
		rightConst->constbyval = partitionValueConst->constbyval;

		restrictClauseList = list_make1(equalityExpr);

		prunedShardList = PruneShards(distributedTableId, tableId, restrictClauseList);
	}

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount != 1)
	{
		char *partitionKeyString = cacheEntry->partitionKeyString;
		char *partitionColumnName = ColumnNameToColumn(distributedTableId,
													   partitionKeyString);
		StringInfo errorMessage = makeStringInfo();
		StringInfo errorHint = makeStringInfo();
		const char *targetCountType = NULL;

		if (prunedShardCount == 0)
		{
			targetCountType = "no";
		}
		else
		{
			targetCountType = "multiple";
		}

		if (prunedShardCount == 0)
		{
			appendStringInfo(errorHint, "Make sure you have created a shard which "
										"can receive this partition column value.");
		}
		else
		{
			appendStringInfo(errorHint, "Make sure the value for partition column "
										"\"%s\" falls into a single shard.",
							 partitionColumnName);
		}

		appendStringInfo(errorMessage, "cannot run INSERT command which targets %s "
									   "shards", targetCountType);

		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL,
										 errorHint->data);

		return NULL;
	}

	return (ShardInterval *) linitial(prunedShardList);
}


/*
 * FindShardForUpdateOrDelete finds the shard interval in which an UPDATE or
 * DELETE command should be applied, or sets planningError when the query
 * needs to be applied to multiple or no shards.
 */
static ShardInterval *
FindShardForUpdateOrDelete(Query *query, DistTableCacheEntry *cacheEntry,
						   DeferredErrorMessage **planningError)
{
	Oid distributedTableId = cacheEntry->relationId;
	char partitionMethod = cacheEntry->partitionMethod;
	CmdType commandType = query->commandType;
	List *restrictClauseList = NIL;
	Index tableId = 1;
	List *prunedShardList = NIL;
	int prunedShardCount = 0;

	Assert(commandType == CMD_UPDATE || commandType == CMD_DELETE);

	restrictClauseList = QueryRestrictList(query, partitionMethod);
	prunedShardList = PruneShards(distributedTableId, tableId, restrictClauseList);

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount > 1)
	{
		char *partitionKeyString = cacheEntry->partitionKeyString;
		char *partitionColumnName = ColumnNameToColumn(distributedTableId,
													   partitionKeyString);
		StringInfo errorMessage = makeStringInfo();
		StringInfo errorHint = makeStringInfo();
		const char *commandName = NULL;

		if (commandType == CMD_UPDATE)
		{
			commandName = "UPDATE";
		}
		else
		{
			commandName = "DELETE";
		}

		appendStringInfo(errorHint, "Consider using an equality filter on "
									"partition column \"%s\" to target a "
									"single shard. If you'd like to run a "
									"multi-shard operation, use "
									"master_modify_multiple_shards().",
						 partitionColumnName);

		if (commandType == CMD_DELETE && partitionMethod == DISTRIBUTE_BY_APPEND)
		{
			appendStringInfo(errorHint, " You can also use "
										"master_apply_delete_command() to drop "
										"all shards satisfying delete criteria.");
		}

		appendStringInfo(errorMessage,
						 "cannot run %s command which targets multiple shards",
						 commandName);

		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL,
										 errorHint->data);

		return NULL;
	}
	else if (prunedShardCount == 0)
	{
		return NULL;
	}
	else
	{
		return (ShardInterval *) linitial(prunedShardList);
	}
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 *
 * Since reference tables do not have partition columns, the function returns
 * NIL for reference tables.
 */
static List *
QueryRestrictList(Query *query, char partitionMethod)
{
	List *queryRestrictList = NIL;

	/*
	 * Reference tables do not have the notion of partition column. Thus,
	 * there are no restrictions on the partition column.
	 */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		return queryRestrictList;
	}

	queryRestrictList = WhereClauseList(query->jointree);

	return queryRestrictList;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsDistributedTable(rangeTableEntry->relid))
		{
			distributedTableId = rangeTableEntry->relid;
			break;
		}
	}

	return distributedTableId;
}


/*
 * ExtractPartitionValue extracts the partition column value from a the target
 * of an INSERT command. If a partition value is missing altogether or is
 * NULL, this function throws an error.
 */
static Expr *
ExtractInsertPartitionValue(Query *query, Var *partitionColumn)
{
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot perform an INSERT without a partition column "
							   "value")));
	}

	return targetEntry->expr;
}


/* RouterSelectJob builds a Job to represent a single shard select query */
static Job *
RouterSelectJob(Query *originalQuery, RelationRestrictionContext *restrictionContext,
				DeferredErrorMessage **planningError)
{
	Job *job = NULL;
	Task *task = NULL;
	bool shardsPresent = false;
	StringInfo queryString = makeStringInfo();
	uint64 shardId = INVALID_SHARD_ID;
	List *placementList = NIL;
	List *relationShardList = NIL;
	bool replacePrunedQueryWithDummy = false;
	bool requiresMasterEvaluation = false;
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasNullifiedModification = false;

	/* router planner should create task even if it deosn't hit a shard at all */
	replacePrunedQueryWithDummy = true;

	/* check if this query requires master evaluation */
	requiresMasterEvaluation = RequiresMasterEvaluation(originalQuery);

	shardsPresent = RouterSelectQuery(originalQuery, restrictionContext,
									  &placementList, &shardId, &relationShardList,
									  replacePrunedQueryWithDummy, planningError);
	if (*planningError)
	{
		return NULL;
	}

	job = CreateJob(originalQuery);

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) originalQuery, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind == RTE_SUBQUERY &&
			!bms_is_empty(rangeTableEntry->updatedCols))
		{
			hasNullifiedModification = true;
			break;
		}
	}

	/* If there are no shards for update, create an empty task list for modifications */
	if (hasNullifiedModification)
	{
		job->taskList = NIL;
		return job;
	}

	pg_get_query_def(originalQuery, queryString);

	if (originalQuery->commandType == CMD_UPDATE)
	{
		ListCell *relationShardCell = NULL;
		char replicationModel = REPLICATION_MODEL_INVALID;

		foreach(relationShardCell, relationShardList)
		{
			RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
			DistTableCacheEntry *cacheEntry =
				DistributedTableCacheEntry(relationShard->relationId);

			if (replicationModel == REPLICATION_MODEL_INVALID)
			{
				replicationModel = cacheEntry->replicationModel;
			}
			else if (replicationModel != cacheEntry->replicationModel)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot perform an UPDATE on tables with "
									   "different replication models")));
			}
		}

		task = CreateTask(MODIFY_TASK);
		task->replicationModel = replicationModel;
	}
	else
	{
		task = CreateTask(ROUTER_TASK);
	}

	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->taskPlacementList = placementList;
	task->relationShardList = relationShardList;

	job->taskList = list_make1(task);
	job->requiresMasterEvaluation = requiresMasterEvaluation;

	return job;
}


/*
 * RouterSelectQuery returns true if there are shards present after running
 * pruning logic.
 *
 * On return true, all RTEs have been updated to point to the relevant shards in
 * the originalQuery. Also, placementList is filled with the list of worker nodes
 * that has all the required shard placements for the query execution.
 * anchorShardId is set to the first pruned shardId of the given query. Finally,
 * relationShardList is filled with the list of relation-to-shard mappings for
 * the query.
 */
bool
RouterSelectQuery(Query *originalQuery, RelationRestrictionContext *restrictionContext,
				  List **placementList, uint64 *anchorShardId, List **relationShardList,
				  bool replacePrunedQueryWithDummy, DeferredErrorMessage **planningError)
{
	List *prunedRelationShardList = TargetShardIntervalsForRouter(originalQuery,
																  restrictionContext);
	uint64 shardId = INVALID_SHARD_ID;
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = originalQuery->commandType;
	ListCell *prunedRelationShardListCell = NULL;
	List *workerList = NIL;
	bool shardsPresent = false;

	*placementList = NIL;

	/*
	 * If prunedRelationShardList is NULL then it means a relation has more
	 * than one shard left after pruning.
	 */
	if (prunedRelationShardList == NULL)
	{
		StringInfo errorMessage = makeStringInfo();
		StringInfo errorHint = makeStringInfo();
		const char *commandName = NULL;

		if (commandType == CMD_UPDATE)
		{
			commandName = "UPDATE";
		}
		else
		{
			commandName = "DELETE";
		}

		appendStringInfo(errorHint, "Consider using an equality filter on "
									"partition column to target a single shard. "
									"If you'd like to run a multi-shard operation, "
									"use master_modify_multiple_shards().");

		appendStringInfo(errorMessage,
						 "cannot run %s command which targets multiple shards",
						 commandName);

		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL,
										 errorHint->data);
		return false;
	}

	Assert(commandType == CMD_SELECT || commandType == CMD_UPDATE);

	foreach(prunedRelationShardListCell, prunedRelationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);

		ShardInterval *shardInterval = NULL;
		RelationShard *relationShard = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardList == NIL)
		{
			continue;
		}

		shardsPresent = true;

		/* all relations are now pruned down to 0 or 1 shards */
		Assert(list_length(prunedShardList) <= 1);

		shardInterval = (ShardInterval *) linitial(prunedShardList);

		/* anchor shard id */
		if (shardId == INVALID_SHARD_ID)
		{
			shardId = shardInterval->shardId;
		}

		/* add relation to shard mapping */
		relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = shardInterval->relationId;
		relationShard->shardId = shardInterval->shardId;

		*relationShardList = lappend(*relationShardList, relationShard);
	}

	/*
	 * We bail out if there are RTEs that prune multiple shards above, but
	 * there can also be multiple RTEs that reference the same relation.
	 */
	if (RelationPrunesToMultipleShards(*relationShardList))
	{
		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 "Relation Prunes To Multiple Shards", NULL,
										 NULL);
		return false;
	}

	/*
	 * Determine the worker that has all shard placements if a shard placement found.
	 * If no shard placement exists and replacePrunedQueryWithDummy flag is set, we will
	 * still run the query but the result will be empty. We create a dummy shard
	 * placement for the first active worker.
	 */
	if (shardsPresent)
	{
		workerList = WorkersContainingAllShards(prunedRelationShardList);
	}
	else if (replacePrunedQueryWithDummy)
	{
		List *workerNodeList = ActiveWorkerNodeList();
		if (workerNodeList != NIL)
		{
			WorkerNode *workerNode = (WorkerNode *) linitial(workerNodeList);
			ShardPlacement *dummyPlacement =
				(ShardPlacement *) CitusMakeNode(ShardPlacement);
			dummyPlacement->nodeName = workerNode->workerName;
			dummyPlacement->nodePort = workerNode->workerPort;

			workerList = lappend(workerList, dummyPlacement);
		}
	}
	else
	{
		/*
		 * For INSERT ... SELECT, this query could be still a valid for some other target
		 * shard intervals. Thus, we should return empty list if there aren't any matching
		 * workers, so that the caller can decide what to do with this task.
		 */
		workerList = NIL;

		return true;
	}

	if (workerList == NIL)
	{
		ereport(DEBUG2, (errmsg("Found no worker with all shard placements")));
		(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 "Found no worker with all shard placements",
										 NULL, NULL);
		return false;
	}

	if (!(RequiresMasterEvaluation(originalQuery) && commandType == CMD_UPDATE))
	{
		UpdateRelationToShardNames((Node *) originalQuery, *relationShardList);
	}

	*placementList = workerList;
	*anchorShardId = shardId;

	return shardsPresent;
}


/*
 * TargetShardIntervalsForRouter performs shard pruning for all referenced relations
 * in the query and returns list of shards per relation. Shard pruning is done based
 * on provided restriction context per relation. The function bails out and returns NULL
 * if any of the relations pruned down to more than one active shard. It also records
 * pruned shard intervals in relation restriction context to be used later on. Some
 * queries may have contradiction clauses like 'and false' or 'and 1=0', such queries
 * are treated as if all of the shards of joining relations are pruned out.
 */
static List *
TargetShardIntervalsForRouter(Query *query,
							  RelationRestrictionContext *restrictionContext)
{
	List *prunedRelationShardList = NIL;
	ListCell *restrictionCell = NULL;

	Assert(restrictionContext != NULL);

	foreach(restrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		Index tableId = relationRestriction->index;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
		int shardCount = cacheEntry->shardIntervalArrayLength;
		List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
		List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
		List *prunedShardList = NIL;
		List *joinInfoList = relationRestriction->relOptInfo->joininfo;
		List *pseudoRestrictionList = extract_actual_clauses(joinInfoList, true);
		bool whereFalseQuery = false;

		relationRestriction->prunedShardIntervalList = NIL;

		/*
		 * Queries may have contradiction clauses like 'false', or '1=0' in
		 * their filters. Such queries would have pseudo constant 'false'
		 * inside relOptInfo->joininfo list. We treat such cases as if all
		 * shards of the table are pruned out.
		 */
		whereFalseQuery = ContainsFalseClause(pseudoRestrictionList);
		if (!whereFalseQuery && shardCount > 0)
		{
			prunedShardList = PruneShards(relationId, tableId, restrictClauseList);

			/*
			 * Quick bail out. The query can not be router plannable if one
			 * relation has more than one shard left after pruning. Having no
			 * shard left is okay at this point. It will be handled at a later
			 * stage.
			 */
			if (list_length(prunedShardList) > 1)
			{
				return NULL;
			}
		}

		relationRestriction->prunedShardIntervalList = prunedShardList;
		prunedRelationShardList = lappend(prunedRelationShardList, prunedShardList);
	}

	return prunedRelationShardList;
}


/*
 * RelationPrunesToMultipleShards returns true if the given list of
 * relation-to-shard mappings contains at least two mappings with
 * the same relation, but different shards.
 */
static bool
RelationPrunesToMultipleShards(List *relationShardList)
{
	ListCell *relationShardCell = NULL;
	RelationShard *previousRelationShard = NULL;

	relationShardList = SortList(relationShardList, CompareRelationShards);

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);

		if (previousRelationShard != NULL &&
			relationShard->relationId == previousRelationShard->relationId &&
			relationShard->shardId != previousRelationShard->shardId)
		{
			return true;
		}

		previousRelationShard = relationShard;
	}

	return false;
}


/*
 * WorkersContainingAllShards returns list of shard placements that contain all
 * shard intervals provided to the function. It returns NIL if no placement exists.
 * The caller should check if there are any shard intervals exist for placement
 * check prior to calling this function.
 */
static List *
WorkersContainingAllShards(List *prunedShardIntervalsList)
{
	ListCell *prunedShardIntervalCell = NULL;
	bool firstShard = true;
	List *currentPlacementList = NIL;

	foreach(prunedShardIntervalCell, prunedShardIntervalsList)
	{
		List *shardIntervalList = (List *) lfirst(prunedShardIntervalCell);
		ShardInterval *shardInterval = NULL;
		uint64 shardId = INVALID_SHARD_ID;
		List *newPlacementList = NIL;

		if (shardIntervalList == NIL)
		{
			continue;
		}

		Assert(list_length(shardIntervalList) == 1);

		shardInterval = (ShardInterval *) linitial(shardIntervalList);
		shardId = shardInterval->shardId;

		/* retrieve all active shard placements for this shard */
		newPlacementList = FinalizedShardPlacementList(shardId);

		if (firstShard)
		{
			firstShard = false;
			currentPlacementList = newPlacementList;
		}
		else
		{
			/* keep placements that still exists for this shard */
			currentPlacementList = IntersectPlacementList(currentPlacementList,
														  newPlacementList);
		}

		/*
		 * Bail out if placement list becomes empty. This means there is no worker
		 * containing all shards referecend by the query, hence we can not forward
		 * this query directly to any worker.
		 */
		if (currentPlacementList == NIL)
		{
			break;
		}
	}

	return currentPlacementList;
}


/*
 * IntersectPlacementList performs placement pruning based on matching on
 * nodeName:nodePort fields of shard placement data. We start pruning from all
 * placements of the first relation's shard. Then for each relation's shard, we
 * compute intersection of the new shards placement with existing placement list.
 * This operation could have been done using other methods, but since we do not
 * expect very high replication factor, iterating over a list and making string
 * comparisons should be sufficient.
 */
List *
IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList)
{
	ListCell *lhsPlacementCell = NULL;
	List *placementList = NIL;

	/* Keep existing placement in the list if it is also present in new placement list */
	foreach(lhsPlacementCell, lhsPlacementList)
	{
		ShardPlacement *lhsPlacement = (ShardPlacement *) lfirst(lhsPlacementCell);
		ListCell *rhsPlacementCell = NULL;
		foreach(rhsPlacementCell, rhsPlacementList)
		{
			ShardPlacement *rhsPlacement = (ShardPlacement *) lfirst(rhsPlacementCell);
			if (rhsPlacement->nodePort == lhsPlacement->nodePort &&
				strncmp(rhsPlacement->nodeName, lhsPlacement->nodeName,
						WORKER_LENGTH) == 0)
			{
				placementList = lappend(placementList, rhsPlacement);
			}
		}
	}

	return placementList;
}


/*
 * MultiRouterPlannableQuery returns true if given query can be router plannable.
 * The query is router plannable if it is a modify query, or if its is a select
 * query issued on a hash partitioned distributed table, and it has a filter
 * to reduce number of shard pairs to one, and all shard pairs are located on
 * the same node. Router plannable checks for select queries can be turned off
 * by setting citus.enable_router_execution flag to false.
 */
static bool
MultiRouterPlannableQuery(Query *query, RelationRestrictionContext *restrictionContext)
{
	CmdType commandType = query->commandType;
	ListCell *relationRestrictionContextCell = NULL;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	Assert(commandType == CMD_SELECT);

	if (!EnableRouterExecution)
	{
		return false;
	}

	if (query->hasForUpdate)
	{
		return false;
	}

	foreach(relationRestrictionContextCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionContextCell);
		RangeTblEntry *rte = relationRestriction->rte;
		if (rte->rtekind == RTE_RELATION)
		{
			/* only hash partitioned tables are supported */
			Oid distributedTableId = rte->relid;
			char partitionMethod = PartitionMethod(distributedTableId);

			if (!(partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
				  DISTRIBUTE_BY_NONE || partitionMethod == DISTRIBUTE_BY_RANGE))
			{
				return false;
			}
		}
	}

	return true;
}


/*
 * Copy a RelationRestrictionContext. Note that several subfields are copied
 * shallowly, for lack of copyObject support.
 *
 * Note that CopyRelationRestrictionContext copies the following fields per relation
 * context: index, relationId, distributedRelation, rte, relOptInfo->baserestrictinfo
 * and relOptInfo->joininfo. Also, the function shallowly copies plannerInfo and
 * prunedShardIntervalList which are read-only. All other parts of the relOptInfo
 * is also shallowly copied.
 */
RelationRestrictionContext *
CopyRelationRestrictionContext(RelationRestrictionContext *oldContext)
{
	RelationRestrictionContext *newContext = (RelationRestrictionContext *)
											 palloc(sizeof(RelationRestrictionContext));
	ListCell *relationRestrictionCell = NULL;

	newContext->hasDistributedRelation = oldContext->hasDistributedRelation;
	newContext->hasLocalRelation = oldContext->hasLocalRelation;
	newContext->allReferenceTables = oldContext->allReferenceTables;
	newContext->relationRestrictionList = NIL;

	foreach(relationRestrictionCell, oldContext->relationRestrictionList)
	{
		RelationRestriction *oldRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		RelationRestriction *newRestriction = (RelationRestriction *)
											  palloc0(sizeof(RelationRestriction));

		newRestriction->index = oldRestriction->index;
		newRestriction->relationId = oldRestriction->relationId;
		newRestriction->distributedRelation = oldRestriction->distributedRelation;
		newRestriction->rte = copyObject(oldRestriction->rte);

		/* can't be copied, we copy (flatly) a RelOptInfo, and then decouple baserestrictinfo */
		newRestriction->relOptInfo = palloc(sizeof(RelOptInfo));
		memcpy(newRestriction->relOptInfo, oldRestriction->relOptInfo,
			   sizeof(RelOptInfo));

		newRestriction->relOptInfo->baserestrictinfo =
			copyObject(oldRestriction->relOptInfo->baserestrictinfo);

		newRestriction->relOptInfo->joininfo =
			copyObject(oldRestriction->relOptInfo->joininfo);

		/* not copyable, but readonly */
		newRestriction->plannerInfo = oldRestriction->plannerInfo;
		newRestriction->prunedShardIntervalList = oldRestriction->prunedShardIntervalList;

		newContext->relationRestrictionList =
			lappend(newContext->relationRestrictionList, newRestriction);
	}

	return newContext;
}


/*
 * ErrorIfQueryHasModifyingCTE checks if the query contains modifying common table
 * expressions and errors out if it does.
 */
static DeferredErrorMessage *
ErrorIfQueryHasModifyingCTE(Query *queryTree)
{
	ListCell *cteCell = NULL;

	Assert(queryTree->commandType == CMD_SELECT);

	foreach(cteCell, queryTree->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
		Query *cteQuery = (Query *) cte->ctequery;

		/*
		 * Here we only check for command type of top level query. Normally there can be
		 * nested CTE, however PostgreSQL dictates that data-modifying statements must
		 * be at top level of CTE. Therefore it is OK to just check for top level.
		 * Similarly, we do not need to check for subqueries.
		 */
		if (cteQuery->commandType != CMD_SELECT)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "data-modifying statements are not supported in "
								 "the WITH clauses of distributed queries",
								 NULL, NULL);
		}
	}

	/* everything OK */
	return NULL;
}


#if (PG_VERSION_NUM >= 100000)

/*
 * get_all_actual_clauses
 *
 * Returns a list containing the bare clauses from 'restrictinfo_list'.
 *
 * This loses the distinction between regular and pseudoconstant clauses,
 * so be careful what you use it for.
 */
static List *
get_all_actual_clauses(List *restrictinfo_list)
{
	List *result = NIL;
	ListCell *l;

	foreach(l, restrictinfo_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		Assert(IsA(rinfo, RestrictInfo));

		result = lappend(result, rinfo->clause);
	}
	return result;
}


#endif
