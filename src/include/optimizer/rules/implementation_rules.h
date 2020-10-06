#pragma once

#include <memory>
#include <vector>

#include "catalog/index_schema.h"
#include "optimizer/rule.h"

namespace terrier::optimizer {

/**
 * Rule transforms Logical Scan -> Sequential Scan
 */
class LogicalGetToPhysicalSeqScan : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalGetToPhysicalSeqScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalExternalFileGet -> ExternalFileGet
 */
class LogicalExternalFileGetToPhysicalExternalFileGet : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalExternalFileGetToPhysicalExternalFileGet();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Generate dummy scan for queries like "SELECT 1", there's no actual
 * table to generate
 */
class LogicalGetToPhysicalTableFreeScan : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalGetToPhysicalTableFreeScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Scan -> Index Scan
 */
class LogicalGetToPhysicalIndexScan : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalGetToPhysicalIndexScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;

  /**
   * Set whether to allow CVEs when checking whether an index can be used
   *
   * This is primarily intended for use by the IndexJoin where a predicate
   * of the form (col1 == col2) has a plausible use. If transforming a
   * normal table scan to an index scan, predicates of form (col1 compare col2)
   * are ignored in determining the high/low key bounds.
   *
   * @param allow Whether to allow CVEs
   */
  void SetAllowCVEs(bool allow) { allow_cves_ = allow; }

 private:
  bool allow_cves_ = false;
};

/**
 * Rule that transforms query derived scans for nested queries
 */
class LogicalQueryDerivedGetToPhysicalQueryDerivedScan : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalQueryDerivedGetToPhysicalQueryDerivedScan();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalDelete -> Delete
 */
class LogicalDeleteToPhysicalDelete : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDeleteToPhysicalDelete();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalUpdate -> Update
 */
class LogicalUpdateToPhysicalUpdate : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalUpdateToPhysicalUpdate();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalInsert -> Insert
 */
class LogicalInsertToPhysicalInsert : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInsertToPhysicalInsert();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalInsertSelect -> InsertSelect
 */
class LogicalInsertSelectToPhysicalInsertSelect : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInsertSelectToPhysicalInsertSelect();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalGroupBy -> HashGroupBy
 */
class LogicalGroupByToPhysicalHashGroupBy : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalGroupByToPhysicalHashGroupBy();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalAggregate -> Aggregate
 */
class LogicalAggregateToPhysicalAggregate : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalAggregateToPhysicalAggregate();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Inner Join to InnerIndexJoin
 */
class LogicalInnerJoinToPhysicalInnerIndexJoin : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInnerJoinToPhysicalInnerIndexJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input OperatorNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorNode to transform
   * @param transformed Vector of transformed OperatorNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Inner Join to InnerNLJoin
 */
class LogicalInnerJoinToPhysicalInnerNLJoin : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInnerJoinToPhysicalInnerNLJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Inner Join to InnerHashJoin
 */
class LogicalInnerJoinToPhysicalInnerHashJoin : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalInnerJoinToPhysicalInnerHashJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Left Join to LeftHashJoin
 */
class LogicalLeftJoinToPhysicalLeftHashJoin : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalLeftJoinToPhysicalLeftHashJoin();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms LogicalLimit -> Limit
 */
class LogicalLimitToPhysicalLimit : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalLimitToPhysicalLimit();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Export -> Physical Export
 */
class LogicalExportToPhysicalExport : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalExportToPhysicalExport();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateDatabase -> Physical CreateDatabase
 */
class LogicalCreateDatabaseToPhysicalCreateDatabase : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateDatabaseToPhysicalCreateDatabase();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateFunction -> Physical CreateFunction
 */
class LogicalCreateFunctionToPhysicalCreateFunction : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateFunctionToPhysicalCreateFunction();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateIndex -> Physical CreateIndex
 */
class LogicalCreateIndexToPhysicalCreateIndex : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateIndexToPhysicalCreateIndex();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateTable -> Physical CreateTable
 */
class LogicalCreateTableToPhysicalCreateTable : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateTableToPhysicalCreateTable();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateNamespace -> Physical CreateNamespace
 */
class LogicalCreateNamespaceToPhysicalCreateNamespace : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateNamespaceToPhysicalCreateNamespace();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateTrigger -> Physical CreateTrigger
 */
class LogicalCreateTriggerToPhysicalCreateTrigger : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateTriggerToPhysicalCreateTrigger();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical CreateView -> Physical CreateView
 */
class LogicalCreateViewToPhysicalCreateView : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalCreateViewToPhysicalCreateView();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical DropDatabase -> Physical DropDatabase
 */
class LogicalDropDatabaseToPhysicalDropDatabase : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDropDatabaseToPhysicalDropDatabase();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical DropTable -> Physical DropTable
 */
class LogicalDropTableToPhysicalDropTable : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDropTableToPhysicalDropTable();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical DropIndex -> Physical DropIndex
 */
class LogicalDropIndexToPhysicalDropIndex : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDropIndexToPhysicalDropIndex();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical DropNamespace -> Physical DropNamespace
 */
class LogicalDropNamespaceToPhysicalDropNamespace : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDropNamespaceToPhysicalDropNamespace();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical DropTrigger -> Physical DropTrigger
 */
class LogicalDropTriggerToPhysicalDropTrigger : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDropTriggerToPhysicalDropTrigger();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical DropView -> Physical DropView
 */
class LogicalDropViewToPhysicalDropView : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalDropViewToPhysicalDropView();

  /**
   * Checks whether the given rule can be applied
   * @param plan AbstractOptimizerNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input AbstractOptimizerNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input AbstractOptimizerNode to transform
   * @param transformed Vector of transformed AbstractOptimizerNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

/**
 * Rule transforms Logical Analyze -> Physical Analyze
 */
class LogicalAnalyzeToPhysicalAnalyze : public Rule {
 public:
  /**
   * Constructor
   */
  LogicalAnalyzeToPhysicalAnalyze();

  /**
   * Checks whether the given rule can be applied
   * @param plan OperatorNode to check
   * @param context Current OptimizationContext executing under
   * @returns Whether the input OperatorNode passes the check
   */
  bool Check(common::ManagedPointer<AbstractOptimizerNode> plan, OptimizationContext *context) const override;

  /**
   * Transforms the input expression using the given rule
   * @param input Input OperatorNode to transform
   * @param transformed Vector of transformed OperatorNodes
   * @param context Current OptimizationContext executing under
   */
  void Transform(common::ManagedPointer<AbstractOptimizerNode> input,
                 std::vector<std::unique_ptr<AbstractOptimizerNode>> *transformed,
                 OptimizationContext *context) const override;
};

}  // namespace terrier::optimizer
