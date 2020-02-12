#pragma once

#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::planner {

/**
 * Utility class for visitor pattern for plan nodes
 */
class PlanVisitor {
 public:
  virtual ~PlanVisitor() = default;

  /**
   * Visit an AggregatePlanNode
   * @param plan AggregatePlanNode
   */
  virtual void Visit(const AggregatePlanNode *plan) {}

  /**
   * Visit an AnalyzePlanNode
   * @param plan AnalyzePlanNode
   */
  virtual void Visit(const AnalyzePlanNode *plan) {}

  /**
   * Visit an CreateDatabasePlanNode
   * @param plan CreateDatabasePlanNode
   */
  virtual void Visit(const CreateDatabasePlanNode *plan) {}

  /**
   * Visit an CreateFunctionPlanNode
   * @param plan CreateFunctionPlanNode
   */
  virtual void Visit(const CreateFunctionPlanNode *plan) {}

  /**
   * Visit an CreateIndexPlanNode
   * @param plan CreateIndexPlanNode
   */
  virtual void Visit(const CreateIndexPlanNode *plan) {}

  /**
   * Visit an CreateNamespacePlanNode
   * @param plan CreateNamespacePlanNode
   */
  virtual void Visit(const CreateNamespacePlanNode *plan) {}

  /**
   * Visit an CreateTablePlanNode
   * @param plan CreateTablePlanNode
   */
  virtual void Visit(const CreateTablePlanNode *plan) {}

  /**
   * Visit an CreateTriggerPlanNode
   * @param plan CreateTriggerPlanNode
   */
  virtual void Visit(const CreateTriggerPlanNode *plan) {}

  /**
   * Visit an CreateViewPlanNode
   * @param plan CreateViewPlanNode
   */
  virtual void Visit(const CreateViewPlanNode *plan) {}

  /**
   * Visit an CSVScanPlanNode
   * @param plan CSVScanPlanNode
   */
  virtual void Visit(const CSVScanPlanNode *plan) {}

  /**
   * Visit an DeletePlanNode
   * @param plan DeletePlanNode
   */
  virtual void Visit(const DeletePlanNode *plan) {}

  /**
   * Visit an DropDatabasePlanNode
   * @param plan DropDatabasePlanNode
   */
  virtual void Visit(const DropDatabasePlanNode *plan) {}

  /**
   * Visit an DropIndexPlanNode
   * @param plan DropIndexPlanNode
   */
  virtual void Visit(const DropIndexPlanNode *plan) {}

  /**
   * Visit an DropNamespacePlanNode
   * @param plan DropNamespacePlanNode
   */
  virtual void Visit(const DropNamespacePlanNode *plan) {}

  /**
   * Visit an DropTablePlanNode
   * @param plan DropTablePlanNode
   */
  virtual void Visit(const DropTablePlanNode *plan) {}

  /**
   * Visit an DropTriggerPlanNode
   * @param plan DropTriggerPlanNode
   */
  virtual void Visit(const DropTriggerPlanNode *plan) {}

  /**
   * Visit an DropViewPlanNode
   * @param plan DropViewPlanNode
   */
  virtual void Visit(const DropViewPlanNode *plan) {}

  /**
   * Visit an ExportExternalFilePlanNode
   * @param plan ExportExternalFilePlanNode
   */
  virtual void Visit(const ExportExternalFilePlanNode *plan) {}

  /**
   * Visit an HashJoinPlanNode
   * @param plan HashJoinPlanNode
   */
  virtual void Visit(const HashJoinPlanNode *plan) {}

  /**
   * Visit an IndexJoinPlanNode
   * @param plan IndexJoinPlanNode
   */
  virtual void Visit(const IndexJoinPlanNode *plan) {}

  /**
   * Visit an IndexScanPlanNode
   * @param plan IndexScanPlanNode
   */
  virtual void Visit(const IndexScanPlanNode *plan) {}

  /**
   * Visit an InsertPlanNode
   * @param plan InsertPlanNode
   */
  virtual void Visit(const InsertPlanNode *plan) {}

  /**
   * Visit an LimitPlanNode
   * @param plan LimitPlanNode
   */
  virtual void Visit(const LimitPlanNode *plan) {}

  /**
   * Visit an NestedLoopJoinPlanNode
   * @param plan NestedLoopJoinPlanNode
   */
  virtual void Visit(const NestedLoopJoinPlanNode *plan) {}

  /**
   * Visit an OrderByPlanNode
   * @param plan OrderByPlanNode
   */
  virtual void Visit(const OrderByPlanNode *plan) {}

  /**
   * Visit an ProjectionPlanNode
   * @param plan ProjectionPlanNode
   */
  virtual void Visit(const ProjectionPlanNode *plan) {}

  /**
   * Visit an SeqScanPlanNode
   * @param plan SeqScanPlanNode
   */
  virtual void Visit(const SeqScanPlanNode *plan) {}

  /**
   * Visit an UpdatePlanNode
   * @param plan UpdatePlanNode
   */
  virtual void Visit(const UpdatePlanNode *plan) {}

};

}  // namespace terrier::planner
