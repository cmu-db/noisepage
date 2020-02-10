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

class PlanVisitor {
 public:
  virtual ~PlanVisitor() = default;

  virtual void Visit(const AggregatePlanNode *plan) {} 
  virtual void Visit(const AnalyzePlanNode *plan) {}
  virtual void Visit(const CreateDatabasePlanNode *plan) {}
  virtual void Visit(const CreateFunctionPlanNode *plan) {}
  virtual void Visit(const CreateIndexPlanNode *plan) {}
  virtual void Visit(const CreateNamespacePlanNode *plan) {}
  virtual void Visit(const CreateTablePlanNode *plan) {}
  virtual void Visit(const CreateTriggerPlanNode *plan) {}
  virtual void Visit(const CreateViewPlanNode *plan) {}
  virtual void Visit(const CSVScanPlanNode *plan) {}
  virtual void Visit(const DeletePlanNode *plan) {}
  virtual void Visit(const DropDatabasePlanNode *plan) {}
  virtual void Visit(const DropIndexPlanNode *plan) {}
  virtual void Visit(const DropNamespacePlanNode *plan) {}
  virtual void Visit(const DropTablePlanNode *plan) {}
  virtual void Visit(const DropTriggerPlanNode *plan) {}
  virtual void Visit(const DropViewPlanNode *plan) {}
  virtual void Visit(const ExportExternalFilePlanNode *plan) {}
  virtual void Visit(const HashJoinPlanNode *plan) {}
  virtual void Visit(const IndexJoinPlanNode *plan) {}
  virtual void Visit(const IndexScanPlanNode *plan) {}
  virtual void Visit(const InsertPlanNode *plan) {}
  virtual void Visit(const LimitPlanNode *plan) {}
  virtual void Visit(const NestedLoopJoinPlanNode *plan) {}
  virtual void Visit(const OrderByPlanNode *plan) {}
  virtual void Visit(const ProjectionPlanNode *plan) {}
  virtual void Visit(const SeqScanPlanNode *plan) {}
  virtual void Visit(const UpdatePlanNode *plan) {}

};

}  // namespace terrier::planner
