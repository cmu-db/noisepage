#include "planner/plannodes/abstract_plan_node.h"
#include <memory>
#include <vector>
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
#include "planner/plannodes/hash_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/result_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/set_op_plan_node.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::planner {

nlohmann::json AbstractPlanNode::ToJson() const {
  nlohmann::json j;
  j["plan_node_type"] = GetPlanNodeType();
  j["children"] = children_;
  j["output_schema"] = output_schema_;

  return j;
}

void AbstractPlanNode::FromJson(const nlohmann::json &j) {
  TERRIER_ASSERT(GetPlanNodeType() == j.at("plan_node_type").get<PlanNodeType>(), "Mismatching plan node types");
  // Deserialize output schema
  if (!j.at("output_schema").is_null()) {
    output_schema_ = std::make_shared<OutputSchema>();
    output_schema_->FromJson(j.at("output_schema"));
  }

  // Deserialize children
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  for (const auto &child_json : children_json) {
    children_.push_back(DeserializePlanNode(child_json));
  }
}

std::shared_ptr<AbstractPlanNode> DeserializePlanNode(const nlohmann::json &json) {
  std::shared_ptr<AbstractPlanNode> plan_node;

  auto plan_type = json.at("plan_node_type").get<PlanNodeType>();
  switch (plan_type) {
    case PlanNodeType::AGGREGATE: {
      plan_node = std::make_shared<AggregatePlanNode>();
      break;
    }

    case PlanNodeType::ANALYZE: {
      plan_node = std::make_shared<AnalyzePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_DATABASE: {
      plan_node = std::make_shared<CreateDatabasePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_FUNC: {
      plan_node = std::make_shared<CreateFunctionPlanNode>();
      break;
    }

    case PlanNodeType::CREATE_INDEX: {
      plan_node = std::make_shared<CreateIndexPlanNode>();
      break;
    }

    case PlanNodeType::CREATE_NAMESPACE: {
      plan_node = std::make_shared<CreateNamespacePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_TABLE: {
      plan_node = std::make_shared<CreateTablePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_TRIGGER: {
      plan_node = std::make_shared<CreateTriggerPlanNode>();
      break;
    }

    case PlanNodeType::CREATE_VIEW: {
      plan_node = std::make_shared<CreateViewPlanNode>();
      break;
    }

    case PlanNodeType::CSVSCAN: {
      plan_node = std::make_shared<CSVScanPlanNode>();
      break;
    }

    case PlanNodeType::DELETE: {
      plan_node = std::make_shared<DeletePlanNode>();
      break;
    }

    case PlanNodeType::DROP_DATABASE: {
      plan_node = std::make_shared<DropDatabasePlanNode>();
      break;
    }

    case PlanNodeType::DROP_INDEX: {
      plan_node = std::make_shared<DropIndexPlanNode>();
      break;
    }

    case PlanNodeType::DROP_NAMESPACE: {
      plan_node = std::make_shared<DropNamespacePlanNode>();
      break;
    }

    case PlanNodeType::DROP_TABLE: {
      plan_node = std::make_shared<DropTablePlanNode>();
      break;
    }

    case PlanNodeType::DROP_TRIGGER: {
      plan_node = std::make_shared<DropTriggerPlanNode>();
      break;
    }

    case PlanNodeType::DROP_VIEW: {
      plan_node = std::make_shared<DropViewPlanNode>();
      break;
    }
    case PlanNodeType::EXPORT_EXTERNAL_FILE: {
      plan_node = std::make_shared<ExportExternalFilePlanNode>();
      break;
    }

    case PlanNodeType::HASHJOIN: {
      plan_node = std::make_shared<HashJoinPlanNode>();
      break;
    }

    case PlanNodeType::HASH: {
      plan_node = std::make_shared<HashPlanNode>();
      break;
    }

    case PlanNodeType::INDEXSCAN: {
      plan_node = std::make_shared<IndexScanPlanNode>();
      break;
    }

    case PlanNodeType::INSERT: {
      plan_node = std::make_shared<InsertPlanNode>();
      break;
    }

    case PlanNodeType::LIMIT: {
      plan_node = std::make_shared<LimitPlanNode>();
      break;
    }

    case PlanNodeType::NESTLOOP: {
      plan_node = std::make_shared<NestedLoopJoinPlanNode>();
      break;
    }

    case PlanNodeType::ORDERBY: {
      plan_node = std::make_shared<OrderByPlanNode>();
      break;
    }

    case PlanNodeType::PROJECTION: {
      plan_node = std::make_shared<ProjectionPlanNode>();
      break;
    }

    case PlanNodeType::RESULT: {
      plan_node = std::make_shared<ResultPlanNode>();
      break;
    }

    case PlanNodeType::SEQSCAN: {
      plan_node = std::make_shared<SeqScanPlanNode>();
      break;
    }

    case PlanNodeType::SETOP: {
      plan_node = std::make_shared<SetOpPlanNode>();
      break;
    }

    case PlanNodeType::UPDATE: {
      plan_node = std::make_shared<UpdatePlanNode>();
      break;
    }

    default:
      throw std::runtime_error("Unknown plan node type during deserialization");
  }

  plan_node->FromJson(json);
  return plan_node;
}

}  // namespace terrier::planner
