#include "planner/plannodes/abstract_plan_node.h"

#include <memory>
#include <utility>
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
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/plan_visitor.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/result_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/set_op_plan_node.h"
#include "planner/plannodes/update_plan_node.h"

namespace terrier::planner {

nlohmann::json AbstractPlanNode::ToJson() const {
  nlohmann::json j;
  j["plan_node_type"] = GetPlanNodeType();
  std::vector<nlohmann::json> children;
  for (const auto &child : children_) {
    children.emplace_back(child->ToJson());
  }
  j["children"] = children;
  j["output_schema"] = output_schema_ == nullptr ? nlohmann::json(nullptr) : output_schema_->ToJson();
  return j;
}

std::vector<std::unique_ptr<parser::AbstractExpression>> AbstractPlanNode::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<parser::AbstractExpression>> exprs;
  TERRIER_ASSERT(GetPlanNodeType() == j.at("plan_node_type").get<PlanNodeType>(), "Mismatching plan node types");
  // Deserialize output schema
  if (!j.at("output_schema").is_null()) {
    output_schema_ = std::make_unique<OutputSchema>();
    auto e1 = output_schema_->FromJson(j.at("output_schema"));
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  }

  // Deserialize children
  auto children_json = j.at("children").get<std::vector<nlohmann::json>>();
  for (const auto &child_json : children_json) {
    auto deserialized = DeserializePlanNode(child_json);
    children_.emplace_back(std::move(deserialized.result_));
    exprs.insert(exprs.end(), std::make_move_iterator(deserialized.non_owned_exprs_.begin()),
                 std::make_move_iterator(deserialized.non_owned_exprs_.end()));
  }

  return exprs;
}

void AbstractPlanNode::MoveChildren(std::vector<std::unique_ptr<AbstractPlanNode>> *adoption_list) {
  for (auto &i : children_) {
    adoption_list->emplace_back(std::move(i));
  }
  children_.clear();
}

JSONDeserializeNodeIntermediate DeserializePlanNode(const nlohmann::json &json) {
  std::unique_ptr<AbstractPlanNode> plan_node;

  auto plan_type = json.at("plan_node_type").get<PlanNodeType>();
  switch (plan_type) {
    case PlanNodeType::AGGREGATE: {
      plan_node = std::make_unique<AggregatePlanNode>();
      break;
    }

    case PlanNodeType::ANALYZE: {
      plan_node = std::make_unique<AnalyzePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_DATABASE: {
      plan_node = std::make_unique<CreateDatabasePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_FUNC: {
      plan_node = std::make_unique<CreateFunctionPlanNode>();
      break;
    }

    case PlanNodeType::CREATE_INDEX: {
      plan_node = std::make_unique<CreateIndexPlanNode>();
      break;
    }

    case PlanNodeType::CREATE_NAMESPACE: {
      plan_node = std::make_unique<CreateNamespacePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_TABLE: {
      plan_node = std::make_unique<CreateTablePlanNode>();
      break;
    }

    case PlanNodeType::CREATE_TRIGGER: {
      plan_node = std::make_unique<CreateTriggerPlanNode>();
      break;
    }

    case PlanNodeType::CREATE_VIEW: {
      plan_node = std::make_unique<CreateViewPlanNode>();
      break;
    }

    case PlanNodeType::CSVSCAN: {
      plan_node = std::make_unique<CSVScanPlanNode>();
      break;
    }

    case PlanNodeType::DELETE: {
      plan_node = std::make_unique<DeletePlanNode>();
      break;
    }

    case PlanNodeType::DROP_DATABASE: {
      plan_node = std::make_unique<DropDatabasePlanNode>();
      break;
    }

    case PlanNodeType::DROP_INDEX: {
      plan_node = std::make_unique<DropIndexPlanNode>();
      break;
    }

    case PlanNodeType::DROP_NAMESPACE: {
      plan_node = std::make_unique<DropNamespacePlanNode>();
      break;
    }

    case PlanNodeType::DROP_TABLE: {
      plan_node = std::make_unique<DropTablePlanNode>();
      break;
    }

    case PlanNodeType::DROP_TRIGGER: {
      plan_node = std::make_unique<DropTriggerPlanNode>();
      break;
    }

    case PlanNodeType::DROP_VIEW: {
      plan_node = std::make_unique<DropViewPlanNode>();
      break;
    }
    case PlanNodeType::EXPORT_EXTERNAL_FILE: {
      plan_node = std::make_unique<ExportExternalFilePlanNode>();
      break;
    }

    case PlanNodeType::HASHJOIN: {
      plan_node = std::make_unique<HashJoinPlanNode>();
      break;
    }

    case PlanNodeType::INDEXSCAN: {
      plan_node = std::make_unique<IndexScanPlanNode>();
      break;
    }

    case PlanNodeType::INSERT: {
      plan_node = std::make_unique<InsertPlanNode>();
      break;
    }

    case PlanNodeType::LIMIT: {
      plan_node = std::make_unique<LimitPlanNode>();
      break;
    }

    case PlanNodeType::NESTLOOP: {
      plan_node = std::make_unique<NestedLoopJoinPlanNode>();
      break;
    }

    case PlanNodeType::ORDERBY: {
      plan_node = std::make_unique<OrderByPlanNode>();
      break;
    }

    case PlanNodeType::PROJECTION: {
      plan_node = std::make_unique<ProjectionPlanNode>();
      break;
    }

    case PlanNodeType::RESULT: {
      plan_node = std::make_unique<ResultPlanNode>();
      break;
    }

    case PlanNodeType::SEQSCAN: {
      plan_node = std::make_unique<SeqScanPlanNode>();
      break;
    }

    case PlanNodeType::SETOP: {
      plan_node = std::make_unique<SetOpPlanNode>();
      break;
    }

    case PlanNodeType::UPDATE: {
      plan_node = std::make_unique<UpdatePlanNode>();
      break;
    }

    default:
      throw std::runtime_error("Unknown plan node type during deserialization");
  }

  auto non_owned_exprs = plan_node->FromJson(json);
  return JSONDeserializeNodeIntermediate{std::move(plan_node), std::move(non_owned_exprs)};
}

}  // namespace terrier::planner
