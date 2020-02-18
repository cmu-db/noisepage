#include <utility>

#include "brain/operating_unit.h"
#include "brain/operating_unit_recorder.h"
#include "parser/expression_defs.h"
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
#include "planner/plannodes/plan_visitor.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "type/type_id.h"

namespace terrier::brain {

type::TypeId OperatingUnitRecorder::DeriveComputation(common::ManagedPointer<parser::AbstractExpression> expr) {
  auto lchild = expr->GetChild(0);
  if (lchild->GetReturnValueType() != type::TypeId::INVALID &&
      lchild->GetReturnValueType() != type::TypeId::PARAMETER_OFFSET) {
    return lchild->GetReturnValueType();
  }

  if (expr->GetChildrenSize() > 1) {
    auto rchild = expr->GetChild(1);
    if (rchild->GetReturnValueType() != type::TypeId::INVALID &&
        rchild->GetReturnValueType() != type::TypeId::PARAMETER_OFFSET) {
      return rchild->GetReturnValueType();
    }
  }

  return type::TypeId::INVALID;
}

ExecutionOperatingUnitType OperatingUnitRecorder::ConvertExpressionType(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  switch (expr->GetExpressionType()) {
    case parser::ExpressionType::OPERATOR_PLUS:
    case parser::ExpressionType::OPERATOR_MINUS: {
      switch (DeriveComputation(expr)) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT:
          return ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS;
        case type::TypeId::DECIMAL:
          return ExecutionOperatingUnitType::OP_DECIMAL_PLUS_OR_MINUS;
        default:
          return ExecutionOperatingUnitType::INVALID;
      }
    }
    case parser::ExpressionType::OPERATOR_MULTIPLY: {
      switch (DeriveComputation(expr)) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT:
          return ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY;
        case type::TypeId::DECIMAL:
          return ExecutionOperatingUnitType::OP_DECIMAL_MULTIPLY;
        default:
          return ExecutionOperatingUnitType::INVALID;
      }
    }
    case parser::ExpressionType::OPERATOR_DIVIDE: {
      switch (DeriveComputation(expr)) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT:
          return ExecutionOperatingUnitType::OP_INTEGER_DIVIDE;
        case type::TypeId::DECIMAL:
          return ExecutionOperatingUnitType::OP_DECIMAL_DIVIDE;
        default:
          return ExecutionOperatingUnitType::INVALID;
      }
    }
    case parser::ExpressionType::COMPARE_EQUAL:
    case parser::ExpressionType::COMPARE_NOT_EQUAL:
    case parser::ExpressionType::COMPARE_LESS_THAN:
    case parser::ExpressionType::COMPARE_GREATER_THAN:
    case parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
    case parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO: {
      switch (DeriveComputation(expr)) {
        case type::TypeId::BOOLEAN:
          return ExecutionOperatingUnitType::OP_BOOL_COMPARE;
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT:
          return ExecutionOperatingUnitType::OP_INTEGER_COMPARE;
        case type::TypeId::DECIMAL:
          return ExecutionOperatingUnitType::OP_DECIMAL_COMPARE;
        case type::TypeId::TIMESTAMP:
        case type::TypeId::DATE:
          return ExecutionOperatingUnitType::OP_INTEGER_COMPARE;
        case type::TypeId::VARCHAR:
        case type::TypeId::VARBINARY:
          // TODO(wz2): Revisit this since VARCHAR/VARBINARY is more than just integer
          return ExecutionOperatingUnitType::OP_INTEGER_COMPARE;
        default:
          return ExecutionOperatingUnitType::INVALID;
      }
    }
    case parser::ExpressionType::AGGREGATE_COUNT:
      return ExecutionOperatingUnitType::OP_AGGREGATE_COUNT;
    case parser::ExpressionType::AGGREGATE_SUM:
      return ExecutionOperatingUnitType::OP_AGGREGATE_SUM;
    case parser::ExpressionType::AGGREGATE_MIN:
      return ExecutionOperatingUnitType::OP_AGGREGATE_MIN;
    case parser::ExpressionType::AGGREGATE_MAX:
      return ExecutionOperatingUnitType::OP_AGGREGATE_MAX;
    case parser::ExpressionType::AGGREGATE_AVG:
      return ExecutionOperatingUnitType::OP_AGGREGATE_AVG;
    default:
      return ExecutionOperatingUnitType::INVALID;
  }
}

std::vector<ExecutionOperatingUnitType> OperatingUnitRecorder::ExtractFeaturesFromExpression(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  if (expr == nullptr) return std::vector<ExecutionOperatingUnitType>();

  std::vector<ExecutionOperatingUnitType> feature_types;
  std::queue<common::ManagedPointer<parser::AbstractExpression>> work;
  work.push(expr);

  while (!work.empty()) {
    auto head = work.front();
    work.pop();

    auto feature = ConvertExpressionType(head);
    if (feature != ExecutionOperatingUnitType::INVALID) {
      feature_types.push_back(feature);
    }

    for (auto child : head->GetChildren()) {
      work.push(child);
    }
  }

  return feature_types;
}

void OperatingUnitRecorder::VisitAbstractPlanNode(const planner::AbstractPlanNode *plan) {
  auto schema = plan->GetOutputSchema();
  if (schema != nullptr) {
    for (auto &column : schema->GetColumns()) {
      auto features = ExtractFeaturesFromExpression(column.GetExpr());
      plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                            std::make_move_iterator(features.end()));
    }
  }
}

void OperatingUnitRecorder::VisitAbstractScanPlanNode(const planner::AbstractScanPlanNode *plan) {
  VisitAbstractPlanNode(plan);

  auto features = ExtractFeaturesFromExpression(plan->GetScanPredicate());
  plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                        std::make_move_iterator(features.end()));
}

void OperatingUnitRecorder::VisitAbstractJoinPlanNode(const planner::AbstractJoinPlanNode *plan) {
  VisitAbstractPlanNode(plan);

  auto features = ExtractFeaturesFromExpression(plan->GetJoinPredicate());
  plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                        std::make_move_iterator(features.end()));
}

void OperatingUnitRecorder::Visit(const planner::InsertPlanNode *plan) {
  VisitAbstractPlanNode(plan);

  for (size_t idx = 0; idx < plan->GetBulkInsertCount(); idx++) {
    for (auto &col : plan->GetValues(idx)) {
      auto features = ExtractFeaturesFromExpression(col);
      plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                            std::make_move_iterator(features.end()));
    }
  }
}

void OperatingUnitRecorder::Visit(const planner::UpdatePlanNode *plan) {
  VisitAbstractPlanNode(plan);

  for (auto &clause : plan->GetSetClauses()) {
    auto features = ExtractFeaturesFromExpression(clause.second);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }
}

void OperatingUnitRecorder::Visit(const planner::DeletePlanNode *plan) { VisitAbstractPlanNode(plan); }

void OperatingUnitRecorder::Visit(const planner::CSVScanPlanNode *plan) { VisitAbstractScanPlanNode(plan); }

void OperatingUnitRecorder::Visit(const planner::SeqScanPlanNode *plan) { VisitAbstractScanPlanNode(plan); }

void OperatingUnitRecorder::Visit(const planner::IndexScanPlanNode *plan) {
  VisitAbstractScanPlanNode(plan);

  for (auto &pair : plan->GetLoIndexColumns()) {
    auto features = ExtractFeaturesFromExpression(pair.second);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }

  for (auto &pair : plan->GetHiIndexColumns()) {
    auto features = ExtractFeaturesFromExpression(pair.second);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }
}

void OperatingUnitRecorder::Visit(const planner::HashJoinPlanNode *plan) {
  VisitAbstractJoinPlanNode(plan);

  for (auto key : plan->GetLeftHashKeys()) {
    auto features = ExtractFeaturesFromExpression(key);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }

  for (auto key : plan->GetRightHashKeys()) {
    auto features = ExtractFeaturesFromExpression(key);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }
}

void OperatingUnitRecorder::Visit(const planner::NestedLoopJoinPlanNode *plan) {
  VisitAbstractJoinPlanNode(plan);

  for (auto key : plan->GetLeftKeys()) {
    auto features = ExtractFeaturesFromExpression(key);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }

  for (auto key : plan->GetRightKeys()) {
    auto features = ExtractFeaturesFromExpression(key);
    plan_features_.insert(plan_features_.end(), std::make_move_iterator(features.begin()),
                          std::make_move_iterator(features.end()));
  }
}

void OperatingUnitRecorder::Visit(const planner::LimitPlanNode *plan) { VisitAbstractPlanNode(plan); }

void OperatingUnitRecorder::Visit(const planner::OrderByPlanNode *plan) { VisitAbstractPlanNode(plan); }

void OperatingUnitRecorder::Visit(const planner::ProjectionPlanNode *plan) { VisitAbstractPlanNode(plan); }

void OperatingUnitRecorder::Visit(const planner::AggregatePlanNode *plan) { VisitAbstractPlanNode(plan); }

ExecutionOperatingUnitFeatureVector OperatingUnitRecorder::RecordTranslators(
    const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators) {
  // Note that OperatorTranslators are roughly 1:1 with a plan node
  // As such, just emplace directly into feature vector
  std::vector<ExecutionOperatingUnitFeature> results{};
  std::unordered_set<const planner::AbstractPlanNode *> plan_nodes;
  for (const auto &translator : translators) {
    // TODO(wz2): Populate actual num_rows/cardinality after #759
    auto feature_type = translator->GetFeatureType();
    auto num_rows = 0;
    auto cardinality = 0.0;
    if (feature_type != ExecutionOperatingUnitType::INVALID && feature_type != ExecutionOperatingUnitType::OUTPUT) {
      plan_nodes.insert(translator->Op());
      results.emplace_back(feature_type, num_rows, cardinality);
    }
  }

  std::unordered_map<ExecutionOperatingUnitType, ExecutionOperatingUnitFeature> features;
  for (auto *plan : plan_nodes) {
    // Consolidate the features based on OutputSchema
    plan_features_ = {};
    plan->Accept(common::ManagedPointer<planner::PlanVisitor>(this));

    for (auto feature : plan_features_) {
      // TODO(wz2): Get these from cost model/plan?
      size_t num_rows = 0;
      auto cardinality = 0.0;
      auto itr = features.find(feature);
      if (itr == features.end()) {
        features.emplace(feature, ExecutionOperatingUnitFeature(feature, num_rows, cardinality));
      } else {
        // Add the number of rows/cardinality to reflect total amount of work in pipeline
        itr->second.SetNumRows(num_rows + itr->second.GetNumRows());
        itr->second.SetCardinality(cardinality + itr->second.GetCardinality());
      }
    }
  }

  // Consolidate final features
  for (auto &feature : features) {
    results.emplace_back(feature.second);
  }

  return results;
}

}  // namespace terrier::brain
