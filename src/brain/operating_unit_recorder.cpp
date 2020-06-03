#include <utility>

#include "brain/operating_unit.h"
#include "brain/operating_unit_recorder.h"
#include "brain/operating_unit_util.h"
#include "execution/ast/ast.h"
#include "execution/ast/type.h"
#include "execution/compiler/operator/aggregate_translator.h"
#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/operator/sort_translator.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/hash_table_entry.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
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
#include "storage/block_layout.h"
#include "type/type_id.h"

namespace terrier::brain {

double OperatingUnitRecorder::ComputeMemoryScaleFactor(execution::ast::StructDecl *decl, size_t total_offset,
                                                       size_t key_size, size_t ref_offset) {
  auto *type = reinterpret_cast<execution::ast::StructTypeRepr *>(decl->TypeRepr());
  auto &fields = type->Fields();

  // Rough loop to get an estimate size of entire struct
  size_t total = total_offset;
  for (auto *field : fields) {
    auto *field_repr = field->TypeRepr();
    if (field_repr->GetType() != nullptr) {
      total += field_repr->GetType()->Size();
    } else if (execution::ast::IdentifierExpr::classof(field_repr)) {
      // Likely built in type
      auto *type = ast_ctx_->LookupBuiltinType(reinterpret_cast<execution::ast::IdentifierExpr *>(field_repr)->Name());
      if (type != nullptr) {
        total += type->Size();
      }
    }
  }

  // For mini-runners, only ints for sort, hj, aggregate
  double num_keys = key_size / 4.0;
  double ref_payload = (num_keys * sizeof(execution::sql::Integer)) + ref_offset;
  return total / ref_payload;
}

size_t OperatingUnitRecorder::ComputeKeySize(
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &exprs) {
  size_t key_size = 0;
  for (auto &expr : exprs) {
    // TODO(wz2): Need a better way to handle varchars
    // GetTypeSize() return storage::VARCHAR_COLUMN for varchars but expr does not have length info
    key_size += storage::AttrSizeBytes(type::TypeUtil::GetTypeSize(expr->GetReturnValueType()));
  }

  TERRIER_ASSERT(key_size > 0, "KeySize must be greater than 0");
  return key_size;
}

size_t OperatingUnitRecorder::ComputeKeySizeOutputSchema(const planner::AbstractPlanNode *plan) {
  size_t key_size = 0;
  for (auto &col : plan->GetOutputSchema()->GetColumns()) {
    key_size += storage::AttrSizeBytes(type::TypeUtil::GetTypeSize(col.GetType()));
  }

  TERRIER_ASSERT(key_size > 0, "KeySize must be greater than 0");
  return key_size;
}

size_t OperatingUnitRecorder::ComputeKeySize(catalog::table_oid_t tbl_oid) {
  size_t key_size = 0;
  auto &schema = accessor_->GetSchema(tbl_oid);
  for (auto &col : schema.GetColumns()) {
    if (col.AttrSize() == storage::VARLEN_COLUMN && col.MaxVarlenSize() > 0) {
      key_size += col.MaxVarlenSize();
    } else {
      key_size += storage::AttrSizeBytes(col.AttrSize());
    }
  }

  TERRIER_ASSERT(key_size > 0, "KeySize must be greater than 0");
  return key_size;
}

size_t OperatingUnitRecorder::ComputeKeySize(catalog::table_oid_t tbl_oid,
                                             const std::vector<catalog::col_oid_t> &cols) {
  size_t key_size = 0;
  auto &schema = accessor_->GetSchema(tbl_oid);
  for (auto &oid : cols) {
    auto &col = schema.GetColumn(oid);
    if (col.AttrSize() == storage::VARLEN_COLUMN && col.MaxVarlenSize() > 0) {
      key_size += col.MaxVarlenSize();
    } else {
      key_size += storage::AttrSizeBytes(col.AttrSize());
    }
  }

  TERRIER_ASSERT(key_size > 0, "KeySize must be greater than 0");
  return key_size;
}

size_t OperatingUnitRecorder::ComputeKeySize(catalog::index_oid_t idx_oid,
                                             const std::vector<catalog::indexkeycol_oid_t> &cols) {
  std::unordered_set<catalog::indexkeycol_oid_t> kcols;
  for (auto &col : cols) kcols.insert(col);

  size_t key_size = 0;
  auto &schema = accessor_->GetIndexSchema(idx_oid);
  for (auto &col : schema.GetColumns()) {
    if (kcols.find(col.Oid()) != kcols.end()) {
      if ((col.Type() == type::TypeId::VARCHAR || col.Type() == type::TypeId::VARBINARY) && col.MaxVarlenSize() > 0) {
        key_size += col.MaxVarlenSize();
      } else {
        key_size += col.AttrSize();
      }
    }
  }

  TERRIER_ASSERT(key_size > 0, "KeySize must be greater than 0");
  return key_size;
}

void OperatingUnitRecorder::AggregateFeatures(brain::ExecutionOperatingUnitType type, size_t key_size, size_t num_keys,
                                              const planner::AbstractPlanNode *plan, size_t scaling_factor,
                                              double mem_factor) {
  // TODO(wz2): Populate actual num_rows/cardinality after #759
  size_t num_rows = 1;
  size_t cardinality = 1;
  if (type == ExecutionOperatingUnitType::OUTPUT) {
    // Uses the network result consumer
    cardinality = 1;

    auto child_translator = current_translator_->GetChildTranslator();
    if (child_translator != nullptr) {
      if (child_translator->Op()->GetPlanNodeType() == planner::PlanNodeType::PROJECTION) {
        auto output = child_translator->Op()->GetOutputSchema()->GetColumn(0).GetExpr();
        if (output && output->GetExpressionType() == parser::ExpressionType::FUNCTION) {
          auto f_expr = output.CastManagedPointerTo<const parser::FunctionExpression>();
          if (f_expr->GetFuncName() == "nprunnersemitint" || f_expr->GetFuncName() == "nprunnersemitreal") {
            auto child = f_expr->GetChild(0);
            TERRIER_ASSERT(child, "NpRunnersEmit should have children");
            TERRIER_ASSERT(child->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT,
                           "Child should be constants");

            auto cve = child.CastManagedPointerTo<const parser::ConstantValueExpression>();
            num_rows = cve->GetInteger().val_;
          }
        }
      }
    }
  } else if (type > ExecutionOperatingUnitType::PLAN_OPS_DELIMITER) {
    // If feature is OUTPUT or computation, then cardinality = num_rows
    cardinality = num_rows;
  } else if (type == ExecutionOperatingUnitType::HASHJOIN_PROBE) {
    TERRIER_ASSERT(plan->GetPlanNodeType() == planner::PlanNodeType::HASHJOIN, "HashJoin plan expected");
    UNUSED_ATTRIBUTE auto *c_plan = plan->GetChild(1);
    num_rows = 1;     // extract from c_plan num_rows (# row to probe)
    cardinality = 1;  // extract from plan num_rows (# matched rows)
  } else if (type == ExecutionOperatingUnitType::IDX_SCAN) {
    // For IDX_SCAN, the feature is as follows:
    // - num_rows is the size of the index
    // - cardinality is the scan size
    if (plan->GetPlanNodeType() == planner::PlanNodeType::INDEXSCAN) {
      num_rows = reinterpret_cast<const planner::IndexScanPlanNode *>(plan)->GetIndexSize();
    } else {
      TERRIER_ASSERT(plan->GetPlanNodeType() == planner::PlanNodeType::INDEXNLJOIN, "Expected IdxJoin");
      num_rows = reinterpret_cast<const planner::IndexJoinPlanNode *>(plan)->GetIndexSize();
    }

    cardinality = 1;  // extract from plan num_rows (this is the scan size)
  }

  num_rows *= scaling_factor;
  cardinality *= scaling_factor;

  auto itr_pair = pipeline_features_.equal_range(type);
  for (auto itr = itr_pair.first; itr != itr_pair.second; itr++) {
    TERRIER_ASSERT(itr->second.GetExecutionOperatingUnitType() == type, "multimap consistency failure");
    if (itr->second.GetKeySize() == key_size && itr->second.GetNumKeys() == num_keys) {
      itr->second.SetNumRows(num_rows + itr->second.GetNumRows());
      itr->second.SetCardinality(cardinality + itr->second.GetCardinality());
      itr->second.AddMemFactor(mem_factor);
      return;
    }
  }

  pipeline_features_.emplace(
      type, ExecutionOperatingUnitFeature(type, num_rows, key_size, num_keys, cardinality, mem_factor));
}

void OperatingUnitRecorder::RecordArithmeticFeatures(const planner::AbstractPlanNode *plan, size_t scaling) {
  for (auto &feature : arithmetic_feature_types_) {
    TERRIER_ASSERT(feature.second > ExecutionOperatingUnitType::PLAN_OPS_DELIMITER, "Expected computation operator");
    if (feature.second != ExecutionOperatingUnitType::INVALID) {
      // Recording of simple operators
      // - num_keys is always 1
      // - key_size is max() inputs
      auto size = storage::AttrSizeBytes(type::TypeUtil::GetTypeSize(feature.first));
      AggregateFeatures(feature.second, size, 1, plan, scaling, 1);
    }
  }

  arithmetic_feature_types_.clear();
}

void OperatingUnitRecorder::VisitAbstractPlanNode(const planner::AbstractPlanNode *plan) {
  auto schema = plan->GetOutputSchema();
  if (schema != nullptr) {
    for (auto &column : schema->GetColumns()) {
      auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(column.GetExpr());
      arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                       std::make_move_iterator(features.end()));
    }
  }
}

void OperatingUnitRecorder::VisitAbstractScanPlanNode(const planner::AbstractScanPlanNode *plan) {
  VisitAbstractPlanNode(plan);

  if (plan->GetScanPredicate() != nullptr) {
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(plan->GetScanPredicate());
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));
  }
}

void OperatingUnitRecorder::Visit(const planner::SeqScanPlanNode *plan) {
  VisitAbstractScanPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  // For a sequential scan:
  // - # keys is how mahy columns are scanned (either # cols in table OR plan->GetColumnOids().size()
  // - Total key size is the size of the columns scanned
  size_t key_size = 0;
  size_t num_keys = 0;
  if (!plan->GetColumnOids().empty()) {
    // We are likely doing an update/delete -- so record key_size = 4; num_keys = 1
    // This mimics the mini_runners
    key_size = 4;
    num_keys = 1;
  } else {
    auto &schema = accessor_->GetSchema(plan->GetTableOid());
    num_keys = schema.GetColumns().size();
    key_size = ComputeKeySize(plan->GetTableOid());
  }

  AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::IndexScanPlanNode *plan) {
  std::unordered_set<catalog::indexkeycol_oid_t> cols;
  for (auto &pair : plan->GetLoIndexColumns()) {
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));

    cols.insert(pair.first);
  }

  for (auto &pair : plan->GetHiIndexColumns()) {
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));

    cols.insert(pair.first);
  }

  // Record operator features
  VisitAbstractScanPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  // For an index scan, # keys is the number of columns in the key lookup
  // The total key size is the size of the key being used to lookup
  std::vector<catalog::indexkeycol_oid_t> col_vec;
  col_vec.reserve(cols.size());
  for (auto col : cols) col_vec.emplace_back(col);

  size_t num_keys = col_vec.size();
  size_t key_size = ComputeKeySize(plan->GetIndexOid(), col_vec);
  AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::VisitAbstractJoinPlanNode(const planner::AbstractJoinPlanNode *plan) {
  if (plan_feature_type_ == ExecutionOperatingUnitType::HASHJOIN_PROBE ||
      plan_feature_type_ == ExecutionOperatingUnitType::NLJOIN_RIGHT ||
      plan_feature_type_ == ExecutionOperatingUnitType::IDXJOIN) {
    // Right side stiches together outputs
    VisitAbstractPlanNode(plan);
  }
}

void OperatingUnitRecorder::Visit(const planner::HashJoinPlanNode *plan) {
  if (plan_feature_type_ == ExecutionOperatingUnitType::HASHJOIN_BUILD) {
    for (auto key : plan->GetLeftHashKeys()) {
      auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key);
      arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                       std::make_move_iterator(features.end()));
    }

    // Get Struct and compute memory scaling factor
    auto offset = sizeof(execution::sql::HashTableEntry);
    auto key_size = ComputeKeySize(plan->GetLeftHashKeys());
    auto translator = current_translator_.CastManagedPointerTo<execution::compiler::HashJoinLeftTranslator>();
    auto scale = ComputeMemoryScaleFactor(translator->GetStructDecl(), offset, key_size, offset);

    // Record features using the row/cardinality of left plan
    auto *c_plan = plan->GetChild(0);
    RecordArithmeticFeatures(c_plan, 1);
    AggregateFeatures(plan_feature_type_, key_size, plan->GetLeftHashKeys().size(), c_plan, 1, scale);
  }

  if (plan_feature_type_ == ExecutionOperatingUnitType::HASHJOIN_PROBE) {
    for (auto key : plan->GetRightHashKeys()) {
      auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key);
      arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                       std::make_move_iterator(features.end()));
    }

    // Record features using the row/cardinality of right plan which is probe
    auto *c_plan = plan->GetChild(1);
    RecordArithmeticFeatures(c_plan, 1);
    AggregateFeatures(plan_feature_type_, ComputeKeySize(plan->GetRightHashKeys()), plan->GetRightHashKeys().size(),
                      plan, 1, 1);
  }

  // Computes against OutputSchema/Join predicate which will
  // use the rows/cardinalities of what the HJ plan produces
  VisitAbstractJoinPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);
}

void OperatingUnitRecorder::Visit(const planner::NestedLoopJoinPlanNode *plan) {
  // NLJOIN_LEFT is a pass through translator
  if (plan_feature_type_ == ExecutionOperatingUnitType::NLJOIN_RIGHT) {
    // Scale them by (num_rows - 1) of left child
    // num_rows - 1 since the right child has already been inserted once
    auto *c_plan = plan->GetChild(1);
    UNUSED_ATTRIBUTE auto *o_plan = plan->GetChild(0);

    // Left and right seq scan already exist once
    // So record the join_predicate information once
    VisitAbstractJoinPlanNode(plan);
    RecordArithmeticFeatures(c_plan, 1);

    // TODO(wz2): after #759, get num_rows  of o_plan;
    size_t o_num_rows = 0;
    if (o_num_rows > 0) {
      // Join Predicate evaluated left * right times
      // Similarly, output evaluated at worst left * right
      // Already recorded once, so record o_num_rows - 1
      VisitAbstractJoinPlanNode(plan);
      RecordArithmeticFeatures(c_plan, o_num_rows - 1);

      // Get all features/card estimates from the right child
      OperatingUnitRecorder rec(accessor_, ast_ctx_);
      rec.plan_feature_type_ = plan_feature_type_;
      plan->GetChild(1)->Accept(common::ManagedPointer<planner::PlanVisitor>(&rec));
      for (auto &feature : rec.pipeline_features_) {
        AggregateFeatures(feature.first, feature.second.GetKeySize(), feature.second.GetNumKeys(), c_plan,
                          o_num_rows - 1, 1);
      }
    }
  }
}

void OperatingUnitRecorder::Visit(const planner::IndexJoinPlanNode *plan) {
  // Scale by num_rows - 1 of the child
  UNUSED_ATTRIBUTE auto *c_plan = plan->GetChild(0);

  // Get features
  std::unordered_set<catalog::indexkeycol_oid_t> cols;
  for (auto &pair : plan->GetLoIndexColumns()) {
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));

    cols.insert(pair.first);
  }

  for (auto &pair : plan->GetHiIndexColumns()) {
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(pair.second);
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));

    cols.insert(pair.first);
  }

  // Above operations are done once per tuple in the child
  RecordArithmeticFeatures(c_plan, 1);

  // Computes against OutputSchema which will use the rows that join produces
  VisitAbstractJoinPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  // Vector of indexkeycol_oid_t columns
  std::vector<catalog::indexkeycol_oid_t> col_vec;
  col_vec.reserve(cols.size());
  for (auto &col : cols) {
    col_vec.emplace_back(col);
  }

  // IndexScan output number of rows is the "cumulative scan size"
  size_t num_keys = col_vec.size();
  size_t key_size = ComputeKeySize(plan->GetIndexOid(), col_vec);
  AggregateFeatures(brain::ExecutionOperatingUnitType::IDX_SCAN, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::InsertPlanNode *plan) {
  if (plan->GetChildrenSize() == 0) {
    // INSERT without a SELECT
    for (size_t idx = 0; idx < plan->GetBulkInsertCount(); idx++) {
      for (auto &col : plan->GetValues(idx)) {
        auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(col);
        arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                         std::make_move_iterator(features.end()));
      }
    }

    // Record features
    VisitAbstractPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    // Record the Insert
    auto key_size = ComputeKeySize(plan->GetTableOid(), plan->GetParameterInfo());
    AggregateFeatures(plan_feature_type_, key_size, plan->GetParameterInfo().size(), plan, 1, 1);
  } else {
    // INSERT with a SELECT
    auto *c_plan = plan->GetChild(0);
    TERRIER_ASSERT(c_plan->GetOutputSchema() != nullptr, "Child must have OutputSchema");

    auto size = ComputeKeySizeOutputSchema(c_plan);
    AggregateFeatures(plan_feature_type_, size, c_plan->GetOutputSchema()->GetColumns().size(), plan, 1, 1);
  }
}

void OperatingUnitRecorder::Visit(const planner::UpdatePlanNode *plan) {
  std::vector<catalog::col_oid_t> cols;
  for (auto &clause : plan->GetSetClauses()) {
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(clause.second);
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));

    cols.emplace_back(clause.first);
  }

  // Record features
  VisitAbstractPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  // Record the Update
  auto key_size = ComputeKeySize(plan->GetTableOid(), cols);
  AggregateFeatures(plan_feature_type_, key_size, cols.size(), plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::DeletePlanNode *plan) {
  VisitAbstractPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  auto &schema = accessor_->GetSchema(plan->GetTableOid());
  auto num_cols = schema.GetColumns().size();
  AggregateFeatures(plan_feature_type_, ComputeKeySize(plan->GetTableOid()), num_cols, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::CSVScanPlanNode *plan) {
  VisitAbstractScanPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  auto num_keys = plan->GetValueTypes().size();
  size_t key_size = 0;
  for (auto type : plan->GetValueTypes()) {
    key_size += type::TypeUtil::GetTypeSize(type);
  }

  AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::LimitPlanNode *plan) {
  VisitAbstractPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);

  // Copy outwards
  auto num_keys = plan->GetOutputSchema()->GetColumns().size();
  auto key_size = ComputeKeySizeOutputSchema(plan);
  AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
}

void OperatingUnitRecorder::Visit(const planner::OrderByPlanNode *plan) {
  if (plan_feature_type_ == ExecutionOperatingUnitType::SORT_BUILD) {
    // SORT_BUILD will operate on sort keys
    std::vector<common::ManagedPointer<parser::AbstractExpression>> keys;
    for (auto key : plan->GetSortKeys()) {
      auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key.first);
      arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                       std::make_move_iterator(features.end()));

      keys.emplace_back(key.first);
    }

    // Get Struct and compute memory scaling factor
    auto key_size = ComputeKeySize(keys);
    auto translator = current_translator_.CastManagedPointerTo<execution::compiler::SortBottomTranslator>();
    auto scale = ComputeMemoryScaleFactor(translator->GetStructDecl(), 0, key_size, 0);

    // Sort build sizes/operations are based on the input (from child)
    auto *c_plan = plan->GetChild(0);
    RecordArithmeticFeatures(c_plan, 1);
    AggregateFeatures(plan_feature_type_, key_size, keys.size(), c_plan, 1, scale);
  } else if (plan_feature_type_ == ExecutionOperatingUnitType::SORT_ITERATE) {
    // SORT_ITERATE will do any output computations
    VisitAbstractPlanNode(plan);
    RecordArithmeticFeatures(plan, 1);

    // Copy outwards
    auto num_keys = plan->GetOutputSchema()->GetColumns().size();
    auto key_size = ComputeKeySizeOutputSchema(plan);
    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
  }
}

void OperatingUnitRecorder::Visit(const planner::ProjectionPlanNode *plan) {
  VisitAbstractPlanNode(plan);
  RecordArithmeticFeatures(plan, 1);
}

void OperatingUnitRecorder::Visit(const planner::AggregatePlanNode *plan) {
  if (plan_feature_type_ == ExecutionOperatingUnitType::AGGREGATE_BUILD) {
    for (auto key : plan->GetAggregateTerms()) {
      auto key_cm = common::ManagedPointer<parser::AbstractExpression>(key.Get());
      auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key_cm);
      arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                       std::make_move_iterator(features.end()));
    }

    for (auto key : plan->GetGroupByTerms()) {
      auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(key);
      arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                       std::make_move_iterator(features.end()));
    }

    // Above computations performed for all of child
    auto *c_plan = plan->GetChild(0);
    RecordArithmeticFeatures(c_plan, 1);

    // Build with the rows of child
    size_t key_size = 0;
    size_t num_keys = 0;
    double mem_factor = 1;
    if (!plan->GetGroupByTerms().empty()) {
      key_size = ComputeKeySize(plan->GetGroupByTerms());
      num_keys = plan->GetGroupByTerms().size();

      // Get Struct and compute memory scaling factor
      auto offset = sizeof(execution::sql::HashTableEntry);
      auto ref_offset = offset + sizeof(execution::sql::CountAggregate);
      auto translator = current_translator_.CastManagedPointerTo<execution::compiler::AggregateBottomTranslator>();
      mem_factor = ComputeMemoryScaleFactor(translator->GetStructDecl(), offset, key_size, ref_offset);
    } else {
      std::vector<common::ManagedPointer<parser::AbstractExpression>> keys;
      for (auto term : plan->GetAggregateTerms()) {
        if (term->IsDistinct()) {
          keys.emplace_back(term.Get());
        }
      }

      if (!keys.empty()) {
        key_size = ComputeKeySize(keys);
        num_keys = keys.size();
      }
    }

    AggregateFeatures(plan_feature_type_, key_size, num_keys, c_plan, 1, mem_factor);
  } else if (plan_feature_type_ == ExecutionOperatingUnitType::AGGREGATE_ITERATE) {
    // AggregateTopTranslator handles any exprs/computations in the output
    VisitAbstractPlanNode(plan);

    // AggregateTopTranslator handles the having clause
    auto features = OperatingUnitUtil::ExtractFeaturesFromExpression(plan->GetHavingClausePredicate());
    arithmetic_feature_types_.insert(arithmetic_feature_types_.end(), std::make_move_iterator(features.begin()),
                                     std::make_move_iterator(features.end()));

    RecordArithmeticFeatures(plan, 1);

    // Copy outwards
    auto num_keys = plan->GetOutputSchema()->GetColumns().size();
    auto key_size = ComputeKeySizeOutputSchema(plan);
    AggregateFeatures(plan_feature_type_, key_size, num_keys, plan, 1, 1);
  }
}

ExecutionOperatingUnitFeatureVector OperatingUnitRecorder::RecordTranslators(
    const std::vector<std::unique_ptr<execution::compiler::OperatorTranslator>> &translators) {
  pipeline_features_ = {};
  for (const auto &translator : translators) {
    plan_feature_type_ = translator->GetFeatureType();
    current_translator_ = common::ManagedPointer(translator);

    if (plan_feature_type_ != ExecutionOperatingUnitType::INVALID) {
      if (plan_feature_type_ == ExecutionOperatingUnitType::OUTPUT) {
        TERRIER_ASSERT(translator->GetChildTranslator(), "OUTPUT should have child translator");
        auto child_type = translator->GetChildTranslator()->GetFeatureType();
        if (child_type == ExecutionOperatingUnitType::INSERT || child_type == ExecutionOperatingUnitType::UPDATE ||
            child_type == ExecutionOperatingUnitType::DELETE) {
          // For insert/update/delete, there actually is no real output happening.
          // So, skip the Output operator
          continue;
        }

        auto *op = translator->GetChildTranslator()->Op();
        auto num_keys = op->GetOutputSchema()->GetColumns().size();
        auto key_size = ComputeKeySizeOutputSchema(op);
        AggregateFeatures(plan_feature_type_, key_size, num_keys, op, 1, 1);
      } else {
        translator->Op()->Accept(common::ManagedPointer<planner::PlanVisitor>(this));
        TERRIER_ASSERT(arithmetic_feature_types_.empty(), "aggregate_feature_types_ should be empty");
      }
    }
  }

  // Consolidate final features
  std::vector<ExecutionOperatingUnitFeature> results{};
  results.reserve(pipeline_features_.size());
  for (auto &feature : pipeline_features_) {
    results.emplace_back(feature.second);
  }

  pipeline_features_.clear();
  return results;
}

}  // namespace terrier::brain
