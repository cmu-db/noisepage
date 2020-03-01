#include "execution/compiler/translator_factory.h"

#include <memory>

#include "common/macros.h"
#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/expression/column_value_translator.h"
#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/expression/derived_value_translator.h"
#include "execution/compiler/expression/function_translator.h"
#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/expression/param_value_translator.h"
#include "execution/compiler/expression/star_translator.h"
#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/operator/aggregate_translator.h"
#include "execution/compiler/operator/delete_translator.h"
#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/operator/index_join_translator.h"
#include "execution/compiler/operator/index_scan_translator.h"
#include "execution/compiler/operator/insert_translator.h"
#include "execution/compiler/operator/limit_translator.h"
#include "execution/compiler/operator/nested_loop_translator.h"
#include "execution/compiler/operator/projection_translator.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/operator/sort_translator.h"
#include "execution/compiler/operator/static_aggregate_translator.h"
#include "execution/compiler/operator/update_translator.h"
#include "execution/compiler/pipeline.h"

namespace terrier::execution::compiler {

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateRegularTranslator(
    const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) {
  // TODO(Amadou): Region allocation is causing issues here (memory content changes).
  // We are temporarily using the std allocation to avoid them.
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::SEQSCAN: {
      return std::make_unique<SeqScanTranslator>(static_cast<const planner::SeqScanPlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::INSERT: {
      return std::make_unique<InsertTranslator>(static_cast<const planner::InsertPlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::DELETE: {
      return std::make_unique<DeleteTranslator>(static_cast<const planner::DeletePlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::UPDATE: {
      return std::make_unique<UpdateTranslator>(static_cast<const planner::UpdatePlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::INDEXNLJOIN: {
      return std::make_unique<IndexJoinTranslator>(static_cast<const planner::IndexJoinPlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::INDEXSCAN: {
      return std::make_unique<IndexScanTranslator>(static_cast<const planner::IndexScanPlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::PROJECTION: {
      return std::make_unique<ProjectionTranslator>(static_cast<const planner::ProjectionPlanNode *>(op), codegen);
    }
    case terrier::planner::PlanNodeType::LIMIT: {
      return std::make_unique<LimitTranslator>(static_cast<const planner::LimitPlanNode *>(op), codegen);
    }
    default:
      UNREACHABLE("Unsupported plan nodes");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateBottomTranslator(
    const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE: {
      auto agg_op = static_cast<const planner::AggregatePlanNode *>(op);
      if (agg_op->GetGroupByTerms().empty()) {
        return std::make_unique<StaticAggregateBottomTranslator>(agg_op, codegen);
      }
      return std::make_unique<AggregateBottomTranslator>(agg_op, codegen);
    }
    case terrier::planner::PlanNodeType::ORDERBY:
      return std::make_unique<SortBottomTranslator>(static_cast<const planner::OrderByPlanNode *>(op), codegen);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateTopTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                           OperatorTranslator *bottom,
                                                                           CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE: {
      auto agg_op = static_cast<const planner::AggregatePlanNode *>(op);
      if (agg_op->GetGroupByTerms().empty()) {
        return std::make_unique<StaticAggregateTopTranslator>(agg_op, codegen, bottom);
      }
      return std::make_unique<AggregateTopTranslator>(agg_op, codegen, bottom);
    }
    case terrier::planner::PlanNodeType::ORDERBY:
      return std::make_unique<SortTopTranslator>(static_cast<const planner::OrderByPlanNode *>(op), codegen, bottom);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateLeftTranslator(
    const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::HASHJOIN:
      return std::make_unique<HashJoinLeftTranslator>(static_cast<const planner::HashJoinPlanNode *>(op), codegen);
    case terrier::planner::PlanNodeType::NESTLOOP:
      return std::make_unique<NestedLoopLeftTranslator>(static_cast<const planner::NestedLoopJoinPlanNode *>(op),
                                                        codegen);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateRightTranslator(
    const terrier::planner::AbstractPlanNode *op, OperatorTranslator *left, CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::HASHJOIN:
      return std::make_unique<HashJoinRightTranslator>(static_cast<const planner::HashJoinPlanNode *>(op), codegen,
                                                       left);
    case terrier::planner::PlanNodeType::NESTLOOP:
      return std::make_unique<NestedLoopRightTranslator>(static_cast<const planner::NestedLoopJoinPlanNode *>(op),
                                                         codegen, left);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<ExpressionTranslator> TranslatorFactory::CreateExpressionTranslator(
    const terrier::parser::AbstractExpression *expression, CodeGen *codegen) {
  auto type = expression->GetExpressionType();
  if (IsComparisonOp(type)) {
    return std::make_unique<ComparisonTranslator>(expression, codegen);
  }
  if (IsArithmeticOp(type)) {
    return std::make_unique<ArithmeticTranslator>(expression, codegen);
  }
  if (IsUnaryOp(type)) {
    return std::make_unique<UnaryTranslator>(expression, codegen);
  }
  if (IsConjunctionOp(type)) {
    return std::make_unique<ConjunctionTranslator>(expression, codegen);
  }
  if (IsConstantVal(type)) {
    return std::make_unique<ConstantTranslator>(expression, codegen);
  }
  if (IsColumnVal(type)) {
    return std::make_unique<ColumnValueTranslator>(expression, codegen);
  }
  if (IsDerivedVal(type)) {
    return std::make_unique<DerivedValueTranslator>(expression, codegen);
  }
  if (IsParamVal(type)) {
    return std::make_unique<ParamValueTranslator>(expression, codegen);
  }
  if (IsNullOp(type)) {
    return std::make_unique<NullCheckTranslator>(expression, codegen);
  }
  if (IsStar(type)) {
    return std::make_unique<StarTranslator>(expression, codegen);
  }
  if (IsFunction(type)) {
    return std::make_unique<FunctionTranslator>(expression, codegen);
  }
  UNREACHABLE("Unsupported expression");
}

}  // namespace terrier::execution::compiler
