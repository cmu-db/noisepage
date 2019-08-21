#include "execution/compiler/translator_factory.h"

#include "common/macros.h"
#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/expression/column_value_translator.h"
#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/expression/derived_value_translator.h"
#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/expression/tuple_value_translator.h"
#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/operator/aggregate_translator.h"
#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/operator/index_join_translator.h"
#include "execution/compiler/operator/insert_translator.h"
#include "execution/compiler/operator/nested_loop_translator.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/operator/sort_translator.h"
#include "execution/compiler/pipeline.h"

namespace terrier::execution::compiler {

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateRegularTranslator(
    const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) {
  // TODO(Amadou): Region allocation is causing issues here (memory content changes).
  // We are temporarily using the std allocation to avoid them.
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::SEQSCAN: {
      return std::make_unique<SeqScanTranslator>(op, codegen);
    }
    case terrier::planner::PlanNodeType::INSERT: {
      return nullptr;
    }
    case terrier::planner::PlanNodeType::INDEXNLJOIN: {
      return std::make_unique<IndexJoinTranslator>(op, codegen);
    }
    default:
      UNREACHABLE("Unsupported plan nodes");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateBottomTranslator(
    const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE:
      return std::make_unique<AggregateBottomTranslator>(op, codegen);
    case terrier::planner::PlanNodeType::ORDERBY:
      return std::make_unique<SortBottomTranslator>(op, codegen);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateTopTranslator(const terrier::planner::AbstractPlanNode *op,
                                                                           OperatorTranslator *bottom,
                                                                           CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE:
      return std::make_unique<AggregateTopTranslator>(op, codegen, bottom);
    case terrier::planner::PlanNodeType::ORDERBY:
      return std::make_unique<SortTopTranslator>(op, codegen, bottom);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateLeftTranslator(
    const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::HASHJOIN:
      return std::make_unique<HashJoinLeftTranslator>(op, codegen);
    case terrier::planner::PlanNodeType::NESTLOOP:
      return std::make_unique<NestedLoopLeftTransaltor>(op, codegen);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<OperatorTranslator> TranslatorFactory::CreateRightTranslator(
    const terrier::planner::AbstractPlanNode *op, OperatorTranslator *left, CodeGen *codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::HASHJOIN:
      return std::make_unique<HashJoinRightTranslator>(op, codegen, left);
    case terrier::planner::PlanNodeType::NESTLOOP:
      return std::make_unique<NestedLoopRightTransaltor>(op, codegen, left);
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

std::unique_ptr<ExpressionTranslator> TranslatorFactory::CreateExpressionTranslator(
    const terrier::parser::AbstractExpression *expression, CodeGen *codegen) {
  auto type = expression->GetExpressionType();
  if (COMPARISON_OP(type)) {
    return std::make_unique<ComparisonTranslator>(expression, codegen);
  }
  if (ARITHMETIC_OP(type)) {
    return std::make_unique<ArithmeticTranslator>(expression, codegen);
  }
  if (UNARY_OP(type)) {
    return std::make_unique<UnaryTranslator>(expression, codegen);
  }
  if (CONJUNCTION_OP(type)) {
    return std::make_unique<ConjunctionTranslator>(expression, codegen);
  }
  if (CONSTANT_VAL(type)) {
    return std::make_unique<ConstantTranslator>(expression, codegen);
  }
  if (COLUMN_VAL(type)) {
    return std::make_unique<ColumnValueTranslator>(expression, codegen);
  }
  if (DERIVED_VAL(type)) {
    return std::make_unique<DerivedValueTranslator>(expression, codegen);
  }
  if (NULL_OP(type)) {
    return std::make_unique<NullCheckTranslator>(expression, codegen);
  }
  UNREACHABLE("Unsupported expression");
}

}  // namespace terrier::execution::compiler
