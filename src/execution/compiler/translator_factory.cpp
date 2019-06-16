#include "execution/compiler/translator_factory.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/expression/tuple_value_translator.h"
#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/operator/insert_translator.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/operator/aggregate_translator.h"
#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/util/macros.h"

namespace tpl::compiler {

OperatorTranslator *TranslatorFactory::CreateRegularTranslator(const terrier::planner::AbstractPlanNode * op,
                                                        CodeGen * codegen) {
  // TODO(Amadou): Region allocation is causing issues here (memory content changes).
  // We are temporarily using the std allocation to avoid them.
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::SEQSCAN: {
      return new SeqScanTranslator(op, codegen);
    }
    case terrier::planner::PlanNodeType::INSERT: {
      return nullptr;
    }
    default:
      UNREACHABLE("Unsupported plan nodes");
  }
}

OperatorTranslator *TranslatorFactory::CreateBottomTranslator(const terrier::planner::AbstractPlanNode *op,
                                                               CodeGen * codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE:
      return new AggregateBottomTranslator(op, codegen);
    case terrier::planner::PlanNodeType::ORDERBY:
      return nullptr;
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

OperatorTranslator *TranslatorFactory::CreateTopTranslator(const terrier::planner::AbstractPlanNode *op,
                                                               OperatorTranslator * bottom,
                                                               CodeGen * codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::AGGREGATE:
      return new AggregateTopTranslator(op, codegen, bottom);
    case terrier::planner::PlanNodeType::ORDERBY:
      return nullptr;
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}


OperatorTranslator *TranslatorFactory::CreateLeftTranslator(const terrier::planner::AbstractPlanNode *op,
                                                            CodeGen * codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::HASHJOIN:
      return new HashJoinLeftTranslator(op, codegen);
    case terrier::planner::PlanNodeType::NESTLOOP:
      return nullptr;
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}


OperatorTranslator *TranslatorFactory::CreateRightTranslator(const terrier::planner::AbstractPlanNode *op,
                                                             OperatorTranslator * left,
                                                             CodeGen * codegen) {
  switch (op->GetPlanNodeType()) {
    case terrier::planner::PlanNodeType::HASHJOIN:
      return new HashJoinRightTranslator(op, codegen, left);
    case terrier::planner::PlanNodeType::NESTLOOP:
      return nullptr;
    default:
      UNREACHABLE("Not a pipeline boundary!");
  }
}

ExpressionTranslator *TranslatorFactory::CreateExpressionTranslator(const terrier::parser::AbstractExpression * expression,
                                                                    CodeGen * codegen) {
  auto type = expression->GetExpressionType();
  if (COMPARISON_OP(type)) {
    auto ret = new ComparisonTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  if (ARITHMETIC_OP(type)) {
    auto ret = new ArithmeticTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  if (UNARY_OP(type)) {
    auto ret = new UnaryTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  if (CONJUNCTION_OP(type)) {
    auto ret = new ConjunctionTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  if (CONSTANT_VAL(type)) {
    auto ret = new ConstantTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  if (TUPLE_VAL(type)) {
    auto ret = new TupleValueTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  if (NULL_OP(type)) {
    auto ret = new NullCheckTranslator(expression, codegen);
    return reinterpret_cast<ExpressionTranslator *>(ret);
  }
  TPL_ASSERT(false, "Unsupported expression");
  return nullptr;
}

}  // namespace tpl::compiler
