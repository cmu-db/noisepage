#include "execution/compiler/translator_factory.h"

#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/util/macros.h"
#include "execution/compiler/expression/arithmetic_translator.h"
#include "execution/compiler/expression/comparison_translator.h"
#include "execution/compiler/expression/conjunction_translator.h"
#include "execution/compiler/expression/constant_translator.h"
#include "execution/compiler/expression/null_check_translator.h"
#include "execution/compiler/expression/tuple_value_translator.h"
#include "execution/compiler/expression/unary_translator.h"
#include "execution/compiler/compilation_context.h"

namespace tpl::compiler {

OperatorTranslator *TranslatorFactory::CreateTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline) {
    switch (op.GetPlanNodeType()) {
      case terrier::planner::PlanNodeType::SEQSCAN: {
        return new (pipeline->GetRegion()) SeqScanTranslator(op, pipeline);
      }
      default:
        TPL_ASSERT(false, "Unsupported plan node for translation");
    }
  }

ExpressionTranslator *TranslatorFactory::CreateTranslator(const terrier::parser::AbstractExpression &expression, CompilationContext &context) {
  auto type = expression.GetExpressionType();
  if(COMPARISON_OP(type)){
    auto ret = new (context.GetRegion()) ComparisonTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  if(ARITHMETIC_OP(type)){
    auto ret = new (context.GetRegion()) ArithmeticTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  if(UNARY_OP(type)){
    auto ret = new (context.GetRegion()) UnaryTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  if(CONJUNCTION_OP(type)){
    auto ret = new (context.GetRegion()) ConjunctionTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  if(CONSTANT_VAL(type)){
    auto ret = new (context.GetRegion()) ConstantTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  if(TUPLE_VAL(type)){
    auto ret = new (context.GetRegion()) TupleValueTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  if(NULL_OP(type)){
    auto ret = new (context.GetRegion()) NullCheckTranslator(&expression, context);
    return reinterpret_cast<ExpressionTranslator*>(ret);
  }
  TPL_ASSERT(false, "Unsupported expression");
  return nullptr;
}

} //namespace tpl::compiler
