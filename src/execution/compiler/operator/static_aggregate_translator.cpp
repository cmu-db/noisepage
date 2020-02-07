#include "execution/compiler/operator/static_aggregate_translator.h"
#include <utility>
#include <vector>
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
StaticAggregateBottomTranslator::StaticAggregateBottomTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen)
: OperatorTranslator(codegen)
, op_(op)
, helper_(codegen, op)
{
}

void StaticAggregateBottomTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields)  {
  // Static aggregations add their aggregates directly in the state.
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    auto term = op_->GetAggregateTerms()[term_idx];
    ast::Expr *agg_type = codegen_->AggregateType(term->GetExpressionType(), term->GetChild(0)->GetReturnValueType());
    state_fields->emplace_back(codegen_->MakeField(helper_.GetAggregate(term_idx), agg_type));
  }
  // Each distinct aggregate needs its own hash table.
  helper_.GenDistinctStateFields(state_fields);
}

void StaticAggregateBottomTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {
  // Create a field of every aggregate term.
  helper_.GenValuesStructs(decls);
  // Distinct aggregate each need a hash table.
  helper_.GenDistinctStructs(decls);
}

void StaticAggregateBottomTranslator::InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) {
  // Each distinct aggregate needs its own key check function.
  helper_.GenDistinctKeyChecks(decls);
}

void StaticAggregateBottomTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  // Static aggregations initialize their aggregates in the global setup.
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *agg = codegen_->GetStateMemberPtr(helper_.GetAggregate(term_idx));
    ast::Expr *agg_init_call = codegen_->OneArgCall(ast::Builtin::AggInit, agg);
    setup_stmts->emplace_back(codegen_->MakeStmt(agg_init_call));
  }
  // Distinct aggregates each initialize their hash tables.
  helper_.InitDistinctTables(setup_stmts);
}

void StaticAggregateBottomTranslator::InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) {
  // Distinct aggregates each free their hash tables.
  helper_.FreeDistinctTables(teardown_stmts);
}

void StaticAggregateBottomTranslator::Consume(FunctionBuilder* builder) {
  // Generate Values
  helper_.FillValues(builder);
  // Then Advance the aggregate
  helper_.GenAdvanceAggs(builder);
}

///////////////////////
//// Top
//////////////////////

void StaticAggregateTopTranslator::Produce(FunctionBuilder* builder) {
  bool has_having = op_->GetHavingClausePredicate() != nullptr;
  // Generate the having condition
  if (has_having) {
    auto predicate = op_->GetHavingClausePredicate().Get();
    auto translator = TranslatorFactory::CreateExpressionTranslator(predicate, codegen_);
    ast::Expr *cond = translator->DeriveExpr(this);
    builder->StartIfStmt(cond);
  }

  if (child_translator_ != nullptr)  {
    child_translator_->Produce(builder);
  } else {
    parent_translator_->Consume(builder);
  }

  // Close the if statement
  if (has_having) {
    builder->FinishBlockStmt();
  }
}

ast::Expr *StaticAggregateTopTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
  return translator->DeriveExpr(this);
}


} // namespace terrier::execution::compiler

