#include "execution/compiler/operator/aggregate_translator.h"
#include <utility>
#include <vector>
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
AggregateBottomTranslator::AggregateBottomTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::AGGREGATE_BUILD), op_(op), helper_(codegen, op) {}

void AggregateBottomTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) {
  // There the aggregation hash tables.
  helper_.DeclareAHTs(state_fields);
}

void AggregateBottomTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {
  // Declare the values struct.
  helper_.GenValuesStruct(decls);
  // Declare the struct of each distinct aggregate.
  helper_.GenAHTStructs(&struct_decl_, decls);
}

void AggregateBottomTranslator::InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) {
  // Generate the key check functions.
  helper_.GenKeyChecks(decls);
}

void AggregateBottomTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  // Initialize all hash tables.
  helper_.InitAHTs(setup_stmts);
}

void AggregateBottomTranslator::InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) {
  // Free all hash tables
  helper_.FreeAHTs(teardown_stmts);
}

void AggregateBottomTranslator::Produce(FunctionBuilder *builder) { child_translator_->Produce(builder); }

void AggregateBottomTranslator::Abort(FunctionBuilder *builder) { child_translator_->Abort(builder); }

void AggregateBottomTranslator::Consume(FunctionBuilder *builder) {
  // Generate Values
  helper_.FillValues(builder, this);
  // Generate hashes
  helper_.GenHashCalls(builder);
  // Construct new hash table entries
  helper_.GenConstruct(builder);
  // Advance non distinct aggregates
  helper_.GenAdvanceNonDistinct(builder);
}

ast::Expr *AggregateBottomTranslator::GetGroupByOutput(uint32_t term_idx) {
  auto global_aht = helper_.GetGlobalAHT();
  return codegen_->MemberExpr(global_aht->Entry(), helper_.GetGroupBy(term_idx));
}

ast::Expr *AggregateBottomTranslator::GetAggregateOutput(uint32_t term_idx) {
  auto global_aht = helper_.GetGlobalAHT();
  auto agg = codegen_->MemberExpr(global_aht->Entry(), helper_.GetAggregate(term_idx));
  return codegen_->OneArgCall(ast::Builtin::AggResult, codegen_->PointerTo(agg));
}

ast::Expr *AggregateBottomTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx,
                                                     terrier::type::TypeId type) {
  return child_translator_->GetOutput(attr_idx);
}

///////////////////////////////////////////////
///// Top Translator
///////////////////////////////////////////////

void AggregateTopTranslator::Produce(FunctionBuilder *builder) {
  DeclareIterator(builder);
  // In case of nested loop joins, let the child produce
  if (child_translator_ != nullptr) {
    child_translator_->Produce(builder);
  } else {
    // Otherwise directly consume the bottom's output
    Consume(builder);
  }
}

void AggregateTopTranslator::Consume(FunctionBuilder *builder) {
  GenHTLoop(builder);
  DeclareResult(builder);
  bool has_having = GenHaving(builder);
  parent_translator_->Consume(builder);

  // Close having statement
  if (has_having) {
    builder->FinishBlockStmt();
  }
  // Close HT loop
  builder->FinishBlockStmt();
  // Close iterator
  CloseIterator(builder);
}

void AggregateTopTranslator::Abort(FunctionBuilder *builder) {
  // Close iterator
  CloseIterator(builder);
  if (child_translator_ != nullptr) child_translator_->Abort(builder);
}

ast::Expr *AggregateTopTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  if (child_idx == 0) return bottom_->GetGroupByOutput(attr_idx);
  return bottom_->GetAggregateOutput(attr_idx);
}

// Let the bottom translator handle this call
ast::Expr *AggregateTopTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr.Get(), codegen_);
  return translator->DeriveExpr(this);
}

// Declare var agg_iterator: *AggregationHashTableIterator
void AggregateTopTranslator::DeclareIterator(FunctionBuilder *builder) {
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::AggregationHashTableIterator);
  builder->Append(codegen_->DeclareVariable(agg_iterator_, iter_type, nullptr));
}

// for (@aggHTIterInit(&agg_iter, &state.table); @aggHTIterHasNext(&agg_iter); @aggHTIterNext(&agg_iter)) {...}
void AggregateTopTranslator::GenHTLoop(FunctionBuilder *builder) {
  auto global_aht = bottom_->helper_.GetGlobalAHT();
  // Loop Initialization
  std::vector<ast::Expr *> init_args{codegen_->PointerTo(agg_iterator_), codegen_->GetStateMemberPtr(global_aht->HT())};
  ast::Expr *init_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableIterInit, std::move(init_args));
  ast::Stmt *loop_init = codegen_->MakeStmt(init_call);
  // Loop condition
  ast::Expr *has_next_call = codegen_->OneArgCall(ast::Builtin::AggHashTableIterHasNext, agg_iterator_, true);
  // Loop update
  ast::Expr *next_call = codegen_->OneArgCall(ast::Builtin::AggHashTableIterNext, agg_iterator_, true);
  ast::Stmt *loop_update = codegen_->MakeStmt(next_call);
  // Make the loop
  builder->StartForStmt(loop_init, has_next_call, loop_update);
}

// Declare var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
void AggregateTopTranslator::DeclareResult(FunctionBuilder *builder) {
  auto global_aht = bottom_->helper_.GetGlobalAHT();
  // @aggHTIterGetRow(&agg_iter)
  ast::Expr *get_row_call = codegen_->OneArgCall(ast::Builtin::AggHashTableIterGetRow, agg_iterator_, true);

  // @ptrcast(*AggPayload, ...)
  ast::Expr *cast_call = codegen_->PtrCast(global_aht->StructType(), get_row_call);

  // Declare var agg_payload
  builder->Append(codegen_->DeclareVariable(global_aht->Entry(), nullptr, cast_call));
}

void AggregateTopTranslator::CloseIterator(FunctionBuilder *builder) {
  // Call @aggHTIterCLose(agg_iter)
  ast::Expr *close_call = codegen_->OneArgCall(ast::Builtin::AggHashTableIterClose, agg_iterator_, true);
  builder->Append(codegen_->MakeStmt(close_call));
}

bool AggregateTopTranslator::GenHaving(execution::compiler::FunctionBuilder *builder) {
  if (op_->GetHavingClausePredicate() != nullptr) {
    auto predicate = op_->GetHavingClausePredicate().Get();
    auto translator = TranslatorFactory::CreateExpressionTranslator(predicate, codegen_);
    ast::Expr *cond = translator->DeriveExpr(this);
    builder->StartIfStmt(cond);
    return true;
  }
  return false;
}

}  // namespace terrier::execution::compiler
