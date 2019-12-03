#include "execution/compiler/operator/aggregate_translator.h"
#include <utility>
#include <vector>
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
AggregateBottomTranslator::AggregateBottomTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen)
    : OperatorTranslator(codegen),
      num_group_by_terms_{static_cast<uint32_t>(op->GetGroupByTerms().size())},
      op_(op),
      hash_val_(codegen->NewIdentifier("hash_val")),
      agg_values_(codegen->NewIdentifier("agg_value")),
      values_struct_(codegen->NewIdentifier("AggValues")),
      payload_struct_(codegen->NewIdentifier("AggPayload")),
      agg_payload_(codegen->NewIdentifier("agg_payload")),
      key_check_(codegen->NewIdentifier("aggKeyCheckFn")),
      agg_ht_(codegen->NewIdentifier("agg_ht")) {}

// Declare the hash table
void AggregateBottomTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) {
  // agg_hash_table : AggregationHashTable
  ast::Expr *ht_type = codegen_->BuiltinType(ast::BuiltinType::Kind::AggregationHashTable);
  state_fields->emplace_back(codegen_->MakeField(agg_ht_, ht_type));
}

// Declare payload and decls struct
void AggregateBottomTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {
  GenPayloadStruct(decls);
  GenValuesStruct(decls);
}

// Create the key check function.
void AggregateBottomTranslator::InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) {
  GenSingleKeyCheckFn(decls);
}

// Call @aggHTInit on the hash table
void AggregateBottomTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  // @aggHTInit(&state.agg_hash_table, @execCtxGetMem(execCtx), @sizeOf(AggPayload))
  ast::Expr *init_call = codegen_->HTInitCall(ast::Builtin::AggHashTableInit, agg_ht_, payload_struct_);
  // Add it the setup statements
  setup_stmts->emplace_back(codegen_->MakeStmt(init_call));
}

// Call @aggHTFree
void AggregateBottomTranslator::InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) {
  ast::Expr *free_call = codegen_->OneArgStateCall(ast::Builtin::AggHashTableFree, agg_ht_);
  teardown_stmts->emplace_back(codegen_->MakeStmt(free_call));
}

void AggregateBottomTranslator::Produce(FunctionBuilder *builder) { child_translator_->Produce(builder); }

void AggregateBottomTranslator::Abort(FunctionBuilder *builder) { child_translator_->Abort(builder); }

void AggregateBottomTranslator::Consume(FunctionBuilder *builder) {
  // Generate values to aggregate
  FillValues(builder);
  // Hash Call
  GenHashCall(builder);
  // Make Lookup call
  GenLookupCall(builder);
  // Construct aggregates if needed
  GenConstruct(builder);
  // Advance aggregates
  GenAdvance(builder);
}

ast::Expr *AggregateBottomTranslator::GetOutput(uint32_t attr_idx) {
  // Either access a scalar group by term
  if (attr_idx < num_group_by_terms_) {
    return GetGroupByTerm(agg_payload_, attr_idx);
  }
  // Or access an aggregate
  // Here, we need to call @aggResult(&agg_payload.expr_i)
  ast::Expr *agg_term = GetAggTerm(agg_payload_, attr_idx - num_group_by_terms_, true);
  return codegen_->BuiltinCall(ast::Builtin::AggResult, {agg_term});
}

ast::Expr *AggregateBottomTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx,
                                                     terrier::type::TypeId type) {
  return child_translator_->GetOutput(attr_idx);
}

ast::Expr *AggregateBottomTranslator::GetGroupByTerm(ast::Identifier object, uint32_t idx) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(GROUP_BY_TERM_NAMES + std::to_string(idx));
  return codegen_->MemberExpr(object, member);
}

ast::Expr *AggregateBottomTranslator::GetAggTerm(ast::Identifier object, uint32_t idx, bool ptr) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(AGG_TERM_NAMES + std::to_string(idx));
  ast::Expr *agg_term = codegen_->MemberExpr(object, member);
  if (ptr) {
    // Return a pointer to the term
    return codegen_->UnaryOp(parsing::Token::Type::AMPERSAND, agg_term);
  }
  // Return the term itself
  return agg_term;
}

/*
 * Generate the aggregation hash table's payload struct
 */
void AggregateBottomTranslator::GenPayloadStruct(util::RegionVector<ast::Decl *> *decls) {
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  // Create a field for every group by term
  uint32_t term_idx = 0;
  for (const auto &term : op_->GetGroupByTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(GROUP_BY_TERM_NAMES + std::to_string(term_idx));
    ast::Expr *type = codegen_->TplType(term->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field for every aggregate term
  term_idx = 0;
  for (const auto &term : op_->GetAggregateTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(AGG_TERM_NAMES + std::to_string(term_idx));
    ast::Expr *type = codegen_->AggregateType(term->GetExpressionType(), term->GetChild(0)->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Make the struct
  decls->emplace_back(codegen_->MakeStruct(payload_struct_, std::move(fields)));
}

/*
 * Generate the aggregation's input values
 */
void AggregateBottomTranslator::GenValuesStruct(util::RegionVector<ast::Decl *> *decls) {
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  // Create a field for every group by term
  uint32_t term_idx = 0;
  for (const auto &term : op_->GetGroupByTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(GROUP_BY_TERM_NAMES + std::to_string(term_idx));
    ast::Expr *type = codegen_->TplType(term->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field of every aggregate term.
  // Unlike the payload, these are scalar types, not aggregate types
  term_idx = 0;
  for (const auto &term : op_->GetAggregateTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(AGG_TERM_NAMES + std::to_string(term_idx));
    ast::Expr *type = codegen_->TplType(term->GetChild(0)->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Make the struct
  decls->emplace_back(codegen_->MakeStruct(values_struct_, std::move(fields)));
}

/*
 * Generate the key check logic
 */
void AggregateBottomTranslator::GenKeyCheck(FunctionBuilder *builder) {
  // Compare group by terms one by one
  // Generate if (payload.term_i )
  for (uint32_t term_idx = 0; term_idx < op_->GetGroupByTerms().size(); term_idx++) {
    ast::Expr *lhs = GetGroupByTerm(agg_payload_, term_idx);
    ast::Expr *rhs = GetGroupByTerm(agg_values_, term_idx);
    ast::Expr *cond = codegen_->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs);
    builder->StartIfStmt(cond);
    builder->Append(codegen_->ReturnStmt(codegen_->BoolLiteral(false)));
    builder->FinishBlockStmt();
  }
  builder->Append(codegen_->ReturnStmt(codegen_->BoolLiteral(true)));
}

/*
 * First declare var agg_values : AggValues
 * For each group by term, generate agg_values.term_i = group_by_term_i
 * For each aggregation expression, agg_values.expr_i = agg_expr_i
 */
void AggregateBottomTranslator::FillValues(FunctionBuilder *builder) {
  // First declare var agg_values: AggValues
  builder->Append(codegen_->DeclareVariable(agg_values_, codegen_->MakeExpr(values_struct_), nullptr));

  // Add group by terms
  uint32_t term_idx = 0;
  for (const auto &term : op_->GetGroupByTerms()) {
    // Set agg_values.term_i = group_term_i
    ast::Expr *lhs = GetGroupByTerm(agg_values_, term_idx);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(term.Get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(this);
    builder->Append(codegen_->Assign(lhs, rhs));
    term_idx++;
  }
  // Add aggregates
  term_idx = 0;
  for (const auto &term : op_->GetAggregateTerms()) {
    // Set agg_values.expr_i = agg_expr_i
    ast::Expr *lhs = GetAggTerm(agg_values_, term_idx, false);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(term->GetChild(0).Get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(this);
    builder->Append(codegen_->Assign(lhs, rhs));
    term_idx++;
  }
}

// Generate var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_ht, agg_hash_val, keyCheck, &agg_values))
void AggregateBottomTranslator::GenLookupCall(FunctionBuilder *builder) {
  // First create @aggHTLookup((&state.agg_ht, agg_hash_val, keyCheck, &agg_values)
  std::vector<ast::Expr *> lookup_args{codegen_->GetStateMemberPtr(agg_ht_), codegen_->MakeExpr(hash_val_),
                                       codegen_->MakeExpr(key_check_), codegen_->PointerTo(agg_values_)};
  ast::Expr *lookup_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableLookup, std::move(lookup_args));

  // Gen create @ptrcast(*AggPayload, ...)
  ast::Expr *cast_call = codegen_->PtrCast(payload_struct_, lookup_call);

  // Declare var agg_payload
  builder->Append(codegen_->DeclareVariable(agg_payload_, nullptr, cast_call));
}

/*
 * First check if agg_payload == nil
 * If so, set agg_payload.term_i = agg_values.term_i for each group by terms
 * Add call @aggInit(&agg_payload.expr_i) for each expression
 */
void AggregateBottomTranslator::GenConstruct(FunctionBuilder *builder) {
  // Make the if statement
  ast::Expr *nil = codegen_->NilLiteral();
  ast::Expr *payload = codegen_->MakeExpr(agg_payload_);
  ast::Expr *cond = codegen_->Compare(parsing::Token::Type::EQUAL_EQUAL, nil, payload);
  builder->StartIfStmt(cond);

  // Set agg_payload = @ptrCast(*AggPayload, @aggHTInsert(&state.agg_table, agg_hash_val))
  std::vector<ast::Expr *> insert_args{codegen_->GetStateMemberPtr(agg_ht_), codegen_->MakeExpr(hash_val_)};
  ast::Expr *insert_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableInsert, std::move(insert_args));
  ast::Expr *cast_call = codegen_->PtrCast(payload_struct_, insert_call);
  builder->Append(codegen_->Assign(codegen_->MakeExpr(agg_payload_), cast_call));

  // Set the Aggregate Keys (agg_payload.term_i = agg_value.term_i)
  for (uint32_t term_idx = 0; term_idx < op_->GetGroupByTerms().size(); term_idx++) {
    ast::Expr *lhs = GetGroupByTerm(agg_payload_, term_idx);
    ast::Expr *rhs = GetGroupByTerm(agg_values_, term_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
  // Call @aggInit(&agg_payload.expr_i) for each expression
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *init_call = codegen_->BuiltinCall(ast::Builtin::AggInit, {GetAggTerm(agg_payload_, term_idx, true)});
    builder->Append(codegen_->MakeStmt(init_call));
  }
  // Finish the if stmt
  builder->FinishBlockStmt();
}

/*
 * For each aggregate expression, call @aggAdvance(&agg_payload.expr_i, &agg_values.expr_i)
 */
void AggregateBottomTranslator::GenAdvance(FunctionBuilder *builder) {
  // Call @aggAdvance(&agg_payload.expr_i) for each expression
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *arg1 = GetAggTerm(agg_payload_, term_idx, true);
    ast::Expr *arg2 = GetAggTerm(agg_values_, term_idx, true);
    ast::Expr *advance_call = codegen_->BuiltinCall(ast::Builtin::AggAdvance, {arg1, arg2});
    builder->Append(codegen_->MakeStmt(advance_call));
  }
}

// Generate var agg_hash_val = @hash(groub_by_term1, group_by_term2, ...)
void AggregateBottomTranslator::GenHashCall(FunctionBuilder *builder) {
  // Create the @hash(group_by_term1, group_by_term2, ...) call
  std::vector<ast::Expr *> hash_args{};
  for (uint32_t term_idx = 0; term_idx < op_->GetGroupByTerms().size(); term_idx++) {
    hash_args.emplace_back(GetGroupByTerm(agg_values_, term_idx));
  }
  ast::Expr *hash_call = codegen_->BuiltinCall(ast::Builtin::Hash, std::move(hash_args));

  // Create the variable declaration
  builder->Append(codegen_->DeclareVariable(hash_val_, nullptr, hash_call));
}

void AggregateBottomTranslator::GenSingleKeyCheckFn(util::RegionVector<terrier::execution::ast::Decl *> *decls) {
  // Generate the function type (*AggPayload, *AggValues) -> bool
  // First make agg_payload: *AggPayload
  ast::Expr *payload_struct_ptr = codegen_->PointerType(payload_struct_);
  ast::FieldDecl *param1 = codegen_->MakeField(agg_payload_, payload_struct_ptr);

  // Then make agg_values: *AggValues
  ast::Expr *values_struct_ptr = codegen_->PointerType(values_struct_);
  ast::FieldDecl *param2 = codegen_->MakeField(agg_values_, values_struct_ptr);

  // Now create the function
  util::RegionVector<ast::FieldDecl *> params({param1, param2}, codegen_->Region());
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Bool);
  FunctionBuilder builder(codegen_, key_check_, std::move(params), ret_type);
  // Fill up the function
  GenKeyCheck(&builder);
  // Add it to top level declarations
  decls->emplace_back(builder.Finish());
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
  // Close the iterator after the loop ends.
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
  return bottom_->GetOutput(attr_idx);
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
  // Loop Initialization
  std::vector<ast::Expr *> init_args{codegen_->PointerTo(agg_iterator_), codegen_->GetStateMemberPtr(bottom_->agg_ht_)};
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
  // @aggHTIterGetRow(&agg_iter)
  ast::Expr *get_row_call = codegen_->OneArgCall(ast::Builtin::AggHashTableIterGetRow, agg_iterator_, true);

  // @ptrcast(*AggPayload, ...)
  ast::Expr *cast_call = codegen_->PtrCast(bottom_->payload_struct_, get_row_call);

  // Declare var agg_payload
  builder->Append(codegen_->DeclareVariable(bottom_->agg_payload_, nullptr, cast_call));
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
