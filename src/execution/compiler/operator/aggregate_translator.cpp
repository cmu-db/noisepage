#include "execution/compiler/operator/aggregate_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/seq_scan_translator.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
AggregateBottomTranslator::AggregateBottomTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen)
    : OperatorTranslator(op, codegen),
      hash_val_(codegen->NewIdentifier(hash_val_name)),
      agg_values_(codegen->NewIdentifier(agg_values_name)),
      values_struct_(codegen->NewIdentifier(values_struct_name)),
      payload_struct_(codegen->NewIdentifier(payload_struct_name)),
      agg_payload_(codegen->NewIdentifier(agg_payload_name)),
      key_check_(codegen->NewIdentifier(key_check_name)),
      agg_ht_(codegen->NewIdentifier(agg_ht_name)) {}

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
  ast::Expr *init_call = codegen_->AggHashTableInit(agg_ht_, payload_struct_);

  // Add it the setup statements
  setup_stmts->emplace_back(codegen_->MakeStmt(init_call));
}

// Call @aggHTFree
void AggregateBottomTranslator::InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) {
  ast::Expr *free_call = codegen_->AggHashTableFree(agg_ht_);
  teardown_stmts->emplace_back(codegen_->MakeStmt(free_call));
}

void AggregateBottomTranslator::Produce(FunctionBuilder *builder) { child_translator_->Produce(builder); }

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
  if (attr_idx < num_group_by_terms) {
    return GetGroupByTerm(agg_payload_, attr_idx);
  }
  // Or access an aggregate
  // Here, we need to call @aggResult(&agg_payload.expr_i)
  ast::Expr *agg_term = GetAggTerm(agg_payload_, attr_idx - num_group_by_terms, true);
  return codegen_->AggResult(agg_term);
}

ast::Expr *AggregateBottomTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx,
                                                     terrier::type::TypeId type) {
  return child_translator_->GetOutput(attr_idx);
}

ast::Expr *AggregateBottomTranslator::GetGroupByTerm(ast::Identifier object, uint32_t idx) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(group_by_term_names + std::to_string(idx));
  return codegen_->MemberExpr(object, member);
}

ast::Expr *AggregateBottomTranslator::GetAggTerm(ast::Identifier object, uint32_t idx, bool ptr) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(agg_term_names + std::to_string(idx));
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
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  // Create a field for every group by term
  uint32_t term_idx = 0;
  for (const auto &term : agg_op->GetGroupByTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(group_by_term_names + std::to_string(term_idx));
    ast::Expr *type = codegen_->TplType(term->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }
  // Set the number of group_by_terms
  num_group_by_terms = term_idx;

  // Create a field for every aggregate term
  term_idx = 0;
  for (const auto &term : agg_op->GetAggregateTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(agg_term_names + std::to_string(term_idx));
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
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  // Create a field for every group by term
  // TODO(Amadou): same as payload, so dedup code.
  uint32_t term_idx = 0;
  for (const auto &term : agg_op->GetGroupByTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(group_by_term_names + std::to_string(term_idx));
    ast::Expr *type = codegen_->TplType(term->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(field_name, type));
    term_idx++;
  }

  // Create a field of every aggregate term.
  // Unlike the payload, these are scalar types, not aggregate types
  term_idx = 0;
  for (const auto &term : agg_op->GetAggregateTerms()) {
    ast::Identifier field_name = codegen_->Context()->GetIdentifier(agg_term_names + std::to_string(term_idx));
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
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  // Compare group by terms one by one
  // Generate if (payload.term_i )
  for (uint32_t term_idx = 0; term_idx < agg_op->GetGroupByTerms().size(); term_idx++) {
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
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  for (const auto &term : agg_op->GetGroupByTerms()) {
    // Set agg_values.term_i = group_term_i
    ast::Expr *lhs = GetGroupByTerm(agg_values_, term_idx);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(term.get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(this);
    builder->Append(codegen_->Assign(lhs, rhs));
    term_idx++;
  }
  // Add aggregates
  term_idx = 0;
  for (const auto &term : agg_op->GetAggregateTerms()) {
    // Set agg_values.expr_i = agg_expr_i
    ast::Expr *lhs = GetAggTerm(agg_values_, term_idx, false);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(term->GetChild(0).get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(this);
    builder->Append(codegen_->Assign(lhs, rhs));
    term_idx++;
  }
}

// Generate var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_ht, agg_hash_val, keyCheck, &agg_values))
void AggregateBottomTranslator::GenLookupCall(FunctionBuilder *builder) {
  // First create @aggHTLookup((&state.agg_ht, agg_hash_val, keyCheck, &agg_values)
  ast::Expr *lookup_call = codegen_->AggHashTableLookup(agg_ht_, hash_val_, key_check_, agg_values_);

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
  ast::Expr *insert_call = codegen_->AggHashTableInsert(agg_ht_, hash_val_);
  ast::Expr *cast_call = codegen_->PtrCast(payload_struct_, insert_call);
  builder->Append(codegen_->Assign(codegen_->MakeExpr(agg_payload_), cast_call));

  // Set the Aggregate Keys (agg_payload.term_i = agg_value.term_i)
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  for (uint32_t term_idx = 0; term_idx < agg_op->GetGroupByTerms().size(); term_idx++) {
    ast::Expr *lhs = GetGroupByTerm(agg_payload_, term_idx);
    ast::Expr *rhs = GetGroupByTerm(agg_values_, term_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
  // Call @aggInit(&agg_payload.expr_i) for each expression
  for (uint32_t term_idx = 0; term_idx < agg_op->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *init_call = codegen_->AggInit(GetAggTerm(agg_payload_, term_idx, true));
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
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  for (uint32_t term_idx = 0; term_idx < agg_op->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *arg1 = GetAggTerm(agg_payload_, term_idx, true);
    ast::Expr *arg2 = GetAggTerm(agg_values_, term_idx, true);
    ast::Expr *advance_call = codegen_->AggAdvance(arg1, arg2);
    builder->Append(codegen_->MakeStmt(advance_call));
  }
}

// Generate var agg_hash_val = @hash(groub_by_term1, group_by_term2, ...)
void AggregateBottomTranslator::GenHashCall(FunctionBuilder *builder) {
  // Create the @hash(group_by_term1, group_by_term2, ...) call
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  util::RegionVector<ast::Expr *> hash_args{codegen_->Region()};
  for (uint32_t term_idx = 0; term_idx < agg_op->GetGroupByTerms().size(); term_idx++) {
    hash_args.emplace_back(GetGroupByTerm(agg_values_, term_idx));
  }
  ast::Expr *hash_call = codegen_->Hash(std::move(hash_args));

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

ast::Expr *AggregateTopTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  return bottom_->GetOutput(attr_idx);
}

// Let the bottom translator handle this call
ast::Expr *AggregateTopTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  auto translator = TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
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
  ast::Expr *init_call = codegen_->AggHashTableIterInit(agg_iterator_, bottom_->agg_ht_);
  ast::Stmt *loop_init = codegen_->MakeStmt(init_call);
  // Loop condition
  ast::Expr *has_next_call = codegen_->AggHashTableIterHasNext(agg_iterator_);
  // Loop update
  ast::Expr *next_call = codegen_->AggHashTableIterNext(agg_iterator_);
  ast::Stmt *loop_update = codegen_->MakeStmt(next_call);
  // Make the loop
  builder->StartForStmt(loop_init, has_next_call, loop_update);
};

// Declare var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
void AggregateTopTranslator::DeclareResult(FunctionBuilder *builder) {
  // @aggHTIterGetRow(agg_iter)
  ast::Expr *get_row_call = codegen_->AggHashTableIterGetRow(agg_iterator_);

  // @ptrcast(*AggPayload, ...)
  ast::Expr *cast_call = codegen_->PtrCast(bottom_->payload_struct_, get_row_call);

  // Declare var agg_payload
  builder->Append(codegen_->DeclareVariable(bottom_->agg_payload_, nullptr, cast_call));
}

void AggregateTopTranslator::CloseIterator(FunctionBuilder *builder) {
  // Call @aggHTIterCLose(agg_iter)
  ast::Expr *close_call = codegen_->AggHashTableIterClose(agg_iterator_);
  builder->Append(codegen_->MakeStmt(close_call));
}

bool AggregateTopTranslator::GenHaving(execution::compiler::FunctionBuilder *builder) {
  auto agg_op = dynamic_cast<const terrier::planner::AggregatePlanNode *>(op_);
  if (agg_op->GetHavingClausePredicate() != nullptr) {
    auto predicate = agg_op->GetHavingClausePredicate().get();
    auto translator = TranslatorFactory::CreateExpressionTranslator(predicate, codegen_);
    ast::Expr *cond = translator->DeriveExpr(this);
    builder->StartIfStmt(cond);
    return true;
  }
  return false;
}

}  // namespace terrier::execution::compiler
