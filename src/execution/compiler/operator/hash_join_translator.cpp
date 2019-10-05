#include "execution/compiler/operator/hash_join_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/hash_join_plan_node.h"

namespace terrier::execution::compiler {
HashJoinLeftTranslator::HashJoinLeftTranslator(const terrier::planner::HashJoinPlanNode *op,
                                               execution::compiler::CodeGen *codegen)
    : OperatorTranslator(codegen),
      op_(op),
      hash_val_{codegen->NewIdentifier(hash_val_name_)},
      build_struct_{codegen->NewIdentifier(build_struct_name_)},
      build_row_{codegen->NewIdentifier(build_row_name_)},
      join_ht_{codegen->NewIdentifier(join_ht_name_)} {}

void HashJoinLeftTranslator::Produce(FunctionBuilder *builder) {
  // Produce the rest of the pipeline
  child_translator_->Produce(builder);
  // Call @joinHTBuild at the end of the pipeline
  GenBuildCall(builder);
}

void HashJoinLeftTranslator::Consume(FunctionBuilder *builder) {
  // First create the left hash_value.
  GenHashCall(builder);
  // Then call insert
  GenHTInsert(builder);
  // Fill up the build row
  FillBuildRow(builder);
}

// Declare the hash table
void HashJoinLeftTranslator::InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) {
  // join_hash_table : JoinHashTable
  ast::Expr *ht_type = codegen_->BuiltinType(ast::BuiltinType::Kind::JoinHashTable);
  state_fields->emplace_back(codegen_->MakeField(join_ht_, ht_type));
}

// Declare the join build struct
void HashJoinLeftTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  // Add child output columns
  GetChildOutputFields(&fields, left_attr_name_);
  // Make the struct
  decls->emplace_back(codegen_->MakeStruct(build_struct_, std::move(fields)));
}

// Call @joinHTInit on the hash table
void HashJoinLeftTranslator::InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) {
  // @joinHTInit(&state.join_table, @execCtxGetMem(execCtx), @sizeOf(BuildRow))
  ast::Expr *init_call = codegen_->JoinHashTableInit(join_ht_, build_struct_);
  // Add it the setup statements
  setup_stmts->emplace_back(codegen_->MakeStmt(init_call));
}

// Call @joinHTFree on the hash table
void HashJoinLeftTranslator::InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) {
  ast::Expr *free_call = codegen_->JoinHashTableFree(join_ht_);
  teardown_stmts->emplace_back(codegen_->MakeStmt(free_call));
}

// Call @joinHTBuild(&state.join_hash_table)
void HashJoinLeftTranslator::GenBuildCall(FunctionBuilder *builder) {
  ast::Expr *build_call = codegen_->JoinHashTableBuild(join_ht_);
  builder->Append(codegen_->MakeStmt(build_call));
}

// Declare var hash_val = @hash(join_keys)
void HashJoinLeftTranslator::GenHashCall(FunctionBuilder *builder) {
  // First create @hash(join_key1, join_key2, ...)
  util::RegionVector<ast::Expr *> hash_args{codegen_->Region()};
  for (const auto &key : op_->GetLeftHashKeys()) {
    std::unique_ptr<ExpressionTranslator> key_translator =
        TranslatorFactory::CreateExpressionTranslator(key.get(), codegen_);
    hash_args.emplace_back(key_translator->DeriveExpr(this));
  }
  ast::Expr *hash_call = codegen_->Hash(std::move(hash_args));

  // Create the variable declaration
  builder->Append(codegen_->DeclareVariable(hash_val_, nullptr, hash_call));
}

// var build_row = @ptrCast(*BuildRow, @joinHTInsert(&state.join_table, hash_val))
void HashJoinLeftTranslator::GenHTInsert(FunctionBuilder *builder) {
  // First create @joinHTInsert(&state.join_table, hash_val)
  ast::Expr *insert_call = codegen_->JoinHashTableInsert(join_ht_, hash_val_);

  // Gen create @ptrcast(*BuildRow, ...)
  ast::Expr *cast_call = codegen_->PtrCast(build_struct_, insert_call);

  // Declare var build_row
  builder->Append(codegen_->DeclareVariable(build_row_, nullptr, cast_call));
}

// Fill up the build row
void HashJoinLeftTranslator::FillBuildRow(FunctionBuilder *builder) {
  // For each child output, set the build attribute
  for (uint32_t attr_idx = 0; attr_idx < op_->GetChild(0)->GetOutputSchema()->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetBuildValue(attr_idx);
    ast::Expr *rhs = child_translator_->GetOutput(attr_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

ast::Expr *HashJoinLeftTranslator::GetBuildValue(uint32_t idx) {
  ast::Identifier member = codegen_->Context()->GetIdentifier(left_attr_name_ + std::to_string(idx));
  return codegen_->MemberExpr(build_row_, member);
}

ast::Expr *HashJoinLeftTranslator::GetOutput(uint32_t attr_idx) { return GetBuildValue(attr_idx); }

ast::Expr *HashJoinLeftTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  return child_translator_->GetOutput(attr_idx);
}

HashJoinRightTranslator::HashJoinRightTranslator(const terrier::planner::HashJoinPlanNode *op,
                                                 execution::compiler::CodeGen *codegen,
                                                 execution::compiler::OperatorTranslator *left)
    : OperatorTranslator{codegen},
      op_(op),
      left_(dynamic_cast<HashJoinLeftTranslator *>(left)),
      hash_val_{codegen->NewIdentifier(hash_val_name_)},
      probe_struct_{codegen->NewIdentifier(probe_struct_name_)},
      probe_row_{codegen->NewIdentifier(probe_row_name_)},
      key_check_{codegen->NewIdentifier(key_check_name_)},
      join_iter_{codegen->NewIdentifier(iterator_name_)} {}

void HashJoinRightTranslator::Produce(FunctionBuilder *builder) {
  // Declare the iterator
  DeclareIterator(builder);
  // Let right child produce its code
  child_translator_->Produce(builder);
  // Close iterator
  GenIteratorClose(builder);
}

void HashJoinRightTranslator::Consume(FunctionBuilder *builder) {
  // Create the right hash_value
  GenHashValue(builder);
  // Materialize the probe tuple if necessary.
  if (!is_child_materializer_) {
    FillProbeRow(builder);
  }
  // Generate the probe loop
  GenProbeLoop(builder);
  // Get the matching tuple
  DeclareMatch(builder);
  // Let the parent consume
  parent_translator_->Consume(builder);
  // Close Loop
  builder->FinishBlockStmt();
}

ast::Expr *HashJoinRightTranslator::GetOutput(uint32_t attr_idx) {
  auto output_expr = op_->GetOutputSchema()->GetColumn(attr_idx).GetExpr();
  std::unique_ptr<ExpressionTranslator> translator =
      TranslatorFactory::CreateExpressionTranslator(output_expr, codegen_);
  return translator->DeriveExpr(this);
}

ast::Expr *HashJoinRightTranslator::GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) {
  TERRIER_ASSERT(child_idx <= 1, "A hash join can only have two children.");
  // For the left child, just get the output at the given index
  if (child_idx == 0) {
    return left_->GetOutput(attr_idx);
  }
  // Other get the output from the probe row.
  return GetProbeValue(attr_idx);
}

ast::Expr *HashJoinRightTranslator::GetProbeValue(uint32_t idx) {
  // If the right child is a materializer, get its output.
  if (is_child_materializer_) {
    return child_translator_->GetOutput(idx);
  }
  // Otherwise get the attribute from the probe row.
  ast::Identifier member = codegen_->Context()->GetIdentifier(right_attr_name_ + std::to_string(idx));
  return codegen_->MemberExpr(probe_row_, member);
}

// Make the probe struct if necessary.
void HashJoinRightTranslator::InitializeStructs(util::RegionVector<ast::Decl *> *decls) {
  // If the child already materialized it's tuple, do nothing
  if (is_child_materializer_) return;

  // Otherwise let the struct have a field for each right child attribute.
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  GetChildOutputFields(&fields, right_attr_name_);
  decls->emplace_back(codegen_->MakeStruct(probe_struct_, std::move(fields)));
}

// Declare a function that checks if the join predicate is true
void HashJoinRightTranslator::InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) {
  // Generate the function type (*State, *ProbeRow, *BuildRow) -> bool
  // State paramater
  ast::Identifier state_variable = codegen_->GetStateVar();
  ast::Expr *state_type = codegen_->PointerType(codegen_->GetStateType());
  ast::FieldDecl *param1 = codegen_->MakeField(state_variable, state_type);

  // Then make probe_row: *ProbeRow depending on whether the previous operator is a materializer.
  ast::FieldDecl *param2;
  is_child_materializer_ = child_translator_->IsMaterializer(&is_child_ptr_);
  if (is_child_materializer_) {
    // Use the previous tuple's name and type
    auto prev_tuple = child_translator_->GetMaterializedTuple();
    ast::Expr *prev_type_ptr = codegen_->PointerType(*prev_tuple.second);
    param2 = codegen_->MakeField(*prev_tuple.first, prev_type_ptr);
  } else {
    // Use the newly declared probe type.
    ast::Expr *probe_struct_ptr = codegen_->PointerType(probe_struct_);
    param2 = codegen_->MakeField(probe_row_, probe_struct_ptr);
  }

  // Then make build_row: *BuildRow
  ast::Expr *build_struct_ptr = codegen_->PointerType(left_->build_struct_);
  ast::FieldDecl *param3 = codegen_->MakeField(left_->build_row_, build_struct_ptr);

  // Now create the function
  util::RegionVector<ast::FieldDecl *> params({param1, param2, param3}, codegen_->Region());
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Bool);

  FunctionBuilder builder(codegen_, key_check_, std::move(params), ret_type);
  // Fill up the function
  GenKeyCheck(&builder);
  // Add it to top level declarations
  decls->emplace_back(builder.Finish());
}

void HashJoinRightTranslator::GenKeyCheck(FunctionBuilder *builder) {
  if (op_->GetJoinPredicate() != nullptr) {
    // Case 1: There is a join predicate
    // Generated code: if (predicate) return true; else return false
    auto pred_translator = TranslatorFactory::CreateExpressionTranslator(op_->GetJoinPredicate().get(), codegen_);
    builder->StartIfStmt(pred_translator->DeriveExpr(this));
    builder->Append(codegen_->ReturnStmt(codegen_->BoolLiteral(true)));
    builder->FinishBlockStmt();
    builder->Append(codegen_->ReturnStmt(codegen_->BoolLiteral(false)));
  } else {
    // Case 2: There is no join predicate
    // Just return true
    builder->Append(codegen_->ReturnStmt(codegen_->BoolLiteral(true)));
  }
}

// Set var hash_val = @hash(right_join_keys)
void HashJoinRightTranslator::GenHashValue(FunctionBuilder *builder) {
  // First create @hash(join_key1, join_key2, ...)
  util::RegionVector<ast::Expr *> hash_args{codegen_->Region()};
  for (const auto &key : op_->GetRightHashKeys()) {
    std::unique_ptr<ExpressionTranslator> key_translator =
        TranslatorFactory::CreateExpressionTranslator(key.get(), codegen_);
    hash_args.emplace_back(key_translator->DeriveExpr(this));
  }
  ast::Expr *hash_call = codegen_->Hash(std::move(hash_args));

  // Create the variable declaration
  builder->Append(codegen_->DeclareVariable(hash_val_, nullptr, hash_call));
}

void HashJoinRightTranslator::FillProbeRow(FunctionBuilder *builder) {
  // Fill the ProbeRow.
  for (uint32_t attr_idx = 0; attr_idx < op_->GetChild(1)->GetOutputSchema()->GetColumns().size(); attr_idx++) {
    ast::Expr *lhs = GetProbeValue(attr_idx);
    ast::Expr *rhs = child_translator_->GetOutput(attr_idx);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void HashJoinRightTranslator::DeclareIterator(FunctionBuilder *builder) {
  ast::Expr *iter_type = codegen_->BuiltinType(ast::BuiltinType::Kind::JoinHashTableIterator);
  builder->Append(codegen_->DeclareVariable(join_iter_, iter_type, nullptr));
}

// Loop to probe the hash table
void HashJoinRightTranslator::GenProbeLoop(FunctionBuilder *builder) {
  // for (@joinHTIterInit(&hti, &state.join_table, hash_val);
  //      @joinHTIterHasNext(&hti, checkJoinKey, execCtx, &lineitem_row);) {...}
  ast::Expr *init_call = codegen_->JoinHashTableIterInit(join_iter_, left_->join_ht_, hash_val_);
  ast::Stmt *loop_init = codegen_->MakeStmt(init_call);
  // Loop condition
  ast::Expr *has_next_call;
  if (is_child_materializer_) {
    auto child_tuple = child_translator_->GetMaterializedTuple();
    has_next_call = codegen_->JoinHashTableIterHasNext(join_iter_, key_check_, *child_tuple.first, is_child_ptr_);
  } else {
    has_next_call = codegen_->JoinHashTableIterHasNext(join_iter_, key_check_, probe_row_, false);
  }
  // Make the loop
  builder->StartForStmt(loop_init, has_next_call, nullptr);
}

// Call @joinHTIterCLose(&join_iter)
void HashJoinRightTranslator::GenIteratorClose(FunctionBuilder *builder) {
  ast::Expr *close_call = codegen_->JoinHashTableIterClose(join_iter_);
  builder->Append(codegen_->MakeStmt(close_call));
}

// var build_row = @ptrCast(*BuildRow, @joinHTIterGetRow(&join_iter))
void HashJoinRightTranslator::DeclareMatch(FunctionBuilder *builder) {
  ast::Expr *get_row_call = codegen_->JoinHashTableIterGetRow(join_iter_);

  // Gen create @ptrcast(*BuildRow, ...)
  ast::Expr *cast_call = codegen_->PtrCast(left_->build_struct_, get_row_call);

  // Declare var build_row
  builder->Append(codegen_->DeclareVariable(left_->build_row_, nullptr, cast_call));
}

}  // namespace terrier::execution::compiler
