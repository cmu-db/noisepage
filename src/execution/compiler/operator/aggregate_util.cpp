#include "execution/compiler/operator/aggregate_util.h"
#include <utility>
#include <vector>
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {

AggregateHelper::AggregateHelper(CodeGen *codegen, const planner::AggregatePlanNode *op)
    : codegen_(codegen),
      op_(op),
      global_info_(codegen),
      agg_values_(codegen->NewIdentifier("agg_values")),
      values_struct_(codegen->NewIdentifier("ValuesStruct")) {
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    group_bys_.emplace_back(codegen->NewIdentifier("group_by"));
  }
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    aggregates_.emplace_back(codegen->NewIdentifier("aggregate"));
    if (op_->GetAggregateTerms()[i]->IsDistinct()) {
      distinct_aggs_.emplace(i, std::make_unique<AHTInfo>(codegen, true, i));
    }
  }
}

void AggregateHelper::InitAHTs(util::RegionVector<ast::Stmt *> *stmts) {
  // Function to initialize an arbitary hash table.
  auto init_ht = [&](const AHTInfo *info) {
    ast::Expr *ht_init_call = codegen_->HTInitCall(ast::Builtin::AggHashTableInit, info->HT(), info->StructType());
    stmts->emplace_back(codegen_->MakeStmt(ht_init_call));
  };

  // Initialize the global hash table.
  if (!op_->GetGroupByTerms().empty()) init_ht(&global_info_);
  // Then initialize each distinct hash table.
  for (const auto &distinct_info : distinct_aggs_) {
    init_ht(distinct_info.second.get());
  }
}

void AggregateHelper::FreeAHTs(util::RegionVector<ast::Stmt *> *stmts) {
  // Function to free an arbitary hash table.
  auto free_ht = [&](const AHTInfo *info) {
    ast::Expr *ht_free_call = codegen_->OneArgStateCall(ast::Builtin::AggHashTableFree, info->HT());
    stmts->emplace_back(codegen_->MakeStmt(ht_free_call));
  };

  // Free the global hash table.
  if (!op_->GetGroupByTerms().empty()) free_ht(&global_info_);
  // Then Free each distinct hash table.
  for (const auto &distinct_info : distinct_aggs_) {
    free_ht(distinct_info.second.get());
  }
}

void AggregateHelper::DeclareAHTs(util::RegionVector<ast::FieldDecl *> *fields) {
  // Function to add an arbitrary hash table.
  auto gen_field = [&](const AHTInfo *info) {
    ast::Expr *ht_type = codegen_->BuiltinType(ast::BuiltinType::Kind::AggregationHashTable);
    fields->emplace_back(codegen_->MakeField(info->HT(), ht_type));
  };

  // Add the global hash table.
  if (!op_->GetGroupByTerms().empty()) gen_field(&global_info_);
  // Add each distinct hash table.
  for (const auto &distinct_info : distinct_aggs_) {
    gen_field(distinct_info.second.get());
  }
}

void AggregateHelper::GenAHTStructs(ast::StructDecl **global_decl, util::RegionVector<ast::Decl *> *decls) {
  // Generic function to add a struct
  auto gen_struct = [&](const AHTInfo *info) {
    // First make the fields
    util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
    // All hash tables need the groub by terms.
    for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
      auto term = op_->GetGroupByTerms()[i];
      ast::Expr *gby_type = codegen_->TplType(term->GetReturnValueType());
      fields.emplace_back(codegen_->MakeField(group_bys_[i], gby_type));
    }
    if (info->IsDistinct()) {
      // Distinct hash tables only need the distinct aggregate values.
      auto agg = op_->GetAggregateTerms()[info->TermIdx()];
      ast::Expr *distinct_type = codegen_->TplType(agg->GetChild(0)->GetReturnValueType());
      fields.emplace_back(codegen_->MakeField(aggregates_[info->TermIdx()], distinct_type));
    } else {
      // The global hash table needs all aggregators.
      for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
        auto agg = op_->GetAggregateTerms()[term_idx];
        ast::Expr *agg_type = codegen_->AggregateType(agg->GetExpressionType(), agg->GetChild(0)->GetReturnValueType());
        fields.emplace_back(codegen_->MakeField(aggregates_[term_idx], agg_type));
      }
    }
    // Then declare the struct
    decls->emplace_back(codegen_->MakeStruct(info->StructType(), std::move(fields)));
  };

  // Add the global hash table's struct.
  if (!op_->GetGroupByTerms().empty()) {
    gen_struct(&global_info_);
    if (global_decl != nullptr) {
      auto *decl = decls->back();
      TERRIER_ASSERT(ast::StructDecl::classof(decl), "Expected StructDecl");
      *global_decl = reinterpret_cast<ast::StructDecl *>(decl);
    }
  }

  // Add each distinct hash table's struct.
  for (const auto &distinct_info : distinct_aggs_) {
    gen_struct(distinct_info.second.get());
  }
}

void AggregateHelper::GenValuesStruct(util::RegionVector<ast::Decl *> *decls) {
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  // Create a field for every group by term
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    auto gby = op_->GetGroupByTerms()[i];
    ast::Expr *type = codegen_->TplType(gby->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(group_bys_[i], type));
  }

  // Create a field of every aggregate term.
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    auto agg = op_->GetAggregateTerms()[i];
    ast::Expr *type = codegen_->TplType(agg->GetChild(0)->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(aggregates_[i], type));
  }

  // Make the struct
  decls->emplace_back(codegen_->MakeStruct(values_struct_, std::move(fields)));
}

void AggregateHelper::GenHashCalls(FunctionBuilder *builder) {
  // Generic function to make a hash call.
  auto make_hash_call = [&](const AHTInfo *info) {
    // First add group by terms
    std::vector<ast::Expr *> hash_args{};
    for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
      hash_args.emplace_back(codegen_->MemberExpr(agg_values_, group_bys_[i]));
    }
    if (info->IsDistinct()) {
      // For distinct aggregates, we also need to add the distinct term
      hash_args.emplace_back(codegen_->MemberExpr(agg_values_, aggregates_[info->TermIdx()]));
    }
    // Make the hash call.
    ast::Expr *hash_call = codegen_->BuiltinCall(ast::Builtin::Hash, std::move(hash_args));
    builder->Append(codegen_->DeclareVariable(info->HashVal(), nullptr, hash_call));
  };

  // Make the hash call for the global hash table.
  if (!op_->GetGroupByTerms().empty()) make_hash_call(&global_info_);
  // Make the hash call for each distinct table.
  for (const auto &distinct_info : distinct_aggs_) {
    make_hash_call(distinct_info.second.get());
  }
}

void AggregateHelper::GenConstruct(FunctionBuilder *builder) {
  auto construct_entry = [&](const AHTInfo *info) {
    // For lookup the entry in the table.
    {
      std::vector<ast::Expr *> lookup_args{codegen_->GetStateMemberPtr(info->HT()), codegen_->MakeExpr(info->HashVal()),
                                           codegen_->MakeExpr(info->KeyCheck()), codegen_->PointerTo(agg_values_)};
      ast::Expr *lookup_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableLookup, std::move(lookup_args));
      ast::Expr *cast_call = codegen_->PtrCast(info->StructType(), lookup_call);
      builder->Append(codegen_->DeclareVariable(info->Entry(), nullptr, cast_call));
    }
    // Then check if the returned entry is null.
    {
      ast::Expr *nil = codegen_->NilLiteral();
      ast::Expr *payload = codegen_->MakeExpr(info->Entry());
      ast::Expr *cond = codegen_->Compare(parsing::Token::Type::EQUAL_EQUAL, nil, payload);
      builder->StartIfStmt(cond);
    }
    // If it is null, then insert a new entry into the table.
    {
      std::vector<ast::Expr *> insert_args{codegen_->GetStateMemberPtr(info->HT()),
                                           codegen_->MakeExpr(info->HashVal())};
      ast::Expr *insert_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableInsert, std::move(insert_args));
      auto cast_call = codegen_->PtrCast(info->StructType(), insert_call);
      builder->Append(codegen_->Assign(codegen_->MakeExpr(info->Entry()), cast_call));
      // Initialize the group by values.
      InitGroupByValues(builder, info);
    }

    if (info->IsDistinct()) {
      // For distinct aggregates, we advance only upon a new insertion (inside the if statement).
      AdvanceDistinct(builder, info);
    } else {
      // For the global hash table, we initialize the payload's aggregates.
      InitGlobalAggregates(builder);
    }
    // Close the if statement
    builder->FinishBlockStmt();
  };

  // Construct the entry of the global hash table.
  if (!op_->GetGroupByTerms().empty()) construct_entry(&global_info_);
  // Make the hash call for each distinct table.
  for (const auto &distinct_info : distinct_aggs_) {
    construct_entry(distinct_info.second.get());
  }
}

void AggregateHelper::GenKeyChecks(util::RegionVector<ast::Decl *> *decls) {
  auto gen_key_check = [&](const AHTInfo *info) {
    // Create a function (entry: *StructType, values: *ValuesStruct) -> bool
    ast::Expr *struct_ptr = codegen_->PointerType(info->StructType());
    ast::FieldDecl *param1 = codegen_->MakeField(info->Entry(), struct_ptr);
    // Then make agg_values: *AggValues
    ast::Expr *values_struct_ptr = codegen_->PointerType(values_struct_);
    ast::FieldDecl *param2 = codegen_->MakeField(agg_values_, values_struct_ptr);
    // Now create the function
    util::RegionVector<ast::FieldDecl *> params({param1, param2}, codegen_->Region());
    ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Bool);
    FunctionBuilder builder(codegen_, info->KeyCheck(), std::move(params), ret_type);
    // Compare the groub by terms
    for (uint32_t term_idx = 0; term_idx < op_->GetGroupByTerms().size(); term_idx++) {
      ast::Expr *lhs = codegen_->MemberExpr(info->Entry(), group_bys_[term_idx]);
      ast::Expr *rhs = codegen_->MemberExpr(agg_values_, group_bys_[term_idx]);
      ast::Expr *cond = codegen_->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs);
      builder.StartIfStmt(cond);
      builder.Append(codegen_->ReturnStmt(codegen_->BoolLiteral(false)));
      builder.FinishBlockStmt();
    }
    // For distinct aggregates, also compare the distinct terms.
    if (info->IsDistinct()) {
      ast::Expr *lhs = codegen_->MemberExpr(info->Entry(), aggregates_[info->TermIdx()]);
      ast::Expr *rhs = codegen_->MemberExpr(agg_values_, aggregates_[info->TermIdx()]);
      ast::Expr *comp = codegen_->Compare(parsing::Token::Type::EQUAL_EQUAL, lhs, rhs);
      builder.Append(codegen_->ReturnStmt(codegen_->OneArgCall(ast::Builtin::SqlToBool, comp)));
    } else {
      // For non distinct aggregates, just return true.
      builder.Append(codegen_->ReturnStmt(codegen_->BoolLiteral(true)));
    }
    decls->emplace_back(builder.Finish());
  };

  // Make the key check function for the global hash table.
  if (!op_->GetGroupByTerms().empty()) gen_key_check(&global_info_);
  // Make the key check function for each distinct table.
  for (const auto &distinct_info : distinct_aggs_) {
    gen_key_check(distinct_info.second.get());
  }
}

void AggregateHelper::GenAdvanceNonDistinct(FunctionBuilder *builder) {
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    auto term = op_->GetAggregateTerms()[term_idx];
    if (!term->IsDistinct()) {
      ast::Expr *arg1;
      if (!op_->GetGroupByTerms().empty()) {
        arg1 = codegen_->PointerTo(codegen_->MemberExpr(global_info_.Entry(), aggregates_[term_idx]));
      } else {
        arg1 = codegen_->GetStateMemberPtr(aggregates_[term_idx]);
      }
      ast::Expr *arg2 = codegen_->PointerTo(codegen_->MemberExpr(agg_values_, aggregates_[term_idx]));
      ast::Expr *advance_call = codegen_->BuiltinCall(ast::Builtin::AggAdvance, {arg1, arg2});
      builder->Append(codegen_->MakeStmt(advance_call));
    }
  }
}

void AggregateHelper::FillValues(FunctionBuilder *builder, OperatorTranslator *translator) {
  // First declare var agg_values: AggValues
  builder->Append(codegen_->DeclareVariable(agg_values_, codegen_->MakeExpr(values_struct_), nullptr));

  // Add group by terms
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    auto gby = op_->GetGroupByTerms()[i];
    ast::Expr *lhs = codegen_->MemberExpr(agg_values_, group_bys_[i]);
    auto expr_translator = TranslatorFactory::CreateExpressionTranslator(gby.Get(), codegen_);
    ast::Expr *rhs = expr_translator->DeriveExpr(translator);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
  // Add aggregates
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    auto agg_term = op_->GetAggregateTerms()[i];
    ast::Expr *lhs = codegen_->MemberExpr(agg_values_, aggregates_[i]);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(agg_term->GetChild(0).Get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(translator);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void AggregateHelper::InitGroupByValues(FunctionBuilder *builder, const AHTInfo *info) {
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    ast::Expr *lhs = codegen_->MemberExpr(info->Entry(), group_bys_[i]);
    ast::Expr *rhs = codegen_->MemberExpr(agg_values_, group_bys_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void AggregateHelper::AdvanceDistinct(FunctionBuilder *builder, const AHTInfo *info) {
  TERRIER_ASSERT(info->IsDistinct(), "Should not be called with non distinct aggregates!");
  // Set the distinct term in the hash table entry
  ast::Expr *lhs = codegen_->MemberExpr(info->Entry(), aggregates_[info->TermIdx()]);
  ast::Expr *rhs = codegen_->MemberExpr(agg_values_, aggregates_[info->TermIdx()]);
  builder->Append(codegen_->Assign(lhs, rhs));

  // The advance the global aggregate.
  ast::Expr *arg1;
  if (!op_->GetGroupByTerms().empty()) {
    arg1 = codegen_->PointerTo(codegen_->MemberExpr(global_info_.Entry(), aggregates_[info->TermIdx()]));
  } else {
    arg1 = codegen_->GetStateMemberPtr(aggregates_[info->TermIdx()]);
  }
  ast::Expr *arg2 = codegen_->PointerTo(codegen_->MemberExpr(agg_values_, aggregates_[info->TermIdx()]));
  ast::Expr *advance_call = codegen_->BuiltinCall(ast::Builtin::AggAdvance, {arg1, arg2});
  builder->Append(codegen_->MakeStmt(advance_call));
}

void AggregateHelper::InitGlobalAggregates(FunctionBuilder *builder) {
  // Call @aggInit(&agg_payload.expr_i) for each expression
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    ast::Expr *agg = codegen_->MemberExpr(global_info_.Entry(), aggregates_[term_idx]);
    ast::Expr *init_call = codegen_->OneArgCall(ast::Builtin::AggInit, codegen_->PointerTo(agg));
    builder->Append(codegen_->MakeStmt(init_call));
  }
}

}  // namespace terrier::execution::compiler
