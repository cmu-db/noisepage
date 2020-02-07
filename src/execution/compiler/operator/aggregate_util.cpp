#include "execution/compiler/operator/aggregate_util.h"
#include <utility>
#include <vector>
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"

namespace terrier::execution::compiler {
AggregateHelper::AggregateHelper(CodeGen *codegen,
                                 const planner::AggregatePlanNode *op)
 : codegen_(codegen)
 , op_(op)
 , values_struct_(codegen->NewIdentifier("ValuesStruct"))
 , agg_values_(codegen->NewIdentifier("agg_values")),
 , hash_val_(codegen->NewIdentifier("hash_val"))
 {

  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    group_bys_.emplace_back(codegen->NewIdentifier("group_by"))
  }
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    aggregates_.emplace_back(codegen->NewIdentifier("aggregate"));
    if (op_->GetAggregateTerms()[i]->IsDistinct()) {
      distinct_aggs_.emplace(i, std::make_unique<DistinctInfo>(codegen));
    }
  }
}

void AggregateHelper::InitDistinctTables(util::RegionVector<ast::Stmt *> *stmts) {
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    auto term = op_->GetAggregateTerms()[i];
    if (term->IsDistinct()) {
      const auto& distinct_agg = distinct_aggs_[i];
      ast::Expr *ht_init_call =
          codegen_->HTInitCall(ast::Builtin::AggHashTableInit, distinct_agg->HT(), distinct_agg->StructType());
      stmts->emplace_back(codegen_->MakeStmt(ht_init_call));
    }
  }
}

void AggregateHelper::FreeDistinctTables(util::RegionVector<ast::Stmt *> *stmts) {
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    auto term = op_->GetAggregateTerms()[i];
    if (term->IsDistinct()) {
      const auto& distinct_agg = distinct_aggs_[i];
      auto ht_free_call = codegen_->OneArgStateCall(ast::Builtin::AggHashTableFree, distinct_agg->HT());
      stmts->emplace_back(codegen_->MakeStmt(ht_free_call));
    }
  }
}


void AggregateHelper::GenDistinctStateFields(util::RegionVector<ast::FieldDecl *> *fields) {
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    auto term = op_->GetAggregateTerms()[term_idx];
    ast::Expr *agg_type = codegen_->AggregateType(term->GetExpressionType(), term->GetChild(0)->GetReturnValueType());
    fields->emplace_back(codegen_->MakeField(aggregates_[term_idx], agg_type));
    if (term->IsDistinct()) {
      auto &distinct_agg = distinct_aggs_[term_idx];
      ast::Expr *ht_type = codegen_->BuiltinType(ast::BuiltinType::Kind::AggregationHashTable);
      fields->emplace_back(codegen_->MakeField(distinct_agg->HT(), ht_type));
    }
  }
}

void AggregateHelper::GenHTStruct(util::RegionVector<ast::Decl *> *decls, const ast::Identifier& struct_name) {
  util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
  // Add the group by terms
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    auto term = op_->GetGroupByTerms()[i];
    ast::Expr *gby_type = codegen_->TplType(term->GetReturnValueType());
    fields.emplace_back(codegen_->MakeField(group_bys_[i], gby_type));
  }

  // Make the struct
  decls->emplace_back(codegen_->MakeStruct(struct_name, std::move(fields)));
}

void AggregateHelper::GenDistinctStructs(util::RegionVector<ast::Decl *> *decls) {
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    auto term = op_->GetAggregateTerms()[i];
    if (term->IsDistinct()) {
      util::RegionVector<ast::FieldDecl *> fields{codegen_->Region()};
      // Add the group by terms
      for (uint32_t j = 0; j < op_->GetGroupByTerms().size(); j++) {
        auto gby = op_->GetGroupByTerms()[j];
        ast::Expr *gby_type = codegen_->TplType(gby->GetReturnValueType());
        fields.emplace_back(codegen_->MakeField(group_bys_[j], gby_type));
      }
      // Add the distinct aggregate
      auto &distinct_agg = distinct_aggs_[i];
      ast::Expr *distinct_type = codegen_->TplType(term->GetChild(0)->GetReturnValueType());
      fields.emplace_back(codegen_->MakeField(aggregates_[i], distinct_type));
      // Make the struct
      decls->emplace_back(codegen_->MakeStruct(distinct_agg.StructType(), std::move(fields)));
    }
  }
}


void AggregateHelper::GenValuesStructs(util::RegionVector<ast::Decl *> *decls) {
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


void AggregateHelper::GenDistinctHashCall(FunctionBuilder* builder, uint32_t term_idx) {
  // First add group by terms
  std::vector<ast::Expr *> hash_args{};
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    hash_args.emplace_back(codegen_->MemberExpr(agg_values_, group_bys_[i]));
  }
  // For distinct aggregates, we also need to add the distinct term
  auto &distinct_agg = distinct_aggs_[term_idx];
  hash_args.emplace_back(codegen_->MemberExpr(agg_values_, aggregates_[term_idx]));
  // Make the hash call.
  ast::Expr *hash_call = codegen_->BuiltinCall(ast::Builtin::Hash, std::move(hash_args));
  builder->Append(codegen_->DeclareVariable(distinct_agg->HashVal(), nullptr, hash_call));
}


void AggregateHelper::GenGlobalHashCall(FunctionBuilder* builder) {
  TERRIER_ASSERT(!op_->GetGroupByTerms().empty(), "Static Aggregates do not have a global hash table");
  // Just add the groub by terms.
  std::vector<ast::Expr *> hash_args{};
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    hash_args.emplace_back(codegen_->MemberExpr(agg_values_, group_bys_[i]));
  }
  ast::Expr *hash_call = codegen_->BuiltinCall(ast::Builtin::Hash, std::move(hash_args));
  builder->Append(codegen_->DeclareVariable(distinct_agg->HashVal(), nullptr, hash_call));
}

void AggregateHelper::GenDistinctKeyChecks(util::RegionVector<ast::Decl *> *decls) {
  for (uint32_t i = 0; i < op_->GetAggregateTerms().size(); i++) {
    auto agg = op_->GetAggregateTerms()[i];
    if (agg->IsDistinct()) {
      auto &distinct_agg = distinct_aggs_[i];
      // Create a function (distinct_entry: *DistinctStruct, values: *ValuesStruct) -> bool
      ast::Expr *distinct_struct_prt = codegen_->PointerType(distinct_agg->StructType());
      ast::FieldDecl *param1 = codegen_->MakeField(distinct_agg->Entry(), distinct_struct_prt);
      // Then make agg_values: *AggValues
      ast::Expr *values_struct_ptr = codegen_->PointerType(values_struct_);
      ast::FieldDecl *param2 = codegen_->MakeField(agg_values_, values_struct_ptr);
      // Now create the function
      util::RegionVector<ast::FieldDecl *> params({param1, param2}, codegen_->Region());
      ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Bool);
      FunctionBuilder builder(codegen_, distinct_agg->KeyCheck(), std::move(params), ret_type);
      // Fill up the function
      GenKeyCheckComparison(&builder, distinct_agg->Entry());
      // return (distinct_entry.agg_val == values.agg_val).
      ast::Expr* rhs = codegen_->MemberExpr(distinct_agg->Entry(), aggregates_[i]);
      ast::Expr* lhs = codegen_->MemberExpr(agg_values_, aggregates_[i]);
      ast::Expr* comp = codegen_->Compare(parsing::Token::Type::EQUAL_EQUAL, lhs, rhs);
      builder.Append(codegen_->ReturnStmt(comp));
      // Add it to top level declarations
      decls->emplace_back(builder.Finish());

    }
  }
}

void AggregateHelper::GenKeyCheckComparison(FunctionBuilder *builder, const ast::Identifier& entry) {
  for (uint32_t term_idx = 0; term_idx < op_->GetGroupByTerms().size(); term_idx++) {
    ast::Expr *lhs = codegen_->MemberExpr(entry, group_bys_[term_idx]);
    ast::Expr *rhs = codegen_->MemberExpr(agg_values_, group_bys_[term_idx]);
    ast::Expr *cond = codegen_->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs);
    builder->StartIfStmt(cond);
    builder->Append(codegen_->ReturnStmt(codegen_->BoolLiteral(false)));
    builder->FinishBlockStmt();
  }
}

void AggregateHelper::GenAdvanceAggs(FunctionBuilder* builder) {
  // Call @aggAdvance(&agg_payload.expr_i, &agg_values.expr_i) for each expression
  for (uint32_t term_idx = 0; term_idx < op_->GetAggregateTerms().size(); term_idx++) {
    auto term = op_->GetAggregateTerms()[term_idx];
    if (term->IsDistinct()) {
      auto &distinct_agg = distinct_aggs_[term_idx];
      // For distinct aggregates, we need to check the content of the hash table first.
      {
        GenDistinctHashCall(builder, term_idx);
        std::vector<ast::Expr *> lookup_args{codegen_->GetStateMemberPtr(distinct_agg->HT()), codegen_->MakeExpr(distinct_agg->HashVal()),
                                             codegen_->MakeExpr(distinct_agg->KeyCheck()), codegen_->PointerTo(agg_values_)};
        ast::Expr *lookup_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableLookup, std::move(lookup_args));
        ast::Expr *cast_call = codegen_->PtrCast(distinct_agg->StructType(), lookup_call);
        builder->Append(codegen_->DeclareVariable(distinct_agg->Entry(), nullptr, cast_call));
      }
      // We only advance if the lookup returns nil
      {
        ast::Expr *nil = codegen_->NilLiteral();
        ast::Expr *payload = codegen_->MakeExpr(distinct_agg->Entry());
        ast::Expr *cond = codegen_->Compare(parsing::Token::Type::EQUAL_EQUAL, nil, payload);
        builder->StartIfStmt(cond);
      }
      // Insert into hash table
      {
        std::vector<ast::Expr *> insert_args{codegen_->GetStateMemberPtr(distinct_agg->HT()), codegen_->MakeExpr(distinct_agg->HashVal())};
        ast::Expr *insert_call = codegen_->BuiltinCall(ast::Builtin::AggHashTableInsert, std::move(insert_args));
        auto cast_call = codegen_->PtrCast(distinct_agg->StructType(), insert_call);
        builder->Append(codegen_->Assign(codegen_->MakeExpr(distinct_agg->Entry()), cast_call));

      }
      // Set values to avoid duplicates
      {
        // First set the groub by values
        InitGroupByValues(distinct_agg->Entry());
        // Then set the distinct aggregate
        ast::Expr *lhs = codegen_->MemberExpr(distinct_agg->Entry(), aggregates_[term_idx]);
        ast::Expr *rhs = codegen_->MemberExpr(agg_values_, aggregates_[term_idx]);
        builder->Append(codegen_->Assign(lhs, rhs));
      }
      // Finally advance
      AdvanceAgg(builder, term_idx);
    } else {
      // For regular aggregates, just advance
      AdvanceAgg(builder, term_idx);
    }
  }}

void AggregateHelper::FillValues(FunctionBuilder *builder) {
  // First declare var agg_values: AggValues
  builder->Append(codegen_->DeclareVariable(agg_values_, codegen_->MakeExpr(values_struct_), nullptr));

  // Add group by terms
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    auto term = op_->GetGroupByTerms()[i];
    ast::Expr *lhs = codegen_->MemberExpr(agg_values_, group_bys_[i]);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(term.Get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(this);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
  // Add aggregates
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    auto term = op_->GetAggregateTerms()[i];
    ast::Expr *lhs = codegen_->MemberExpr(agg_values_, aggregates_[i]);
    auto term_translator = TranslatorFactory::CreateExpressionTranslator(term->GetChild(0).Get(), codegen_);
    ast::Expr *rhs = term_translator->DeriveExpr(this);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}

void AggregateHelper::InitGroupByValues(FunctionBuilder* builder, const ast::Identifier& entry) {
  for (uint32_t i = 0; i < op_->GetGroupByTerms().size(); i++) {
    ast::Expr *lhs = codegen_->MemberExpr(entry, group_bys_[i]);
    ast::Expr *rhs = codegen_->MemberExpr(agg_values_, group_bys_[i]);
    builder->Append(codegen_->Assign(lhs, rhs));
  }
}


void AggregateHelper::AdvanceAgg(FunctionBuilder* builder, uint32_t term_idx) {
  ast::Expr *arg1 = codegen_->GetStateMemberPtr(aggregates_[term_idx]);
  ast::Expr *arg2 = codegen_->PointerTo(codegen_->MemberExpr(agg_values_, aggregates_[term_idx]));
  ast::Expr *advance_call = codegen_->BuiltinCall(ast::Builtin::AggAdvance, {arg1, arg2});
  builder->Append(codegen_->MakeStmt(advance_call));
  builder->FinishBlockStmt();
}

}