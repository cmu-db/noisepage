#include "execution/compiler/operator/distinct_aggregation_util.h"

namespace terrier::execution::compiler {

DistinctAggregationFilter::DistinctAggregationFilter(size_t agg_term_idx, const planner::AggregateTerm &agg_term,
                                                     uint32_t num_group_by, CompilationContext *ctx, Pipeline *pipeline,
                                                     CodeGen *codegen)
    : key_type_(ctx->GetCodeGen()->MakeFreshIdentifier("KeyType")),
      key_check_fn_(ctx->GetCodeGen()->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("DistinctKeyFn"))),
      num_group_by_(num_group_by) {
  auto *ht_type = codegen->BuiltinType(ast::BuiltinType::AggregationHashTable);
  ht_ = ctx->GetQueryState()->DeclareStateEntry(codegen, "hashTable" + std::to_string(agg_term_idx), ht_type);
}

ast::StructDecl *DistinctAggregationFilter::GenerateKeyStruct(
    CodeGen *codegen, const planner::AggregateTerm &agg_term,
    const std::vector<planner::GroupByTerm> &group_bys) const {
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(1 + group_bys.size());

  // Add Agg Terms
  auto name = codegen->MakeIdentifier(AGG_VALUE_NAME);
  auto type = codegen->TplType(sql::GetTypeId(agg_term->GetReturnValueType()));
  fields.push_back(codegen->MakeField(name, type));

  // Add Group BY
  for (size_t term_idx = 0; term_idx < group_bys.size(); ++term_idx) {
    auto term = group_bys[term_idx];
    auto field_name = codegen->MakeIdentifier(GROUPBY_VALUE_NAME + std::to_string(term_idx));
    auto field_type = codegen->TplType(sql::GetTypeId(term->GetReturnValueType()));
    fields.push_back(codegen->MakeField(field_name, field_type));
  }

  return codegen->DeclareStruct(key_type_, std::move(fields));
}

ast::FunctionDecl *DistinctAggregationFilter::GenerateDistinctCheckFunction(
    CodeGen *codegen, const std::vector<planner::GroupByTerm> &group_bys) const {
  // Payload in the hash table
  auto payload = codegen->MakeIdentifier("payload");

  // Key to be checked
  auto key = codegen->MakeIdentifier("key");

  auto params = codegen->MakeFieldList({codegen->MakeField(payload, codegen->PointerType(key_type_)),
                                        codegen->MakeField(key, codegen->PointerType(key_type_))});

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Bool);

  FunctionBuilder builder(codegen, key_check_fn_, std::move(params), ret_type);
  {
    // Check for AGG value first
    auto lhs = GetAggregateValue(codegen, codegen->MakeExpr(payload));
    auto rhs = GetAggregateValue(codegen, codegen->MakeExpr(key));

    If check_match(&builder, codegen->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
    { builder.Append(codegen->Return(codegen->ConstBool(false))); }
    check_match.EndIf();

    // Check for group by
    for (uint32_t term_idx = 0; term_idx < group_bys.size(); ++term_idx) {
      auto grp_lhs = GetGroupByValue(codegen, codegen->MakeExpr(payload), term_idx);
      auto grp_rhs = GetGroupByValue(codegen, codegen->MakeExpr(key), term_idx);

      If check_match_grpby(&builder, codegen->Compare(parsing::Token::Type::BANG_EQUAL, grp_lhs, grp_rhs));
      builder.Append(codegen->Return(codegen->ConstBool(false)));
    }

    builder.Append(codegen->Return(codegen->ConstBool(true)));
  }

  return builder.Finish();
}
void DistinctAggregationFilter::AggregateDistinct(CodeGen *codegen, FunctionBuilder *function, ast::Expr *advance_call,
                                                  ast::Expr *agg_val, const std::vector<ast::Expr *> &group_bys) const {
  // prepare key: var key
  auto lookup_key = FillLookupKey(codegen, function, agg_val, group_bys);

  // Hash
  auto hash_keys = ComputeHash(codegen, function, lookup_key);

  // Check for duplicates
  auto lookup_call = codegen->AggHashTableLookup(ht_.GetPtr(codegen), codegen->MakeExpr(hash_keys), key_check_fn_,
                                                 codegen->AddressOf(codegen->MakeExpr(lookup_key)), key_type_);
  auto lookup_payload = codegen->MakeFreshIdentifier("lookupPayload");
  function->Append(codegen->DeclareVarWithInit(lookup_payload, lookup_call));

  If check_new_key(function, codegen->IsNilPointer(codegen->MakeExpr(lookup_payload)));
  {
    // Insert new agg_val into the filter
    auto insert_call = codegen->AggHashTableInsert(ht_.GetPtr(codegen), codegen->MakeExpr(hash_keys), false, key_type_);
    function->Append(codegen->Assign(codegen->MakeExpr(lookup_payload), insert_call));

    // Initialize the payload
    AssignPayload(codegen, function, lookup_payload, lookup_key);

    // Perform aggregate
    function->Append(advance_call);
  }
  check_new_key.EndIf();
}

}  // namespace terrier::execution::compiler
