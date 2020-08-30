#include "execution/compiler/operator/distinct_aggregation_util.h"

namespace terrier::execution::compiler {

DistinctAggregationFilter::DistinctAggregationFilter(size_t agg_term_idx, const planner::AggregateTerm &agg_term,
                                                     CompilationContext *ctx, Pipeline *pipeline, CodeGen *codegen)
    : key_type_(ctx->GetCodeGen()->MakeFreshIdentifier("KeyType")),
      key_check_fn_(ctx->GetCodeGen()->MakeFreshIdentifier(pipeline->CreatePipelineFunctionName("DistinctKeyFn"))) {
  auto *ht_type = codegen->BuiltinType(ast::BuiltinType::AggregationHashTable);
  ht_ = ctx->GetQueryState()->DeclareStateEntry(codegen, "hashTable" + std::to_string(agg_term_idx), ht_type);
}

ast::StructDecl *DistinctAggregationFilter::GenerateKeyStruct(CodeGen *codegen,
                                                              const planner::AggregateTerm &agg_term) const {
  auto fields = codegen->MakeEmptyFieldList();
  fields.reserve(1);
  auto name = codegen->MakeIdentifier(AGG_VALUE_NAME);
  auto type = codegen->TplType(sql::GetTypeId(agg_term->GetReturnValueType()));
  fields.push_back(codegen->MakeField(name, type));

  return codegen->DeclareStruct(key_type_, std::move(fields));
}

ast::FunctionDecl *DistinctAggregationFilter::GenerateDistinctCheckFunction(CodeGen *codegen) const {
  // Payload in the hash table
  auto payload = codegen->MakeIdentifier("payload");

  // Key to be checked
  auto key = codegen->MakeIdentifier("key");

  auto params = codegen->MakeFieldList({codegen->MakeField(payload, codegen->PointerType(key_type_)),
                                        codegen->MakeField(key, codegen->PointerType(key_type_))});

  auto ret_type = codegen->BuiltinType(ast::BuiltinType::Kind::Bool);

  FunctionBuilder builder(codegen, key_check_fn_, std::move(params), ret_type);
  {
    // Check for the value
    auto lhs = GetAggregateValue(codegen, codegen->MakeExpr(payload));
    auto rhs = GetAggregateValue(codegen, codegen->MakeExpr(key));

    If check_match(&builder, codegen->Compare(parsing::Token::Type::BANG_EQUAL, lhs, rhs));
    { builder.Append(codegen->Return(codegen->ConstBool(false))); }
    check_match.EndIf();
    builder.Append(codegen->Return(codegen->ConstBool(true)));
  }

  return builder.Finish();
}

void DistinctAggregationFilter::AggregateDistinct(CodeGen *codegen, FunctionBuilder *function, ast::Expr *advance_call,
                                                  ast::Expr *val) const {
  auto hash_val = codegen->MakeFreshIdentifier("hashVal");
  function->Append(codegen->DeclareVarWithInit(hash_val, codegen->Hash({val})));
  // prepare key: var key
  auto key = codegen->MakeFreshIdentifier("lookupKey");
  function->Append(codegen->DeclareVarNoInit(key, codegen->MakeExpr(key_type_)));

  // key = {val}
  auto lhs = GetAggregateValue(codegen, codegen->MakeExpr(key));
  function->Append(codegen->Assign(lhs, val));  // rhs = val

  // Check for duplicates
  auto lookup_call = codegen->AggHashTableLookup(ht_.GetPtr(codegen), codegen->MakeExpr(hash_val), key_check_fn_,
                                                 codegen->AddressOf(codegen->MakeExpr(key)), key_type_);
  auto lookup_payload = codegen->MakeFreshIdentifier("lookupPayload");
  function->Append(codegen->DeclareVarWithInit(lookup_payload, lookup_call));

  If check_new_key(function, codegen->IsNilPointer(codegen->MakeExpr(lookup_payload)));
  {
    // Insert new agg_val into the filter
    auto insert_call = codegen->AggHashTableInsert(ht_.GetPtr(codegen), codegen->MakeExpr(hash_val), false, key_type_);
    function->Append(codegen->Assign(codegen->MakeExpr(lookup_payload), insert_call));

    // Initialize the payload
    function->Append(codegen->Assign(GetAggregateValue(codegen, codegen->MakeExpr(lookup_payload)), val));

    // Perform aggregate
    function->Append(advance_call);
  }
  check_new_key.EndIf();
}

}  // namespace terrier::execution::compiler
