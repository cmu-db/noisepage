#pragma once
#include <execution/compiler/if.h>

#include <vector>

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/state_descriptor.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

constexpr char AGG_VALUE_NAME[] = "agg_distinct";
constexpr char GROUPBY_VALUE_NAME[] = "groupby_distinct";

/**
 * A filter for distinct aggregation
 */
class DistinctAggregationFilter {
 public:
  /**
   * Creates a distinct aggregation filter. It is associated with a hash/static aggregation
   * translator, for each aggregate term.
   * @param agg_term_idx idx in the aggregate terms list
   * @param agg_term aggregate term
   * @param num_group_by  number of group by terms
   * @param ctx compilation context
   * @param pipeline pipeline to which the key check function belongs
   * @param codegen codegen
   */
  DistinctAggregationFilter(size_t agg_term_idx, const planner::AggregateTerm &agg_term, uint32_t num_group_by,
                            CompilationContext *ctx, Pipeline *pipeline, CodeGen *codegen);

  /**
   * Declare the KeyType struct that stores the aggregate value in the hash table
   * @param codegen  Codegen object
   * @param agg_term  Aggregate term
   * @param group_bys List of Group By terms
   * @return struct declaration
   */
  ast::StructDecl *GenerateKeyStruct(CodeGen *codegen, const planner::AggregateTerm &agg_term,
                                     const std::vector<planner::GroupByTerm> &group_bys) const;

  /**
   * Generate a key check function for aggregate values that hash to the same hash value
   * @param codegen
   * @param group_bys a list of group by terms
   * @return function declaration pointer
   */
  ast::FunctionDecl *GenerateDistinctCheckFunction(CodeGen *codegen,
                                                   const std::vector<planner::GroupByTerm> &group_bys) const;

  /**
   * Initialize the filter by initializing the aggregation hash table. This is called at
   * InitializeQueryState() of the translator
   * @param codegen  CodegGen object
   * @param function function builder
   * @param exec_ctx execution context
   * @param memory_pool memory pool
   */
  void Initialize(CodeGen *codegen, FunctionBuilder *function, ast::Expr *exec_ctx, ast::Expr *memory_pool) const {
    function->Append(codegen->AggHashTableInit(ht_.GetPtr(codegen), exec_ctx, memory_pool, key_type_));
  }

  /**
   * Tear down the hash table. This is called at the TearDownQueryState() of the translator
   * @param codegen CodeGen object
   * @param function Function builder
   */
  void TearDown(CodeGen *codegen, FunctionBuilder *function) const {
    function->Append(codegen->AggHashTableFree(ht_.GetPtr(codegen)));
  }

  /**
   * Advance a aggregate update to the underlying aggregate accumulator only if the current value
   * has not existed in the hash table.
   * @param codegen CodeGen object
   * @param function Function builder
   * @param advance_call AST call that advance the aggregation
   * @param agg_val Aggregate value converted to ast::Expr
   * @param group_bys List of group by values converted to ast::Expr
   */
  void AggregateDistinct(CodeGen *codegen, FunctionBuilder *function, ast::Expr *advance_call, ast::Expr *agg_val,
                         const std::vector<ast::Expr *> &group_bys) const;

  ast::Identifier GetKeyType() const { return key_type_; }

 private:
  /**
   * Get the value to be aggregated from the KeyType payload
   * @param codegen CodeGen
   * @param row KeyType struct
   * @return the aggregate value
   */
  ast::Expr *GetAggregateValue(CodeGen *codegen, ast::Expr *row) const {
    auto member = codegen->MakeIdentifier(AGG_VALUE_NAME);
    return codegen->AccessStructMember(row, member);
  }

  /**
   * Get the group by value from the payload
   * @param codegen Codegen object
   * @param row KeyType struct
   * @param idx the index of the group by terms
   * @return the group by term
   */
  ast::Expr *GetGroupByValue(CodeGen *codegen, ast::Expr *row, uint32_t idx) const {
    auto member = codegen->MakeIdentifier(GROUPBY_VALUE_NAME + std::to_string(idx));
    return codegen->AccessStructMember(row, member);
  }

  ast::Identifier ComputeHash(CodeGen *codegen, FunctionBuilder *function, ast::Identifier row) const {
    std::vector<ast::Expr *> keys;
    // Hash the AGG value
    keys.push_back(GetAggregateValue(codegen, codegen->MakeExpr(row)));

    // Hash the GroupBy terms
    for (uint32_t idx = 0; idx < num_group_by_; ++idx) {
      keys.push_back(GetGroupByValue(codegen, codegen->MakeExpr(row), idx));
    }

    auto hash_val = codegen->MakeFreshIdentifier("hashVal");
    function->Append(codegen->DeclareVarWithInit(hash_val, codegen->Hash(keys)));
    return hash_val;
  }

  ast::Identifier FillLookupKey(CodeGen *codegen, FunctionBuilder *function, ast::Expr *agg_val,
                                const std::vector<ast::Expr *> &group_bys) const {
    auto lookup_key = codegen->MakeFreshIdentifier("lookupKey");
    function->Append(codegen->DeclareVarNoInit(lookup_key, codegen->MakeExpr(key_type_)));

    // Fill in agg value
    auto agg_lhs = GetAggregateValue(codegen, codegen->MakeExpr(lookup_key));
    function->Append(codegen->Assign(agg_lhs, agg_val));

    // Fill in Group bys
    for (uint32_t idx = 0; idx < group_bys.size(); ++idx) {
      auto grp_lhs = GetGroupByValue(codegen, codegen->MakeExpr(lookup_key), idx);
      function->Append(codegen->Assign(grp_lhs, group_bys[idx]));
    }

    return lookup_key;
  }

  void AssignPayload(CodeGen *codegen, FunctionBuilder *function, ast::Identifier payload,
                     ast::Identifier lookup_key) const {
    // Assign Agg Value
    auto agg_lhs = GetAggregateValue(codegen, codegen->MakeExpr(payload));
    auto agg_rhs = GetAggregateValue(codegen, codegen->MakeExpr(lookup_key));
    function->Append(codegen->Assign(agg_lhs, agg_rhs));

    // Assign Group By Values
    for (uint32_t idx = 0; idx < num_group_by_; ++idx) {
      auto grp_lhs = GetGroupByValue(codegen, codegen->MakeExpr(payload), idx);
      auto grp_rhs = GetGroupByValue(codegen, codegen->MakeExpr(lookup_key), idx);
      function->Append(codegen->Assign(grp_lhs, grp_rhs));
    }
  }

  // Hash table value type
  ast::Identifier key_type_;

  // Key check function for hash collision
  ast::Identifier key_check_fn_;
  // Hash table
  compiler::StateDescriptor::Entry ht_;

  // Number of GroupBy
  uint32_t num_group_by_;
};

}  // namespace terrier::execution::compiler
