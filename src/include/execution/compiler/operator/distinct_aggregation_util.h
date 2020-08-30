#pragma once
#include <execution/compiler/if.h>

#include <vector>

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/state_descriptor.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

constexpr char AGG_VALUE_NAME[] = "agg_distinct";

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
   * @param ctx compilation context
   * @param pipeline pipeline to which the key check function belongs
   * @param codegen codegen
   */
  DistinctAggregationFilter(size_t agg_term_idx, const planner::AggregateTerm &agg_term, CompilationContext *ctx,
                            Pipeline *pipeline, CodeGen *codegen);

  /**
   * Declare the KeyType struct that stores the aggregate value in the hash table
   * @param codegen  Codegen object
   * @param agg_term  Aggregate term
   * @return struct declaration
   */
  ast::StructDecl *GenerateKeyStruct(CodeGen *codegen, const planner::AggregateTerm &agg_term) const;

  /**
   * Generate a key check function for aggregate values that hash to the same hash value
   * @param codegen
   * @return function declaration pointer
   */
  ast::FunctionDecl *GenerateDistinctCheckFunction(CodeGen *codegen) const;

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
   * @param val The current value being examined
   */
  void AggregateDistinct(CodeGen *codegen, FunctionBuilder *function, ast::Expr *advance_call, ast::Expr *val) const;

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
  // Hash table value type
  ast::Identifier key_type_;

  // Key check function for hash collision
  ast::Identifier key_check_fn_;
  // Hash table
  compiler::StateDescriptor::Entry ht_;
};

}  // namespace terrier::execution::compiler
