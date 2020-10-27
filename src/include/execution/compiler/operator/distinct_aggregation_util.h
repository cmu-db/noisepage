#pragma once

#include <vector>

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/if.h"
#include "execution/compiler/state_descriptor.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/aggregate_plan_node.h"

/* Forward Declare for optimized compilation */
namespace noisepage::planner {
class AggregatePlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

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
   */
  void Initialize(CodeGen *codegen, FunctionBuilder *function, ast::Expr *exec_ctx) const {
    function->Append(codegen->AggHashTableInit(ht_.GetPtr(codegen), exec_ctx, key_type_));
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

 private:
  /**
   * Get the value to be aggregated from the KeyType payload
   * @param codegen CodeGen
   * @param row KeyType struct
   * @return the aggregate value
   */
  ast::Expr *GetAggregateValue(CodeGen *codegen, ast::Expr *row) const;

  /**
   * Get the group by value from the payload
   * @param codegen Codegen object
   * @param row KeyType struct
   * @param idx the index of the group by terms
   * @return the group by term
   */
  ast::Expr *GetGroupByValue(CodeGen *codegen, ast::Expr *row, uint32_t idx) const;

  /**
   * Compute the hash of a hashtable key
   * @param codegen Codegen object
   * @param function Function builder
   * @param row A key to be inserted
   * @return
   */
  ast::Identifier ComputeHash(CodeGen *codegen, FunctionBuilder *function, ast::Identifier row) const;

  /**
   * Fill up the look up keys from aggregate values and group by values
   * @param codegen  Codegen object
   * @param function  Function builder
   * @param agg_val  Aggregate value
   * @param group_bys  list of Group By values
   * @return the key to be inserted
   */
  ast::Identifier FillLookupKey(CodeGen *codegen, FunctionBuilder *function, ast::Expr *agg_val,
                                const std::vector<ast::Expr *> &group_bys) const;

  /**
   * Assign a look up key struct into the payload (from the hashtable)
   * @param codegen  Codegen object
   * @param function  Function builder
   * @param payload  Placeholder to fill in values
   * @param lookup_key Sources of the values
   */
  void AssignPayload(CodeGen *codegen, FunctionBuilder *function, ast::Identifier payload,
                     ast::Identifier lookup_key) const;

  // Hash table value type
  ast::Identifier key_type_;

  // Key check function for hash collision
  ast::Identifier key_check_fn_;
  // Hash table
  compiler::StateDescriptor::Entry ht_;

  // Number of GroupBy
  uint32_t num_group_by_;
};

}  // namespace noisepage::execution::compiler
