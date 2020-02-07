#pragma once

#include <utility>
#include "planner/plannodes/aggregate_plan_node.h"
#include "execution/compiler/operator/operator_translator.h"


namespace terrier::execution::compiler {

/**
 * Contains all information needed for an aggregation hash table..
 */
class DistinctInfo {
 public:
  explicit DistinctInfo(CodeGen *codegen)
      : ht_(codegen->NewIdentifier("distinct_ht")),
        struct_(codegen->NewIdentifier("DistinctStruct")),
        key_check_(codegen->NewIdentifier("distictKeyCheckFn")),
        entry_(codegen->NewIdentifier("distinct_entry")),
        hash_val_(codegen->NewIdentifier("distinct_hash_val")) {}

  const ast::Identifier &HT() const { return ht_; }
  const ast::Identifier &StructType() const { return struct_; }
  const ast::Identifier &KeyCheck() const { return key_check_; }
  const ast::Identifier &Entry() const { return entry_; }
  const ast::Identifier &HashVal() const { return hash_val_; }
 private:
  // Hash table to check for duplicates.
  ast::Identifier ht_;
  // Types of entries of the hash hash table
  ast::Identifier struct_;
  // Key check function
  ast::Identifier key_check_;
  // Single entry from the hash table
  ast::Identifier entry_;
  // Hash values for the hash table
  ast::Identifier hash_val_;
};

/**
 * This class contains functionality needed by regular aggregations and static aggregations.
 */
class AggregateHelper {
 public:
  AggregateHelper(CodeGen *codegen, const planner::AggregatePlanNode *op);


  void InitDistinctTables(util::RegionVector<ast::Stmt *> *stmts);
  void FreeDistinctTables(util::RegionVector<ast::Stmt *> *stmts);

  /**
   * Generate the hash value for a distinct aggregate
   * @param builder Current function builder
   * @param term_idx index of the distinct aggregate.
   */
  void GenDistinctHashCall(FunctionBuilder* builder, uint32_t term_idx);

  void GenGlobalHashCall(FunctionBuilder* builder);

  void GenLookup(FunctionBuilder *builder,
                 const ast::Identifier &ht,
                 const ast::Identifier &hash_val,
                 const ast::Identifier key_check_fn);

  void GenDistinctLookup(FunctionBuilder *builder, uint32_t term_idx);

  void GenDistinctStateFields(util::RegionVector<ast::FieldDecl *> *fields);

  void GenAdvanceAggs(FunctionBuilder* builder);

  void GenHTStruct(util::RegionVector<ast::Decl *> *decls, const ast::Identifier& struct_name);
  void GenDistinctStructs(util::RegionVector<ast::Decl *> *decls);
  void GenValuesStructs(util::RegionVector<ast::Decl *> *decls);

  void FillValues(FunctionBuilder *builder);

  void GenDistinctKeyChecks(util::RegionVector<ast::Decl *> *decls);


  const ast::Identifier& GetAggregate(uint32_t term_idx) {return aggregates_[term_idx];}
  const ast::Identifier& GetGroupBy(uint32_t term_idx) {return group_bys_[term_idx];}
  const DistinctInfo* GetDistinctInfo(uint32_t term_idx) {return distinct_aggs_.at(term_idx).get(); }

 private:

  void GenKeyCheckComparison(FunctionBuilder *builder, const ast::Identifier &entry);
  void InitGroupByValues(FunctionBuilder* builder, const ast::Identifier& entry);
  void AdvanceAgg(FunctionBuilder* builder, uint32_t term_idx);

  CodeGen *codegen_;
  const planner::AggregatePlanNode *op_;

  // An entry into the global hash table.
  ast::Identifier global_entry_;
  // The global struct type
  ast::Identifier global_struct_;
  // The global key check function
  ast::Identifier global_key_check_;
  // The global hash table.
  ast::Identifier global_ht_;

  // Values that will be aggregated.
  ast::Identifier agg_values_;
  // Type of the values' struct.
  ast::Identifier values_struct_;
  // Name of the groub by terms
  std::vector<ast::Identifier> group_bys_;
  // Name of the groub by aggregate terms
  std::vector<ast::Identifier> aggregates_;
  // Information about distinct aggregates.
  std::unordered_map<uint32_t, std::unique_ptr<DistinctInfo>> distinct_aggs_;
};
}