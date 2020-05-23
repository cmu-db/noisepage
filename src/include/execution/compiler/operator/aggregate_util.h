#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Contains all information needed for an aggregation hash table..
 */
class AHTInfo {
 public:
  /**
   * Non distinct constructor.
   * @param codegen The code generator
   */
  explicit AHTInfo(CodeGen *codegen) : AHTInfo(codegen, false, 0) {}

  /**
   * Generic constructor
   * @param codegen The code generator
   * @param is_distinct Whether this hash table is for a distinct aggregate.
   * @param term_idx Index of the distinct aggregate.
   */
  explicit AHTInfo(CodeGen *codegen, bool is_distinct, uint32_t term_idx)
      : is_distinct_(is_distinct),
        term_idx_(term_idx),
        ht_(codegen->NewIdentifier("aht")),
        struct_(codegen->NewIdentifier("AHTStruct")),
        key_check_(codegen->NewIdentifier("ahtKeyCheckFn")),
        entry_(codegen->NewIdentifier("aht_entry")),
        hash_val_(codegen->NewIdentifier("aht_hash_val")) {}

  /**
   * @return Whether the hash table is for a distinct aggregate.
   */
  bool IsDistinct() const { return is_distinct_; }

  /**
   * @return Index of the distinct aggregate.
   */
  uint32_t TermIdx() const {
    TERRIER_ASSERT(is_distinct_, "Only Valid for distinct aggregates");
    return term_idx_;
  }

  /**
   * @return The hash table identifier
   */
  ast::Identifier HT() const { return ht_; }

  /**
   * @return The type of the struct
   */
  const ast::Identifier &StructType() const { return struct_; }

  /**
   * @return The key check function
   */
  const ast::Identifier &KeyCheck() const { return key_check_; }

  /**
   * @return Identifier of a hash table entry
   */
  const ast::Identifier &Entry() const { return entry_; }

  /**
   * @return Identifier of the hash value
   */
  const ast::Identifier &HashVal() const { return hash_val_; }

 private:
  // Whether this is a distinct aggregate
  bool is_distinct_;
  // Index of the distinct aggregate.
  uint32_t term_idx_;
  // Name of the hash table.
  ast::Identifier ht_;
  // Types of entries of the hash table
  ast::Identifier struct_;
  // Key check function
  ast::Identifier key_check_;
  // Single entry from the hash table.
  ast::Identifier entry_;
  // Hash values for the hash table
  ast::Identifier hash_val_;
};

/**
 * This class contains functionality needed by regular aggregations and static aggregations.
 */
class AggregateHelper {
 public:
  /**
   * Constructor
   * @param codegen The code generator
   * @param op The aggregate plan node
   */
  AggregateHelper(CodeGen *codegen, const planner::AggregatePlanNode *op);

  /**
   * Initialize all hash tables.
   * @param stmts List of setup statements.
   */
  void InitAHTs(util::RegionVector<ast::Stmt *> *stmts);

  /**
   * Free all hash tables.
   * @param stmts List of teardown statements.
   */
  void FreeAHTs(util::RegionVector<ast::Stmt *> *stmts);

  /**
   * Declare all hash tables.
   * @param fields List of state fields.
   */
  void DeclareAHTs(util::RegionVector<ast::FieldDecl *> *fields);

  /**
   * Generate the key check function of each hash table.
   * @param decls List of top level declarations.
   */
  void GenKeyChecks(util::RegionVector<ast::Decl *> *decls);

  /**
   * Generate the input structs of each hash table.
   * @param global_decl Output to place struct decl
   * @param decls List of top level declarations.
   */
  void GenAHTStructs(ast::StructDecl **global_decl, util::RegionVector<ast::Decl *> *decls);

  /**
   * Generate the struct containing the values to aggregate and groub by.
   * @param decls List of top level declarations.
   */
  void GenValuesStruct(util::RegionVector<ast::Decl *> *decls);

  /**
   * Generate the hash value for each hash table.
   * @param builder Current function builder
   */
  void GenHashCalls(FunctionBuilder *builder);

  /**
   * Generate code to construct new hash table entries
   * @param builder Current function builder
   */
  void GenConstruct(FunctionBuilder *builder);

  /**
   * Fill the values that will be used to aggregate and group
   * @param builder Current function builder
   * @param translator The translator requesting the values.
   */
  void FillValues(FunctionBuilder *builder, OperatorTranslator *translator);

  /**
   * Unconditionally advance non distinct aggregates.
   * @param builder Current function builder.
   */
  void GenAdvanceNonDistinct(FunctionBuilder *builder);

  /**
   * @param term_idx Index of the aggregate.
   * @return The identifier of the aggregate term.
   */
  const ast::Identifier &GetAggregate(uint32_t term_idx) const { return aggregates_[term_idx]; }

  /**
   * @param term_idx Index of the group by term.
   * @return The identifer of the group by term.
   */
  const ast::Identifier &GetGroupBy(uint32_t term_idx) const { return group_bys_[term_idx]; }

  /**
   * @return The info of the global hash table.
   */
  const AHTInfo *GetGlobalAHT() const { return &global_info_; }

 private:
  /**
   * Set groub by values in the given hash table entry.
   * @param builder Current function builder.
   * @param info The hash table to initialize
   */
  void InitGroupByValues(FunctionBuilder *builder, const AHTInfo *info);

  /**
   * Advance a distinct aggregate.
   * @param builder Current function builder.
   * @param info The hash table of the distinct aggregate
   */
  void AdvanceDistinct(FunctionBuilder *builder, const AHTInfo *info);

  /**
   * Initialize the aggregates of the global hash table.
   * @param builder Current function builder.
   */
  void InitGlobalAggregates(FunctionBuilder *builder);

  // The code generator
  CodeGen *codegen_;
  // The aggregate operator
  const planner::AggregatePlanNode *op_;
  // An entry into the global hash table.
  AHTInfo global_info_;
  // Values that will be aggregated.
  ast::Identifier agg_values_;
  // Type of the values' struct.
  ast::Identifier values_struct_;
  // Name of the groub by terms
  std::vector<ast::Identifier> group_bys_;
  // Name of the groub by aggregate terms
  std::vector<ast::Identifier> aggregates_;
  // Information about distinct aggregates.
  std::unordered_map<uint32_t, std::unique_ptr<AHTInfo>> distinct_aggs_;
};
}  // namespace terrier::execution::compiler
