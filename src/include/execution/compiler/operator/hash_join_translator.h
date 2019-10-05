#pragma once

#include <planner/plannodes/hash_join_plan_node.h>
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::execution::compiler {

/*
 * TODO(Amadou): These two classes have a lot in common. The only difference between several of their methods
 * is one index value (0 vs 1), or one function call (GetRightJoinKeys vs GetLeftJoinKeys) so refactoring is possible.
 * For now, I am keeping them separate to simplify things until I am sure the code works.
 */

// Forward declare for friendship
class HashJoinRightTranslator;

/**
 * Left translator for joins
 */
class HashJoinLeftTranslator : public OperatorTranslator {
 public:
  HashJoinLeftTranslator(const terrier::planner::HashJoinPlanNode *op, CodeGen *codegen);

  // Insert tuples into the hash table
  void Produce(FunctionBuilder *builder) override;

  void Consume(FunctionBuilder *builder) override;

  // Add the join hash table
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare JoinBuild struct
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Call @joinHTInit on the hash table
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Call @joinHTFree on the hash table
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  ast::Expr *GetOutput(uint32_t attr_idx) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  const planner::AbstractPlanNode* Op() override {
    return op_;
  }

 private:
  friend class HashJoinRightTranslator;

  // Iterator through the left join keys and hash them
  void GenHashCall(FunctionBuilder *builder);

  // Generate the join hash table insertion code
  void GenHTInsert(FunctionBuilder *builder);

  // Fill the build row
  void FillBuildRow(FunctionBuilder *builder);

  // Get an attribute from attribute struct
  ast::Expr *GetBuildValue(uint32_t idx);

  // Build the hash table
  void GenBuildCall(FunctionBuilder *builder);

  // The hash join plan node
  const planner::HashJoinPlanNode* op_;

  // Structs, functions, and locals
  static constexpr const char *hash_val_name_ = "hash_val";
  static constexpr const char *build_struct_name_ = "JoinBuild";
  static constexpr const char *build_row_name_ = "build_row";
  static constexpr const char *join_ht_name_ = "join_hash_table";
  static constexpr const char *left_attr_name_ = "left_attr";
  ast::Identifier hash_val_;
  ast::Identifier build_struct_;
  ast::Identifier build_row_;
  ast::Identifier join_ht_;
};

/**
 * Right translator for joins
 */
class HashJoinRightTranslator : public OperatorTranslator {
 public:
  HashJoinRightTranslator(const terrier::planner::HashJoinPlanNode *op, CodeGen *codegen, OperatorTranslator *left);

  void Produce(FunctionBuilder *builder) override;

  void Consume(FunctionBuilder *builder) override;

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Declare JoinProbe struct if the previous operator is not a materializer
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Declare the keyCheck function
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Does nothing (left operator already initialized the hash table)
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing (left operator already freed the hash table)
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

  // Get the output at idx
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  // Dispatch the call to the correct child
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  // This is not a materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = false;
    return false;
  }

  const planner::AbstractPlanNode* Op() override {
    return op_;
  }

 private:
  // Returns a probe value
  ast::Expr *GetProbeValue(uint32_t idx);

  // Make the probing hash value
  void GenHashValue(FunctionBuilder *builder);

  // Fill the probe row if necessary
  void FillProbeRow(FunctionBuilder *builder);

  // Declare the hash table iterator
  void DeclareIterator(FunctionBuilder *builder);

  // Loop to probe the hash table
  void GenProbeLoop(FunctionBuilder *builder);

  // Close the iterator after the loop
  void GenIteratorClose(FunctionBuilder *builder);

  // Declare the matching tuple
  void DeclareMatch(FunctionBuilder *builder);

  // Complete the join key check function
  void GenKeyCheck(FunctionBuilder *builder);

  // The hash join plan node
  const planner::HashJoinPlanNode* op_;
  // The left translator
  HashJoinLeftTranslator *left_;

  /*
   * Whether the right child is a materializer. And whether the materialized tuple is a pointer.
   */
  bool is_child_materializer_{false};
  bool is_child_ptr_{false};

  // Structs, functions, and locals
  static constexpr const char *hash_val_name_ = "hash_val";
  static constexpr const char *probe_struct_name_ = "JoinProbe";
  static constexpr const char *probe_row_name_ = "probe_row";
  static constexpr const char *key_check_name_ = "joinKeyCheck";
  static constexpr const char *iterator_name_ = "join_iterator";
  static constexpr const char *right_attr_name_ = "right_attr";
  ast::Identifier hash_val_;
  ast::Identifier probe_struct_;
  ast::Identifier probe_row_;
  ast::Identifier key_check_;
  ast::Identifier join_iter_;
};
}  // namespace terrier::execution::compiler