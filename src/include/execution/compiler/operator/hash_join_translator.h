#pragma once

#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/hash_join_plan_node.h"

namespace terrier::execution::compiler {

// Forward declare for friendship
class HashJoinRightTranslator;

/**
 * Left translator for joins
 */
class HashJoinLeftTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  HashJoinLeftTranslator(const terrier::planner::HashJoinPlanNode *op, CodeGen *codegen);

  // Insert tuples into the hash table
  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
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

  const planner::AbstractPlanNode *Op() override { return op_; }

  /**
   * @returns struct declaration
   */
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

 private:
  friend class HashJoinRightTranslator;

  // Iterate through the left join keys and hash them
  void GenHashCall(FunctionBuilder *builder);

  // Generate the join hash table insertion code
  void GenHTInsert(FunctionBuilder *builder);

  // Fill the build row
  void FillBuildRow(FunctionBuilder *builder);

  // Get an attribute from attribute struct
  ast::Expr *GetBuildValue(uint32_t idx);

  // Get the mark flag
  ast::Expr *GetMarkFlag();

  // Build the hash table
  void GenBuildCall(FunctionBuilder *builder);

  // The hash join plan node
  const planner::HashJoinPlanNode *op_;

  // Struct Decl
  ast::StructDecl *struct_decl_;

  // Structs, functions, and locals
  static constexpr const char *LEFT_ATTR_NAME = "left_attr";
  ast::Identifier hash_val_;
  ast::Identifier build_struct_;
  ast::Identifier build_row_;
  ast::Identifier join_ht_;
  // This boolean is used for semi and anti joins.
  // It indicates whether a tuple has been matched or not.
  ast::Identifier mark_;
};

/**
 * Right translator for joins
 */
class HashJoinRightTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   * @param left The corresponding left translator
   */
  HashJoinRightTranslator(const terrier::planner::HashJoinPlanNode *op, CodeGen *codegen, OperatorTranslator *left);

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
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

  const planner::AbstractPlanNode *Op() override { return op_; }

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

  // If statement for left semi joins
  void GenLeftSemiJoinCondition(FunctionBuilder *builder);

  // Complete the join key check function
  void GenKeyCheck(FunctionBuilder *builder);

  // The hash join plan node
  const planner::HashJoinPlanNode *op_;
  // The left translator
  HashJoinLeftTranslator *left_;

  /*
   * Whether the right child is a materializer. And whether the materialized tuple is a pointer.
   */
  bool is_child_materializer_{false};
  bool is_child_ptr_{false};

  // Structs, functions, and locals
  static constexpr const char *RIGHT_ATTR_NAME = "right_attr";
  ast::Identifier hash_val_;
  ast::Identifier probe_struct_;
  ast::Identifier probe_row_;
  ast::Identifier key_check_;
  ast::Identifier join_iter_;
};
}  // namespace terrier::execution::compiler
