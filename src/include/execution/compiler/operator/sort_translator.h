#pragma once
#include <utility>
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/order_by_plan_node.h"

namespace terrier::execution::compiler {

class SortTopTranslator;

/**
 * The Sorter bottom translator.
 * TODO(Amadou): In general, the sorter and the aggregator are similar in structure, so some refactoring is possible.
 */
class SortBottomTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  SortBottomTranslator(const terrier::planner::OrderByPlanNode *op, CodeGen *codegen);

  // Declare the Sorter
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare SorterRow struct
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Create the comparison function
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Call @sorterInit on the Sorter
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Call @asorterFree on the Sorter
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  // This is materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // Return the payload and its type
  std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() override {
    return {&sorter_row_, &sorter_struct_};
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

  /**
   * @returns struct declaration
   */
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

 private:
  friend class SortTopTranslator;

  // Return the member of the object at the given index
  ast::Expr *GetAttribute(ast::Identifier object, uint32_t attr_idx);
  // Insert into sorter
  void GenSorterInsert(FunctionBuilder *builder);
  // Gen top k finish
  void GenFinishTopK(FunctionBuilder *builder);
  // Fill the sorter row
  void FillSorterRow(FunctionBuilder *builder);
  // Call Sort()
  void GenSorterSort(FunctionBuilder *builder);
  // Generate the comparisons in the comparison function
  void GenComparisons(FunctionBuilder *builder);

  // The sort plan node
  const planner::OrderByPlanNode *op_;

  // Struct Decl
  ast::StructDecl *struct_decl_;

  /**
   * GetChildOutput will need to return different results depending on the calling function.
   * In the comparison function, it will use either the lhs or rhs of the comparison to generate expressions.
   * In the main pipeline, it will use the child node to generate expression.
   * This enum and variable help keep track of this.
   * TODO(Amadou): If it turns out that the OrderBy Clause is always a column index and not an arbitrary expression,
   * then is is unnecessary.
   */
  enum class CurrentRow { Child, Lhs, Rhs };
  CurrentRow current_row_{CurrentRow::Child};

  // Structs, Functions, and local variables needed.
  static constexpr const char *SORTER_ATTR_PREFIX = "sorter_attr";
  ast::Identifier sorter_;
  ast::Identifier sorter_row_;
  ast::Identifier sorter_struct_;
  ast::Identifier comp_fn_;
  ast::Identifier comp_lhs_;
  ast::Identifier comp_rhs_;
};

/**
 * The Sorter top translator.
 */
class SortTopTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   * @param bottom The corresponding bottom translator
   */
  SortTopTranslator(const terrier::planner::OrderByPlanNode *op, CodeGen *codegen, OperatorTranslator *bottom);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  // This is materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // Return the payload and its type
  std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() override {
    return {&bottom_->sorter_row_, &bottom_->sorter_struct_};
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  // Declare the iterator
  void DeclareIterator(FunctionBuilder *builder);
  // Generate the iteration loop
  void GenForLoop(FunctionBuilder *builder);
  // Close the iterator
  void CloseIterator(FunctionBuilder *builder);
  // Get the next tuple of the sorter
  void DeclareResult(FunctionBuilder *builder);

  // The sort plan node
  const planner::OrderByPlanNode *op_;

  // The bottom translator
  SortBottomTranslator *bottom_;

  // Local variables
  ast::Identifier sort_iter_;
  ast::Identifier num_tuples_;
};

}  // namespace terrier::execution::compiler
