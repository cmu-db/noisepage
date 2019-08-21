#pragma once
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::execution::compiler {

class SortTopTranslator;

/**
 * The Sorter bottom translator.
 * TODO(Amadou): In general, the sorter and the aggregator are similar in structure, so some refactoring is possible.
 */
class SortBottomTranslator : public OperatorTranslator {
 public:
  SortBottomTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen);

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

  // Let child produce and sort
  void Produce(FunctionBuilder *builder) override;

  // Generate sorter insert code
  void Consume(FunctionBuilder *builder) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  // Return the attribute at idx
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  // This is materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // Return the payload and its type
  std::pair<ast::Identifier *, ast::Identifier *> GetMaterializedTuple() override {
    return {&sorter_row_, &sorter_struct_};
  }

 private:
  friend class SortTopTranslator;

  /**
   * Return the member of the object at the given index
   */
  ast::Expr *GetAttribute(ast::Identifier object, uint32_t attr_idx);

  void GenSorterInsert(FunctionBuilder *builder);
  void FillSorterRow(FunctionBuilder *builder);
  void GenSorterSort(FunctionBuilder *builder);

  /*
   * Generate the comparisons in the comparison function
   */
  void GenComparisons(FunctionBuilder *builder);

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
  static constexpr const char *sorter_name_ = "sorter";
  static constexpr const char *sorter_row_name_ = "sorter_row";
  static constexpr const char *sorter_struct_name_ = "SorterRow";
  static constexpr const char *sorter_attr_prefix_ = "sorter_attr";
  static constexpr const char *comp_fn_name_ = "sorterCompare";
  static constexpr const char *comp_lhs_name_ = "sorter_lhs";
  static constexpr const char *comp_rhs_name_ = "sorter_rhs";
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
  SortTopTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen, OperatorTranslator *bottom);

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

  // Generate iteration code
  void Produce(FunctionBuilder *builder) override;

  // Pass through
  void Consume(FunctionBuilder *builder) override;

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  // Return the output at the given index
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  // This is materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // Return the payload and its type
  std::pair<ast::Identifier *, ast::Identifier *> GetMaterializedTuple() override {
    return {&bottom_->sorter_row_, &bottom_->sorter_struct_};
  }

 private:
  void DeclareIterator(FunctionBuilder *builder);
  void GenForLoop(FunctionBuilder *builder);
  void CloseIterator(FunctionBuilder *builder);
  void DeclareResult(FunctionBuilder *builder);

  // The bottom translator
  SortBottomTranslator *bottom_;

  // Local variables
  static constexpr const char *iter_name_ = "sorter_iter";
  ast::Identifier sort_iter_;
};

}  // namespace terrier::execution::compiler