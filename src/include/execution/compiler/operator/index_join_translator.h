#pragma once
#include "execution/compiler/operator/operator_translator.h"

namespace tpl::compiler {

/**
 * Index Nested Loop join translator.
 */
class IndexJoinTranslator : public OperatorTranslator {
 public:
  IndexJoinTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen* codegen);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Create an index key struc
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Does nothing
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override {}

  // Produce index scan code.
  void Produce(FunctionBuilder * builder) override;

  // Pass Through
  ast::Expr * GetOutput(uint32_t attr_idx) override;

  ast::Expr * GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

 private:
  // Declare the index iterator
  void DeclareIterator(FunctionBuilder * builder);
  // Declare the index key
  void DeclareKey(FunctionBuilder * builder);
  // Fill the key with table data
  void FillKey(FunctionBuilder * builder);
  // Generate the index iteration loop
  void GenForLoop(FunctionBuilder * builder);
  // Free the iterator
  void FreeIterator(FunctionBuilder * builder);

  // Structs and local variables
  static constexpr const char * iter_name_ = "index_iter";
  static constexpr const char * index_key_name_ = "index_key";
  static constexpr const char * index_struct_name_ = "IndexKey";
  static constexpr const char * index_key_prefix_ = "key";
  ast::Identifier index_iter_;
  ast::Identifier index_struct_;
  ast::Identifier index_key_;
};
}