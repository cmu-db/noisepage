#pragma once

#include "planner/plannodes/seq_scan_plan_node.h"
#include "execution/compiler/operator/operator_translator.h"
#include "parser/expression/tuple_value_expression.h"

namespace tpl::compiler {

/**
 * SeqScan Translator
 */
class SeqScanTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node
   * @param pipeline current pipeline
   */
  SeqScanTranslator(const terrier::planner::AbstractPlanNode * op, CodeGen * codegen);

  void Produce(FunctionBuilder * builder) override;

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

  // Get an attribute by making a pci call
  ast::Expr* GetOutput(uint32_t attr_idx) override;

  // For a seq scan, this a pci call too
  ast::Expr* GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  // This is a materializer
  bool IsMaterializer(bool * is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // This is vectorizable only if the scan is vectorizable
  bool IsVectorizable() override {
    return is_vectorizable_;
  }

  // Return the pci and its type
  std::pair<ast::Identifier*, ast::Identifier*> GetMaterializedTuple() override {
    return {&pci_, &pci_type_};
  }

  /**
   * Used by vectorized operators to declare a pci
   * @param builder function builder to use
   * @param iters an array of pci
   */
  void DeclarePCIVec(FunctionBuilder * builder, ast::Identifier iters);

  /**
   * Used by vectorized operators to set the pci's position
   * @param builder function builder to use
   * @param index position to set
   */
  void SetPCIPosition(FunctionBuilder * builder, ast::Identifier index);

  void GenVectorizedLoop(FunctionBuilder * builder, );

 private:
  // var tvi : TableVectorIterator
  void DeclareTVI(FunctionBuilder * builder);

  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  void GenTVILoop(FunctionBuilder * builder);

  void DeclarePCI(FunctionBuilder * builder);


  // var pci = @tableIterGetPCI(&tvi)
  // for (; @pciHasNext(pci); @pciAdvance(pci)) {...}
  void GenPCILoop(FunctionBuilder * builder);

  // if (cond) {...}
  void GenScanCondition(FunctionBuilder * builder);

  // @tableIterClose(&tvi)
  void GenTVIClose(FunctionBuilder * builder);

  // Whether the seq scan can be vectorized
  static bool IsVectorizable(const terrier::parser::AbstractExpression * predicate);

  // Generated vectorized filters
  void GenVectorizedPredicate(FunctionBuilder * builder, const terrier::parser::AbstractExpression * predicate);

  // Declare the iters variable for vectorized execution
  void DeclareIters(FunctionBuilder * builder);

  // Whether there is a scan predicate.
  const terrier::planner::SeqScanPlanNode * seqscan_op_;
  bool has_predicate_;
  bool is_vectorizable_;
  bool is_vectorized;

  // Structs, functions and locals
  static constexpr const char * tvi_name_ = "tvi";
  static constexpr const char * pci_name_ = "pci";
  static constexpr const char * row_name_ = "row";
  static constexpr const char * table_struct_name_= "TableRow";
  static constexpr const char * pci_type_name_ = "ProjectedColumnsIterator";
  ast::Identifier tvi_;
  ast::Identifier pci_;
  ast::Identifier row_;
  ast::Identifier table_struct_;
  ast::Identifier pci_type_;
};

}  // namespace tpl::compiler
