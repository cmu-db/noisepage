#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/seq_scan_plan_node.h"

namespace terrier::execution::compiler {

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
  SeqScanTranslator(const terrier::planner::SeqScanPlanNode *op, CodeGen *codegen);

  void Produce(FunctionBuilder *builder) override;

  // Pass through
  void Consume(FunctionBuilder *builder) override;

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

  ast::Expr *GetOutput(uint32_t attr_idx) override;

  // Should not be called here
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    UNREACHABLE("SeqScan nodes should use column value expressions");
  }

  // This is a materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // This is vectorizable only if the scan is vectorizable
  bool IsVectorizable() override { return is_vectorizable_; }

  // Return the pci and its type
  std::pair<ast::Identifier *, ast::Identifier *> GetMaterializedTuple() override { return {&pci_, &pci_type_}; }

  // Used by column value expression to get a column.
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override;

 private:
  // var tvi : TableVectorIterator
  void DeclareTVI(FunctionBuilder *builder);

  void SetOids(FunctionBuilder *builder);

  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  void GenTVILoop(FunctionBuilder *builder);

  void DeclarePCI(FunctionBuilder *builder);

  // var pci = @tableIterGetPCI(&tvi)
  // for (; @pciHasNext(pci); @pciAdvance(pci)) {...}
  void GenPCILoop(FunctionBuilder *builder);

  // if (cond) {...}
  void GenScanCondition(FunctionBuilder *builder);

  // @tableIterClose(&tvi)
  void GenTVIClose(FunctionBuilder *builder);

  // @tableIterReset(&tvi)
  void GenTVIReset(FunctionBuilder *builder);

  // Whether the seq scan can be vectorized
  static bool IsVectorizable(const terrier::parser::AbstractExpression *predicate);

  // Generated vectorized filters
  void GenVectorizedPredicate(FunctionBuilder *builder, const terrier::parser::AbstractExpression *predicate);

  const planner::AbstractPlanNode* Op() override {
    return op_;
  }

 private:
  const planner::SeqScanPlanNode *op_;
  const catalog::Schema &schema_;
  std::vector<catalog::col_oid_t> input_oids_;
  storage::ProjectionMap pm_;
  bool has_predicate_;
  bool is_vectorizable_;

  // Structs, functions and locals
  static constexpr const char *tvi_name_ = "tvi";
  static constexpr const char *col_oids_name_ = "col_oids";
  static constexpr const char *pci_name_ = "pci";
  static constexpr const char *row_name_ = "row";
  static constexpr const char *table_struct_name_ = "TableRow";
  static constexpr const char *pci_type_name_ = "ProjectedColumnsIterator";
  ast::Identifier tvi_;
  ast::Identifier col_oids_;
  ast::Identifier pci_;
  ast::Identifier row_;
  ast::Identifier table_struct_;
  ast::Identifier pci_type_;
};

}  // namespace terrier::execution::compiler
