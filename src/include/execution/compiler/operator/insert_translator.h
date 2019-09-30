#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/insert_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Insert Translator
 */
class InsertTranslator : public OperatorTranslator {
 public:

  InsertTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen);

  /// Declare the inserter
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare payload and probe structs
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Create the key check function.
  //void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Call @inserterInit on inserter
  // Call @inserterInitBind(&inserter)
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Call @aggHTFree
  //void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  // real stuff
  void Produce(FunctionBuilder *builder) override;

  void Consume(FunctionBuilder *builder) override;

  // This is materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // Return the payload and its type
//  std::pair<ast::Identifier *, ast::Identifier *> GetMaterializedTuple() override {
//    return {&agg_payload_, &payload_struct_};
//  }

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

 private:
  static constexpr const char *inserter_name_ = "inserter";

  ast::Identifier inserter_struct_;

  static constexpr const char *table_pr_name_ "projected_row";
  ast::Identifier table_pr_;
//  const planner::InsertPlanNode *insert_op_;
//  const catalog::Schema &schema_;
//  std::vector<catalog::col_oid_t> input_oids_;
//  storage::ProjectionMap pm_;
//  bool has_predicate_;
//  bool is_vectorizable_;
//
//  // Structs, functions and locals
//  static constexpr const char *tvi_name_ = "tvi";
//  static constexpr const char *col_oids_name_ = "col_oids";
//  static constexpr const char *pci_name_ = "pci";
//  static constexpr const char *row_name_ = "row";
//  static constexpr const char *table_struct_name_ = "TableRow";
//  static constexpr const char *pci_type_name_ = "ProjectedColumnsIterator";
//  ast::Identifier tvi_;
//  ast::Identifier col_oids_;
//  ast::Identifier pci_;
//  ast::Identifier row_;
//  ast::Identifier table_struct_;
//  ast::Identifier pci_type_;
};

}  // namespace tpl::compiler
