#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "planner/plannodes/cte_scan_plan_node.h"

namespace terrier::execution::compiler {

/**
 * CteScan Translator
 */
class CteScanTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  CteScanTranslator(const terrier::planner::CteScanPlanNode *op, CodeGen *codegen);

  // Pass through
  void Produce(FunctionBuilder *builder) override;

  // Pass through
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }

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

  ast::Expr *GetOutput(uint32_t attr_idx) override {
    // We need to do this as the output schema can be only one thing
    // Either a constant value expression or a derived value expression
    // Leader is given preference - As that node is the LEADER..!!

    auto type = op_->GetOutputSchema()->GetColumn(attr_idx).GetType();
    auto name = op_->GetOutputSchema()->GetColumn(attr_idx).GetName();

    auto nullable = false;
    // ToDo(Rohan) : Think if this can be simplified
    uint16_t projection_map_index = projection_map_[static_cast<catalog::col_oid_t>(col_name_to_oid_[name])];
    return codegen_->PCIGet(read_pci_, type, nullable, projection_map_index);
  }

  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return child_translator_->GetOutput(attr_idx);
  }

  // Is always vectorizable.
  bool IsVectorizable() override { return true; }

  // Should not be called here
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override {
    UNREACHABLE("Projection nodes should not use column value expressions");
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  const planner::CteScanPlanNode *op_;
  // Declare Cte Scan Itarator
  void DeclareCteScanIterator(FunctionBuilder *builder);
  // Set Column Types for insertion
  void SetColumnTypes(FunctionBuilder *builder);
  // Declare the insert PR
  void DeclareInsertPR(FunctionBuilder *builder);
  // Get the pr to insert
  void GetInsertPR(FunctionBuilder *builder);
  // Fill the insert PR from the child's output
  void FillPRFromChild(FunctionBuilder *builder);
  // Insert into table.
  void GenTableInsert(FunctionBuilder *builder);
  ast::Identifier col_types_;
  std::vector<int> all_types_;
  ast::Identifier insert_pr_;
  std::vector<catalog::col_oid_t> col_oids_;
  std::unordered_map<std::string, uint32_t> col_name_to_oid_;
  storage::ProjectionMap projection_map_;
  ast::Identifier read_col_oids_;
  ast::Identifier read_tvi_;
  ast::Identifier read_pci_;
  void SetReadOids(FunctionBuilder *builder);
  void DeclareReadTVI(FunctionBuilder *builder);
  void GenReadTVIClose(FunctionBuilder *builder);
  void DoTableScan(FunctionBuilder *builder);

  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  void GenTVILoop(FunctionBuilder *builder);

  void DeclarePCI(FunctionBuilder *builder);
  void DeclareSlot(FunctionBuilder *builder);

  // var pci = @tableIterGetPCI(&tvi)
  // for (; @pciHasNext(pci); @pciAdvance(pci)) {...}
  void GenPCILoop(FunctionBuilder *builder);

  void GenReadTVIReset(FunctionBuilder *builder);
};

}  // namespace terrier::execution::compiler
