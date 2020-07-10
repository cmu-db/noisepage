#pragma once
#include <vector>
#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/operator/operator_translator.h"

namespace terrier::planner {
class CreateIndexPlanNode;
}

namespace terrier::execution::compiler {

/**
 * Create Index Translator
 * This translator takes in an CreateIndexPlanNode to perform a sequantial scan and index population. It first
 * initialize a storage interface and create the index through ddl_executor. Based on the GetIndexPR and InitTablePR in
 * the storage_interface, index PR and table PR are fetched/initialized. Then a sequantial scan similar to
 * seq_scan_translator is performed to scan through tuples. For each tuple, FillTablePR in the storage_interface is
 * called to fill the index PR. Finally we'll insert into the index given the PR and the tuple.
 */

class CreateIndexTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op The plan node
   * @param codegen The code generator
   */
  CreateIndexTranslator(const terrier::planner::CreateIndexPlanNode *op, CodeGen *codegen);

  // Does nothing
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override {}

  // Does nothing
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override {}

  // Does nothing.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override{};

  // Does nothing
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override {}

  // Does nothing
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override{};

  // Produce and consume logic
  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  const planner::AbstractPlanNode *Op() override;
  ast::Expr *GetOutput(uint32_t attr_idx) override { UNREACHABLE("Create Index don't output anything"); };

  // Should not be called here
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    UNREACHABLE("Create Index nodes does not have a child");
  }

 private:
  // Declare the index_inserter
  void DeclareIndexInserter(FunctionBuilder *builder);
  void DeclareTVI(FunctionBuilder *builder);

  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  void GenTVILoop(FunctionBuilder *builder);

  void DeclarePCI(FunctionBuilder *builder);
  void DeclareSlot(FunctionBuilder *builder);

  // var pci = @tableIterGetPCI(&tvi)
  // for (; @pciHasNext(pci); @pciAdvance(pci)) {...}
  void GenPCILoop(FunctionBuilder *builder);

  // @tableIterReset(&tvi)
  // void GenTVIReset(FunctionBuilder *builder);
  void GenTVIClose(FunctionBuilder *builder);

  void GenIndexInserterFree(FunctionBuilder *builder);
  // Insert into table.
  void GenCreateIndex(FunctionBuilder *builder);

  void GenGetIndexPR(FunctionBuilder *builder);
  void GenGetTablePR(FunctionBuilder *builder);
  void GenFillTablePR(FunctionBuilder *builder);
  void GenIndexInsert(FunctionBuilder *builder);

  void SetOids(FunctionBuilder *builder);

 private:
  const planner::CreateIndexPlanNode *op_;
  ast::Identifier index_inserter_;
  ast::Identifier index_pr_;
  ast::Identifier table_pr_;
  ast::Identifier col_oids_;
  ast::Identifier tvi_;
  ast::Identifier pci_;
  ast::Identifier slot_;
  const catalog::Schema &table_schema_;
  catalog::index_oid_t index_oid_;
  std::vector<catalog::col_oid_t> all_oids_;
  storage::ProjectionMap table_pm_;
  PRFiller pr_filler_;
};

}  // namespace terrier::execution::compiler
