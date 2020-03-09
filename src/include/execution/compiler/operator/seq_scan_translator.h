#pragma once

#include <utility>
#include <vector>
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
   * @param op The plan node
   * @param codegen The code generator
   */
  SeqScanTranslator(const terrier::planner::SeqScanPlanNode *op, CodeGen *codegen);

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
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

  // This is vectorizable only if the predicate is vectorizable
  bool IsVectorizable() override { return is_vectorizable_; }
  /**
   * Recursively walk down the predicate tree to check if it is vectorizable.
   * @param predicate The predicate to check
   * @return Whether the predicate is vectorizable or not.
   */
  static bool IsVectorizable(const terrier::parser::AbstractExpression *predicate);

  // Return the pci and its type
  std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() override {
    return {&pci_, &pci_type_};
  }

  // Used by column value expression to get a column.
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override;

  // Return the current slot.
  ast::Expr *GetSlot() override { return codegen_->PointerTo(slot_); }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  // var tvi : TableVectorIterator
  void DeclareTVI(FunctionBuilder *builder);

  void SetOids(FunctionBuilder *builder);

  void DoTableScan(FunctionBuilder *builder);

  // for (@tableIterInit(&tvi, ...); @tableIterAdvance(&tvi);) {...}
  void GenTVILoop(FunctionBuilder *builder);

  void DeclarePCI(FunctionBuilder *builder);
  void DeclareSlot(FunctionBuilder *builder);

  // var pci = @tableIterGetPCI(&tvi)
  // for (; @pciHasNext(pci); @pciAdvance(pci)) {...}
  void GenPCILoop(FunctionBuilder *builder);

  // if (cond) {...}
  void GenScanCondition(FunctionBuilder *builder);

  // @tableIterClose(&tvi)
  void GenTVIClose(FunctionBuilder *builder);

  // @tableIterReset(&tvi)
  void GenTVIReset(FunctionBuilder *builder);

  // Generated vectorized filters
  void GenVectorizedPredicate(FunctionBuilder *builder, const terrier::parser::AbstractExpression *predicate);

  // Create the input oids used for the scans.
  // When the plan's oid list is empty (like in "SELECT COUNT(*)"), then we just read the first column of the table.
  // Otherwise we just read the plan's oid list.
  // This is because the storage layer needs to read at least one column.
  // TODO(Amadou): Create a special code path for COUNT(*).
  // This requires a new table iterator that doesn't materialize tuples as well as a few builtins.
  static std::vector<catalog::col_oid_t> MakeInputOids(const catalog::Schema &schema,
                                                       const planner::SeqScanPlanNode *op_) {
    if (op_->GetColumnOids().empty()) {
      return {schema.GetColumn(0).Oid()};
    }
    return op_->GetColumnOids();
  }

 private:
  const planner::SeqScanPlanNode *op_;
  const catalog::Schema &schema_;
  std::vector<catalog::col_oid_t> input_oids_;
  storage::ProjectionMap pm_;
  bool has_predicate_;
  bool is_vectorizable_;

  // Structs, functions and locals
  ast::Identifier tvi_;
  ast::Identifier col_oids_;
  ast::Identifier pci_;
  ast::Identifier slot_;
  ast::Identifier pci_type_;
};

}  // namespace terrier::execution::compiler
