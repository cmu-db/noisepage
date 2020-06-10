#pragma once

#include <utility>
#include "execution/compiler/operator/aggregate_util.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

// Forward declare
class AggregateTopTranslator;

/**
 * Aggregate Bottom Translator
 * This translator is responsible for the build phase.
 */
class AggregateBottomTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node to translate
   * @param codegen code generator
   */
  AggregateBottomTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen);

  // Declare the hash tables
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare the values and payload structs
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Create the key check functions.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Initialize all hash tables.
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Free all hash tables.
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  void Produce(FunctionBuilder *builder) override;
  void Abort(FunctionBuilder *builder) override;
  void Consume(FunctionBuilder *builder) override;

  // Pass through to the child
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  // Should not be called.
  ast::Expr *GetOutput(uint32_t attr_idx) override {
    UNREACHABLE("Use the more descriptive 'GetGroupByOutput' and 'GetAggregateOutput instead'");
  }

  // This is materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = true;
    return true;
  }

  // Return the payload and its type
  std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() override {
    auto global_aht = helper_.GetGlobalAHT();
    return {&global_aht->Entry(), &global_aht->StructType()};
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

  /**
   * @returns struct declaration
   */
  ast::StructDecl *GetStructDecl() const { return struct_decl_; }

 private:
  /**
   * Return the group by output at the given index
   * @param idx index of the term
   * @return the group by term at the given index
   */
  ast::Expr *GetGroupByOutput(uint32_t idx);

  /**
   * Return the aggregate output at the given index
   * @param idx index of the term
   * @return the agg by term at the given index
   */
  ast::Expr *GetAggregateOutput(uint32_t idx);

  // Make the top translator a friend class.
  friend class AggregateTopTranslator;

 private:
  const planner::AggregatePlanNode *op_;
  AggregateHelper helper_;

  // Struct Decl
  ast::StructDecl *struct_decl_;
};

/**
 * Aggregate Top Translator
 * This aggregator is responsible for the probe phase.
 */
class AggregateTopTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node
   * @param codegen The code generator
   * @param bottom The corresponding bottom translator
   */
  AggregateTopTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen, OperatorTranslator *bottom)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE),
        op_(op),
        bottom_(dynamic_cast<AggregateBottomTranslator *>(bottom)),
        agg_iterator_("agg_iter") {}

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

  // Let the bottom translator handle these call
  ast::Expr *GetOutput(uint32_t attr_idx) override;
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override;

  // This is a materializer
  bool IsMaterializer(bool *is_ptr) override {
    *is_ptr = false;
    return true;
  }

  // Pass the call to the bottom translator.
  std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() override {
    return bottom_->GetMaterializedTuple();
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  // Declare var agg_iterator: *AggregationHashTableIterator
  void DeclareIterator(FunctionBuilder *builder);

  // for (@aggHTIterInit(agg_iter, &state.table); @aggHTIterHasNext(&agg_iter); @aggHTIterNext(agg_iter)) {...}
  void GenHTLoop(FunctionBuilder *builder);

  // Declare var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(&agg_iter))
  void DeclareResult(FunctionBuilder *builder);

  // Call @aggHTIterClose(&agg_iter)
  void CloseIterator(FunctionBuilder *builder);

  // Generate an if statement for the having clause
  // Return true iff there is a having clause
  bool GenHaving(FunctionBuilder *builder);

  const planner::AggregatePlanNode *op_;
  // Used to access member of the resulting aggregate
  AggregateBottomTranslator *bottom_;

  // Structs, Functions, and local variables needed.
  ast::Identifier agg_iterator_;
};
}  // namespace terrier::execution::compiler
