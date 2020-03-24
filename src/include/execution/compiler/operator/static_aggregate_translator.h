#pragma once

#include <unordered_map>
#include <utility>
#include "execution/compiler/operator/aggregate_util.h"
#include "execution/compiler/operator/operator_translator.h"
#include "planner/plannodes/aggregate_plan_node.h"

namespace terrier::execution::compiler {

// Forward declare
class StaticAggregateTopTranslator;

/**
 * A static aggregation is one in which there is no group by term. It differs from a regular aggregation in that
 * there is no aggregation hash table and that there must be an output.
 */
class StaticAggregateBottomTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node to translate
   * @param codegen code generator
   */
  StaticAggregateBottomTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen);

  // Declare aggregates and distinct hash tables.
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare the values struct and the payload of each distinct aggregate
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Initialize comparison functions for distinct aggregates.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Initialize the aggregates
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Free Distinct
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  // Pass Through
  void Produce(FunctionBuilder *builder) override { child_translator_->Produce(builder); };
  // Pass Through
  void Abort(FunctionBuilder *builder) override { child_translator_->Abort(builder); }
  void Consume(FunctionBuilder *builder) override;

  // Pass through to the child
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return child_translator_->GetOutput(attr_idx);
  }

  // Return the attribute at idx
  ast::Expr *GetOutput(uint32_t attr_idx) override {
    ast::Expr *agg = codegen_->GetStateMemberPtr(helper_.GetAggregate(attr_idx));
    return codegen_->OneArgCall(ast::Builtin::AggResult, agg);
  }

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  friend class StaticAggregateTopTranslator;
  const planner::AggregatePlanNode *op_;
  AggregateHelper helper_;
};

/**
 * The iteration side of the static aggregate.
 */
class StaticAggregateTopTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node
   * @param codegen The code generator
   * @param bottom The corresponding bottom translator
   */
  StaticAggregateTopTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen,
                               OperatorTranslator *bottom)
      : OperatorTranslator(codegen, brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE),
        op_(op),
        bottom_(static_cast<StaticAggregateBottomTranslator *>(bottom)) {}

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

  // Generate the havinf clause
  void Produce(FunctionBuilder *builder) override;

  // Pass through
  void Abort(FunctionBuilder *builder) override {
    if (child_translator_ != nullptr) child_translator_->Abort(builder);
  }

  // Pass through
  void Consume(FunctionBuilder *builder) override { parent_translator_->Consume(builder); }

  // Pass through to the child
  ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) override {
    return bottom_->GetOutput(attr_idx);
  }

  // Return the attribute at idx
  ast::Expr *GetOutput(uint32_t attr_idx) override;

  const planner::AbstractPlanNode *Op() override { return op_; }

 private:
  const planner::AggregatePlanNode *op_;
  StaticAggregateBottomTranslator *bottom_;
};

}  // namespace terrier::execution::compiler
