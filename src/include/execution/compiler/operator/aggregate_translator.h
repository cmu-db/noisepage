#pragma once

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

  // Declare the hash table
  void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) override;

  // Declare payload and probe structs
  void InitializeStructs(util::RegionVector<ast::Decl *> *decls) override;

  // Create the key check function.
  void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) override;

  // Call @aggHTInit on the hash table
  void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) override;

  // Call @aggHTFree
  void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) override;

  void Produce(FunctionBuilder *builder) override;

  void Consume(FunctionBuilder *builder) override;

  // Pass through to the child
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
    return {&agg_payload_, &payload_struct_};
  }

  const planner::AbstractPlanNode* Op() override {
    return op_;
  }

 private:
  /**
   * Return the group by term at the given index
   * @param object either agg_payload_ or agg_values_
   * @param idx index of the term
   * @return the group by term at the given index
   */
  ast::Expr *GetGroupByTerm(ast::Identifier object, uint32_t idx);

  /**
   * Return the aggregate term at the given index
   * @param object with agg_payload_ or agg_value_
   * @param idx index of the term
   * @param ptr whether to return a pointer to the term or not
   * @return the group by term at the given index
   */
  ast::Expr *GetAggTerm(ast::Identifier object, uint32_t idx, bool ptr);

  /*
   * Generate the aggregation hash table's payload struct
   */
  void GenPayloadStruct(util::RegionVector<ast::Decl *> *decls);

  /*
   * Generate the aggregation's input values
   */
  void GenValuesStruct(util::RegionVector<ast::Decl *> *decls);

  /*
   * Generate the key check logic
   */
  void GenKeyCheck(FunctionBuilder *builder);

  /*
   * First declare var agg_values : AggValues
   * For each group by term, generate agg_values.term_i = group_by_term_i
   * For each aggregation expression, agg_values.expr_i = agg_expr_i
   */
  void FillValues(FunctionBuilder *builder);

  // Generate var agg_payload = @ptrCast(*AggPayload, @aggHTLookup(&state.agg_ht, agg_hash_val, keyCheck, &agg_values))
  void GenLookupCall(FunctionBuilder *builder);

  /*
   * First check if agg_payload == nil
   * If so, set agg_payload.term_i = agg_values.term_i for each group by terms
   * Add call @aggInit(&agg_payload.expr_i) for each expression
   */
  void GenConstruct(FunctionBuilder *builder);

  /*
   * For each aggregate expression, call @aggAdvance(&agg_payload.expr_i, &agg_values.expr_i)
   */
  void GenAdvance(FunctionBuilder *builder);

  // Generate var agg_hash_val = @hash(groub_by_term1, group_by_term2, ...)
  void GenHashCall(FunctionBuilder *builder);

  // Tuple at a time key check
  void GenSingleKeyCheckFn(util::RegionVector<ast::Decl *> *decls);

  // Make the top translator a friend class.
  friend class AggregateTopTranslator;

 private:
  // The number of group by terms.
  uint32_t num_group_by_terms{0};
  const planner::AggregatePlanNode* op_;

  // Structs, Functions, and local variables needed.
  // TODO(Amadou): This list is blowing up. Figure out a different to manage local variable names.
  static constexpr const char *hash_val_name = "agg_hash_val";
  static constexpr const char *agg_payload_name = "agg_payload";
  static constexpr const char *agg_values_name = "agg_values";
  static constexpr const char *payload_struct_name = "AggPayload";
  static constexpr const char *values_struct_name = "AggValues";
  static constexpr const char *key_check_name = "aggKeyCheckFn";
  static constexpr const char *agg_ht_name = "agg_hash_table";
  static constexpr const char *group_by_term_names = "group_by_term";
  static constexpr const char *agg_term_names = "agg_term";
  ast::Identifier hash_val_;
  ast::Identifier agg_values_;
  ast::Identifier values_struct_;
  ast::Identifier payload_struct_;
  ast::Identifier agg_payload_;
  ast::Identifier key_check_;
  ast::Identifier agg_ht_;
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
   * @param pipeline current pipeline
   */
  AggregateTopTranslator(const terrier::planner::AggregatePlanNode *op, CodeGen *codegen, OperatorTranslator *bottom)
      : OperatorTranslator(codegen),
        op_(op),
        bottom_(dynamic_cast<AggregateBottomTranslator *>(bottom)),
        agg_iterator_(agg_iterator_name) {}

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

  /**
   * First declare the HT iterator
   * For generate the HT loop
   * Finally declare the result of the aggregate.
   * Close the iterator after the loop
   */
  void Produce(FunctionBuilder *builder) override;

  // Pass through
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
  std::pair<ast::Identifier *, ast::Identifier *> GetMaterializedTuple() override {
    return bottom_->GetMaterializedTuple();
  }

  const planner::AbstractPlanNode* Op() override {
    return op_;
  }

 private:
  // Declare var agg_iterator: *AggregationHashTableIterator
  void DeclareIterator(FunctionBuilder *builder);

  // for (@aggHTIterInit(agg_iter, &state.table); @aggHTIterHasNext(agg_iter); @aggHTIterNext(agg_iter)) {...}
  void GenHTLoop(FunctionBuilder *builder);

  // Declare var agg_payload = @ptrCast(*AggPayload, @aggHTIterGetRow(agg_iter))
  void DeclareResult(FunctionBuilder *builder);

  // Call @aggHTIterClose(agg_iter)
  void CloseIterator(FunctionBuilder *builder);

  // Generate an if statement for the having clause
  // Return true if if there is a having clause
  bool GenHaving(FunctionBuilder *builder);

  const planner::AggregatePlanNode* op_;
  // Used to access member of the resulting aggregate
  AggregateBottomTranslator *bottom_;

  // Structs, Functions, and local variables needed.
  static constexpr const char *agg_iterator_name = "agg_iterator";
  ast::Identifier agg_iterator_;
};
}  // namespace terrier::execution::compiler
