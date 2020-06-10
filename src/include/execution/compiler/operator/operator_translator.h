#pragma once
#include <string>
#include <utility>
#include "brain/brain_defs.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/expression/expression_translator.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Generic Operator Translator
 * TODO(Amadou): Only a few operations need all of these methods at once (sorting, aggregations, hash joins).
 * Other operations need just a few of them. So we could add a default implementation.
 * Default implementations make it easier to forget about something though, so I am leaving it like this for now.
 */
class OperatorTranslator : public ExpressionEvaluator {
 public:
  /**
   * Constructor
   * @param codegen The code generator to use
   * @param feature Feature Type
   */
  explicit OperatorTranslator(CodeGen *codegen, brain::ExecutionOperatingUnitType feature)
      : codegen_(codegen), feature_type_(feature) {}

  /**
   * Destructor
   */
  virtual ~OperatorTranslator() = default;

  /**
   * @returns child translator
   */
  OperatorTranslator *GetChildTranslator() const { return child_translator_; }

  /**
   * Add top-level struct declarations
   * @param decls list of top-level declarations
   */
  virtual void InitializeStructs(util::RegionVector<ast::Decl *> *decls) = 0;

  /**
   * Add top-level helper function
   * @param decls list of top-level declarations
   */
  virtual void InitializeHelperFunctions(util::RegionVector<ast::Decl *> *decls) = 0;

  /**
   * Add statements to the setup function
   * @param setup_stmts list of statements in the setup function
   */
  virtual void InitializeSetup(util::RegionVector<ast::Stmt *> *setup_stmts) = 0;

  /**
   * Add statements to the teardown function
   * @param teardown_stmts list of statements in the teardown function
   */
  virtual void InitializeTeardown(util::RegionVector<ast::Stmt *> *teardown_stmts) = 0;

  /**
   * Add fields to the state struct
   * @param state_fields list of fields of the state struct
   */
  virtual void InitializeStateFields(util::RegionVector<ast::FieldDecl *> *state_fields) = 0;

  /**
   * Produce code for the operator
   * @param builder builder of the pipeline function
   */
  virtual void Produce(FunctionBuilder *builder) = 0;

  /**
   * Do cleanup for abort
   * @param builder builder of the pipeline function
   */
  virtual void Abort(FunctionBuilder *builder) = 0;

  /**
   * Consume code for the operator
   * @param builder builder of the pipeline function
   */
  virtual void Consume(FunctionBuilder *builder) = 0;

  /**
   * Setup state needed before generating code
   * @param child_translator the child translator
   * @param parent_translator the parent translator
   * @param vectorize whether the pipeline is vectorized
   * @param parallelize whether the pipeline is paralellized
   */
  void Prepare(OperatorTranslator *child_translator, OperatorTranslator *parent_translator, bool vectorize,
               bool parallelize) {
    child_translator_ = child_translator;
    parent_translator_ = parent_translator;
    vectorized_pipeline_ = vectorize;
    parallelized_pipeline_ = parallelize;
  }

  /**
   * @return Whether this operator is vectorizable
   */
  virtual bool IsVectorizable() { return false; }

  /**
   * @return Whether this operator is parallelizable
   */
  virtual bool IsParallelizable() { return false; }

  /**
   * Return a table column value.
   * @param col_oid oid of the column
   * @return an expression representing the value
   */
  ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) override {
    UNREACHABLE("This operator does not interact with tables");
  }

  /**
   * @return The current tuple slot.
   */
  virtual ast::Expr *GetSlot() { UNREACHABLE("This operator does not interact with tables"); }

  /**
   * @param attr_idx index into the output schema
   * @return the output at the given index
   */
  virtual ast::Expr *GetOutput(uint32_t attr_idx) = 0;

  /**
   * Whether this operator materializes structs.
   * Currently, this is used to simplify the probe phase of hash joins. The right side of the join does not have
   * to materialize a tuple if the right child already materialized it.
   * @param is_ptr whether the function outputs a pointer.
   * @return Whether this operator is a materializer.
   */
  virtual bool IsMaterializer(bool *is_ptr) {
    *is_ptr = false;
    return false;
  }

  /**
   * Return the identifiers of the materialized tuple if this a materializer.
   * The first element is the identifer of the local variable.
   * The second element is the identifier of its type.
   * Most operators should not be materializers.
   * @return a pair containing the identifiers of the output tuple and its type.
   */
  virtual std::pair<const ast::Identifier *, const ast::Identifier *> GetMaterializedTuple() {
    return {nullptr, nullptr};
  }

  /**
   * Used by operators when they need to generate a struct containing a child's output.
   * Also used by the output layer to materialize the output
   * @param fields where to append the fields
   * @param prefix of the field name
   */
  void GetChildOutputFields(util::RegionVector<ast::FieldDecl *> *fields, const std::string &prefix) {
    uint32_t attr_idx = 0;
    for (const auto &col : child_translator_->Op()->GetOutputSchema()->GetColumns()) {
      ast::Identifier field_name = codegen_->Context()->GetIdentifier(prefix + std::to_string(attr_idx));
      ast::Expr *type = codegen_->TplType(col.GetExpr()->GetReturnValueType());
      fields->emplace_back(codegen_->MakeField(field_name, type));
      attr_idx++;
    }
  }

  /**
   * @return the plan node as a generic node
   */
  virtual const planner::AbstractPlanNode *Op() = 0;

  /**
   * @return feature type
   */
  brain::ExecutionOperatingUnitType GetFeatureType() const { return feature_type_; }

 protected:
  /**
   * The code generator to use
   */
  CodeGen *codegen_;

  /**
   * ExecutionOperatingUnitType
   */
  brain::ExecutionOperatingUnitType feature_type_{brain::ExecutionOperatingUnitType::INVALID};

  /**
   * The child operator translator.
   */
  OperatorTranslator *child_translator_{nullptr};

  /**
   * The parent operator translator
   */
  OperatorTranslator *parent_translator_{nullptr};

  /**
   * Whether the whole pipeline is produced in vectorized mode
   */
  bool vectorized_pipeline_{false};

  /**
   * Whether the whole pipeline is produced in parallel mode
   */
  bool parallelized_pipeline_{false};
};
}  // namespace terrier::execution::compiler
