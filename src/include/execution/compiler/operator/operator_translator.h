#pragma once

#include "execution/compiler/codegen.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::execution::compiler {

/**
 * Generic Operator Translator
 * TODO(Amadou): Only a few operations need all of these methods at once (sorting, aggregations, hash joins).
 * Other operations need just a few of them. So we could add a default implementation.
 * For now, I am leaving it like this so that the compiler will force me to think about all methods.
 */
class OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op operator to translate
   * @param pipeline current pipeline
   */
  OperatorTranslator(const terrier::planner::AbstractPlanNode *op, CodeGen *codegen) : op_(op), codegen_(codegen) {}

  /**
   * Destructor
   */
  virtual ~OperatorTranslator() = default;

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
   * Consume code for the operator
   * @param builder builder of the pipeline function
   */
  virtual void Consume(FunctionBuilder *builder) = 0;

  /**
   * Casts an operator to a given type
   * @tparam T type to cast to
   * @return the casted operator
   */
  template <typename T>
  const T &GetOperatorAs() const {
    return static_cast<const T &>(op_);
  }

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
   * Whether this operator materializes structs.
   * Currently, this is used to simplify the probe phase of hash joins. The right side of the join does not have
   * to materialize a tuple if the right child already materialized it.
   * But I suspect it can simplify sorters, and perhaps other things.
   * Currently, SeqScan, Agg, and Sort are the materializers.
   * Most operators should return false here.
   * @param is_ptr[out] whether the function outputs a pointer.
   */
  virtual bool IsMaterializer(bool *is_ptr) {
    *is_ptr = false;
    return false;
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
  virtual ast::Expr *GetTableColumn(const catalog::col_oid_t &col_oid) {
    UNREACHABLE("This operator does not interact with tables");
  }

  /**
   * Return the identifiers of the materialized tuple if this a materializer.
   * The first element is the identifer of the local variable.
   * The second element is the identifier of its type.
   * Most operators should not be materializers.
   */
  virtual std::pair<ast::Identifier *, ast::Identifier *> GetMaterializedTuple() { return {nullptr, nullptr}; }

  /**
   * @param attr_idx index into the output schema
   * @return the output at the given index
   */
  virtual ast::Expr *GetOutput(uint32_t attr_idx) = 0;

  /**
   * @param child_idx index of the child (0 or 1)
   * @param attr_idx index of the child's output
   * @param type type of the attribute
   * @return the child's output at the given index
   */
  virtual ast::Expr *GetChildOutput(uint32_t child_idx, uint32_t attr_idx, terrier::type::TypeId type) = 0;

  /**
   * Used by operators when they need to generate a struct containing a child's output.
   * Also used by the output layer to materialize the output
   * @param fields where to append the fields
   * @param prefix of the field name
   */
  void GetChildOutputFields(util::RegionVector<ast::FieldDecl *> *fields, const std::string &prefix) {
    uint32_t attr_idx = 0;
    for (const auto &col : child_translator_->op_->GetOutputSchema()->GetColumns()) {
      ast::Identifier field_name = codegen_->Context()->GetIdentifier(prefix + std::to_string(attr_idx));
      ast::Expr *type = codegen_->TplType(col.GetExpr()->GetReturnValueType());
      fields->emplace_back(codegen_->MakeField(field_name, type));
      attr_idx++;
    }
  }

 protected:
  /**
   * The plan node
   */
  const terrier::planner::AbstractPlanNode *op_;

  /**
   * The code generator to use
   */
  CodeGen *codegen_;

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
