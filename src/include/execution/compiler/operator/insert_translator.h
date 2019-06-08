#pragma once

#include "execution/compiler/operator/operator_translator.h"

namespace tpl::ast {
class StructTypeRepr;
}

namespace tpl::compiler {

/**
 * Insert Translator
 */
class InsertTranslator : public OperatorTranslator {
 public:
  /**
   * Constructor
   * @param op plan node
   * @param pipeline current pipeline
   */
  InsertTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);

  /**
   * Initialize the query state.
   * For insert, we just need to define a insert struct.
   */
  void InitializeQueryState() override;

  /**
   * Tears down the query state.
   * For insert, nothing is needed.
   */
  void TeardownQueryState() override {}

  /**
   * Only called for insert-from-select
   * TODO(WAN): in Peloton, insert-from-select would invoke the select to Produce() and then Consume() those tuples
   * @param context consumer context to use
   * @param batch tuple to consume
   */
  void Consume(const ConsumerContext *context, RowBatch *batch) const override;

  /**
   * Generates insertion code
   * First we fill up a struct, then make the builtin insert call.
   */
  void Produce() override;

 private:
  ast::StructTypeRepr *struct_ty_;
};

}  // namespace tpl::compiler
