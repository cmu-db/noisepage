#pragma once

#include "execution/compiler/operator/operator_translator.h"

namespace tpl::ast {
class StructTypeRepr;
}

namespace tpl::compiler {

class InsertTranslator : public OperatorTranslator {
 public:
  InsertTranslator(const terrier::planner::AbstractPlanNode &op, Pipeline *pipeline);

  void InitializeQueryState() override;
  void TeardownQueryState() override {}

  // TODO(WAN): in Peloton, insert-from-select would invoke the select to Produce() and then Consume() those tuples
  void Consume(const ConsumerContext *context, RowBatch *batch) const override;

  void Produce() override;
 private:
  ast::StructTypeRepr *struct_ty_;
};


} // namespace tpl::compiler