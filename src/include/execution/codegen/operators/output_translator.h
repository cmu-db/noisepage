#pragma once

#include <string_view>
#include <vector>

#include "execution/codegen/ast_fwd.h"
#include "execution/codegen/operators/operator_translator.h"
#include "execution/codegen/pipeline.h"

namespace terrier::parser {
class AbstractExpression;
}  // namespace terrier::parser

namespace terrier::execution::codegen {

class FunctionBuilder;

/**
 * A translator for outputting tuples.
 */
class OutputTranslator : public OperatorTranslator {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  OutputTranslator(const planner::AbstractPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(OutputTranslator);

  /**
   * Define the output struct.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Perform the main work of the translator.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Output translator needs to finalize the output.
   */
  void FinishPipelineWork(const Pipeline &pipeline, FunctionBuilder *function) const override;

  /**
   * Does not interact with tables.
   */
  ast::Expr *GetTableColumn(uint16_t col_oid) const override { UNREACHABLE("Output does not interact with tables."); }

 private:
  ast::Identifier output_var_;
  ast::Identifier output_struct_;
};

}  // namespace terrier::execution::codegen
