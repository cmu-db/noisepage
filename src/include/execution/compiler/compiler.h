#pragma once

#include <memory>
#include <vector>
#include "execution/compiler/expression/expression_translator.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/operator/output_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/util/region.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::execution::compiler {
/**
 * Compiler is the main class that performs compilation through GeneratePlan.
 */
class Compiler {
 public:
  /**
   * Constructor
   * @param codegen The code generator
   * @param plan The plan node to execute
   */
  Compiler(CodeGen *codegen, const planner::AbstractPlanNode *plan);

  /**
   * Convert the plan to AST and type check it
   */
  ast::File *Compile();

 private:
  // Create the pipelines
  void MakePipelines(const terrier::planner::AbstractPlanNode &op, Pipeline *curr_pipeline);
  // Generate the state struct
  void GenStateStruct(util::RegionVector<ast::Decl *> *top_level, util::RegionVector<ast::FieldDecl *> &&fields);
  // Generate the top level helpers
  void GenHelperStructsAndFunctions(util::RegionVector<ast::Decl *> *top_level,
                                    util::RegionVector<ast::Decl *> &&decls);
  // Generate the global functions (setup, teardown, ...)
  void GenFunction(util::RegionVector<ast::Decl *> *top_level, ast::Identifier fn_name,
                   util::RegionVector<ast::Stmt *> &&stmts);
  // Generate main.
  ast::Decl *GenMainFunction();
  CodeGen *codegen_;
  const planner::AbstractPlanNode *plan_;
  std::vector<std::unique_ptr<Pipeline>> pipelines_;
};

}  // namespace terrier::execution::compiler
