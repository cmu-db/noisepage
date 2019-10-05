#pragma once

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
   * @param query query to compiler
   * @param consumer consumer for upper layers
   */
  Compiler(CodeGen* codegen, const planner::AbstractPlanNode* plan);

  /**
   * Convert the plan to AST and type check it
   */
  ast::File* Compile();

 private:
  void MakePipelines(const terrier::planner::AbstractPlanNode &op, Pipeline *curr_pipeline);
  void GenStateStruct(util::RegionVector<ast::Decl *> *top_level, util::RegionVector<ast::FieldDecl *> &&fields);
  void GenHelperStructsAndFunctions(util::RegionVector<ast::Decl *> *top_level,
                                    util::RegionVector<ast::Decl *> &&decls);
  void GenFunction(util::RegionVector<ast::Decl *> *top_level, ast::Identifier fn_name,
                   util::RegionVector<ast::Stmt *> &&stmts);
  ast::Decl *GenMainFunction();
  CodeGen* codegen_;
  const planner::AbstractPlanNode* plan_;
  std::vector<std::unique_ptr<Pipeline>> pipelines_;
};

}  // namespace terrier::execution::compiler