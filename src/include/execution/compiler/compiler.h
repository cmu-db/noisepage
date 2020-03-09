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
 * Compiler is the main class that performs compilation Compile().
 */
class Compiler {
 public:
  /**
   * Constructor
   * @param query_id query identifier
   * @param codegen The code generator
   * @param plan The plan node to compile
   */
  Compiler(query_id_t query_id, CodeGen *codegen, const planner::AbstractPlanNode *plan);

  /**
   * Convert the plan to AST and type check it
   * @result The generated top-level node.
   */
  ast::File *Compile();

 private:
  /**
   * Recursively walks down the query tree to fill up the vector of pipelines.
   * A pipeline is a sequence of operations that can be performed on a single tuple.
   * @param op The current operation in the recursive calls.
   * @param curr_pipeline The current pipeline being constructed.
   */
  void MakePipelines(const terrier::planner::AbstractPlanNode &op, Pipeline *curr_pipeline);

  // Generate the state struct.
  void GenStateStruct(util::RegionVector<ast::Decl *> *top_level, util::RegionVector<ast::FieldDecl *> &&fields);
  // Generate the global functions (setup, teardown, ...)
  void GenFunction(util::RegionVector<ast::Decl *> *top_level, ast::Identifier fn_name,
                   util::RegionVector<ast::Stmt *> &&stmts);
  // Generate main.
  ast::Decl *GenMainFunction();

  query_id_t query_identifier_;
  CodeGen *codegen_;
  const planner::AbstractPlanNode *plan_;
  std::vector<std::unique_ptr<Pipeline>> pipelines_;
};

}  // namespace terrier::execution::compiler
