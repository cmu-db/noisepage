#include "execution/compiler/pipeline.h"

#include <memory>
#include <utility>
#include <vector>

namespace terrier::execution::compiler {
void Pipeline::Initialize(util::RegionVector<ast::Decl *> *decls, util::RegionVector<ast::FieldDecl *> *state_fields,
                          util::RegionVector<ast::Stmt *> *setup_stmts,
                          util::RegionVector<ast::Stmt *> *teardown_stmts) {
  for (uint32_t i = 0; i < pipeline_.size(); i++) {
    // Get previous, current, and parent translator
    OperatorTranslator *child_translator = nullptr;
    OperatorTranslator *parent_translator = nullptr;
    OperatorTranslator *curr_translator = pipeline_[i].get();
    if (i > 0) child_translator = pipeline_[i - 1].get();
    if (i < pipeline_.size() - 1) parent_translator = pipeline_[i + 1].get();

    // Initialize
    curr_translator->Prepare(child_translator, parent_translator, is_vectorizable_, is_parallelizable_);
    curr_translator->InitializeStateFields(state_fields);
    curr_translator->InitializeStructs(decls);
    curr_translator->InitializeHelperFunctions(decls);
    curr_translator->InitializeSetup(setup_stmts);
    curr_translator->InitializeTeardown(teardown_stmts);
  }
}

ast::Decl *Pipeline::Produce(uint32_t pipeline_idx) {
  pipeline_idx_ = pipeline_idx;
  // Function name
  ast::Identifier fn_name = GetPipelineName();

  // Function parameter
  util::RegionVector<ast::FieldDecl *> params = codegen_->ExecParams();

  // Function return type (nil)
  ast::Expr *ret_type = codegen_->BuiltinType(ast::BuiltinType::Kind::Nil);

  FunctionBuilder builder{codegen_, fn_name, std::move(params), ret_type};

  // for (const auto & translator: pipeline_) {
  pipeline_[pipeline_.size() - 1]->Produce(&builder);
  //}
  return builder.Finish();
}

}  // namespace terrier::execution::compiler
