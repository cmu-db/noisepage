#pragma once

#include <string>
#include "execution/ast/ast.h"
#include "execution/util/region_containers.h"

namespace tpl::compiler {

class CodeGen;

class FunctionBuilder {
 public:
  FunctionBuilder(CodeGen &code_ctx,
                  ast::Identifier fn_name,
                  util::RegionVector<ast::FieldDecl *> fn_params,
                  ast::Expr *fn_ret_type);

  ast::FunctionDecl *Finish();

 private:
  CodeGen &codegen_;
  FunctionBuilder *prev_fn_;

  ast::Identifier fn_name_;
  util::RegionVector<ast::FieldDecl *> fn_params_;
  ast::Expr *fn_ret_type_;

  util::RegionVector<ast::Stmt *> fn_body_;
};

}