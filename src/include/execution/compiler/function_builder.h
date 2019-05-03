#pragma once

#include "execution/compiler/code_context.h"
#include "execution/compiler/codegen.h"
#include "execution/util/macros.h"

namespace tpl::compiler {

class CodeContext;

class FunctionDeclaration {};

class FunctionBuilder {
  friend class CodeGen;

 public:
  FunctionBuilder(CodeContext *ctx, std::string &name, ast::Type *ret_type, util::RegionVector<ast::FieldDecl*> &args,
                  tpl::compiler::CodeGen *codeGen);

  DISALLOW_COPY_AND_MOVE(FunctionBuilder);

  void ReturnAndFinish(ast::Expr *ret);

 private:
  bool finished_;
  CodeContext *ctx_;
  CodeGen *codeGen_;
  ast::Type *ret_type_;
  util::RegionVector<ast::FieldDecl*> args_;
  std::string name_;
};

}  // namespace tpl::compiler