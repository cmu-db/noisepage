#pragma once

#include "execution/compiler/code_context.h"
#include "execution/util/macros.h"

namespace tpl::compiler {

class CodeContext;

class FunctionDeclaration {

};

class FunctionBuilder {
  friend class CodeGen;
 public:
  FunctionBuilder(CodeContext *ctx, std::string name, ast::Type *ret_type, util::RegionVector<ast::FieldDecl> args);

  DISALLOW_COPY_AND_MOVE(FunctionBuilder);

  void ReturnAndFinish(ast::Expr *ret);

 private:
  bool finished_;
  CodeContext *ctx_;
  FunctionBuilder *prev_fn_;


};


}