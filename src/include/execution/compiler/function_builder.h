#pragma once

#include <string>
#include "execution/util/region_containers.h"

namespace tpl::ast {
class BlockStmt;
class Expr;
}

namespace tpl::compiler {

class CodeContext;

class FunctionBuilder {
 public:
  FunctionBuilder(CodeContext *code_ctx, std::string name, ast::Expr *ret_type);

  void ReturnAndFinish(ast::Expr *ret_val);

 private:
  CodeContext *code_ctx_;

  ast::BlockStmt *stmt;
};

}