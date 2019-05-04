#pragma once

namespace tpl::ast {
class Expr;
}

namespace tpl::compiler {

class CodeGen;

class QueryState {
 public:
  void FinalizeType(CodeGen *);
  ast::Expr *GetType();
 private:
};

}