#pragma once

namespace tpl::ast {
class Expr;
}

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class CodeGen;

class QueryState {
 public:
  QueryState(util::Region *region);
  void FinalizeType(CodeGen *);
  ast::Expr *GetType();
 private:
};

}