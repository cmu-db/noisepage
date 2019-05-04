#pragma once

#include "execution/ast/ast.h"
#include "execution/util/common.h"

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class CodeGen;

class QueryState {
 public:
  using Id = u32;
  explicit QueryState(ast::Identifier state_name);

  Id RegisterState(std::string name, ast::Expr *type, ast::Expr *value);
  ast::MemberExpr *GetMember(tpl::compiler::CodeGen *codegen, Id id);

  ast::Expr *FinalizeType(CodeGen *codegen);
  ast::Expr *GetType() const { return constructed_type_; }

 private:
  struct StateInfo {
    std::string name;
    ast::Expr *type;
    ast::Expr *value; // only for local states?

    StateInfo(std::string name, ast::Expr *type, ast::Expr *value)
    : name(std::move(name)), type(type), value(value) {}
  };

  ast::Identifier state_name_;
  ast::Expr *constructed_type_;
  std::vector<StateInfo> states_;


};

}