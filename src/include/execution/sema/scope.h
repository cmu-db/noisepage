#pragma once

#include "llvm/ADT/DenseMap.h"

#include "execution/ast/identifier.h"
#include "execution/util/common.h"

namespace tpl {

namespace ast {
class Decl;
class Type;
}  // namespace ast

namespace sema {

class Scope {
 public:
  enum class Kind : u8 { Block, Function, File, Loop };

  Scope(Scope *outer, Kind scope_kind) { Init(outer, scope_kind); }

  void Init(Scope *outer, Kind scope_kind) {
    outer_ = outer;
    scope_kind_ = scope_kind;
    decls_.clear();
  }

  // Declare an element with the given name and type in this scope. Return true
  // if successful and false if an element with the given name already exits in
  // the local scope.
  bool Declare(ast::Identifier decl_name, ast::Type *type);

  ast::Type *Lookup(ast::Identifier name) const;
  ast::Type *LookupLocal(ast::Identifier name) const;

  Scope *outer() const { return outer_; }

 private:
  Scope *outer_;

  Kind scope_kind_;

  llvm::DenseMap<ast::Identifier, ast::Type *> decls_;
};

}  // namespace sema
}  // namespace tpl
