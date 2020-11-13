#pragma once

#include <llvm/ADT/DenseMap.h>

#include "execution/ast/identifier.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution {

namespace ast {
class Decl;
class Type;
}  // namespace ast

namespace sema {

/**
 * Stores variable that are in the current scope.
 */
class Scope {
 public:
  /**
   * The kind of scope
   */
  enum class Kind : uint8_t { Block, Function, File, Loop };

  /**
   * Constructor
   * @param outer the parent scope
   * @param scope_kind the scope kind
   */
  Scope(Scope *outer, Kind scope_kind) { Init(outer, scope_kind); }

  /**
   * (Re)initialize the scope
   * @param outer the parent scope
   * @param scope_kind the scope kine
   */
  void Init(Scope *outer, Kind scope_kind) {
    outer_ = outer;
    scope_kind_ = scope_kind;
    decls_.clear();
  }

  /**
   * Declare an element with the given name and type in this scope. Return true
   * if successful and false if an element with the given name already exits in
   * the local scope.

   * @param decl_name element to declare
   * @param type type of the element
   * @return true iff declaration is successful
   */
  bool Declare(ast::Identifier decl_name, ast::Type *type);

  /**
   * Recursively looks for the type an identifier
   * @param name identifier to lookup
   * @return type of the identifier or nullptr if the it's not found.
   */
  ast::Type *Lookup(ast::Identifier name) const;

  /**
   * Looks for the type of the identifier in the current scope only.
   * @param name identifier to lookup
   * @return type of the identifier or nullptr if the it's not found.
   */
  ast::Type *LookupLocal(ast::Identifier name) const;

  /**
   * @return the parent scope
   */
  Scope *Outer() const { return outer_; }

 private:
  // The outer scope.
  Scope *outer_;
  // The scope kind.
  Kind scope_kind_;
  // The mapping of identifiers to their types.
  llvm::DenseMap<ast::Identifier, ast::Type *> decls_;
};

}  // namespace sema
}  // namespace noisepage::execution
