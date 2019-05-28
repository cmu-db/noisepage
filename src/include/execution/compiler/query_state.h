#pragma once

#include <string>
#include <utility>
#include <vector>
#include "execution/ast/ast.h"
#include "execution/util/common.h"

namespace tpl::util {
class Region;
}

namespace tpl::compiler {

class CodeGen;

/**
 * Represents the query state of the plan.
 * Each node stores the information it needs in this query state.
 * For example, a join plan node may store its hash table here.
 * In addition to this, each node may define a top level struct.
 * For example, a insert plan node may define a struct represented inserted tuples.
 */
class QueryState {
 public:
  /// Each field will have a unique id used by the nodes to access them
  using Id = u32;

  /**
   * Constructor
   * @param state_name name of the struct
   */
  explicit QueryState(ast::Identifier state_name);

  /**
   * Adds a new field and returns its id number.
   * @param name name of the field
   * @param type type of the field
   * @param value value of the field
   * @return id of the field
   */
  Id RegisterState(std::string name, ast::Expr *type, ast::Expr *value);

  /**
   * Access a member of the struct
   * @param codegen code generator to use
   * @param id id of the member
   * @return generated member expression.
   */
  ast::MemberExpr *GetMember(tpl::compiler::CodeGen *codegen, Id id);

  /**
   * Finalizes the struct type.
   * @param codegen code generator
   */
  void FinalizeType(CodeGen *codegen);

  /**
   * @return the constructed struct type.
   */
  ast::StructTypeRepr *GetType() const { return constructed_type_; }

 private:
  /// A single state element
  struct StateInfo {
    /// Name of the field
    std::string name;
    /// Type of the field
    ast::Expr *type;
    /// Value of the field
    ast::Expr *value;  // only for local states?

    /**
     * Constructor
     * @param name name of field
     * @param type type of the field
     * @param value value of the field
     */
    StateInfo(std::string name, ast::Expr *type, ast::Expr *value) : name(std::move(name)), type(type), value(value) {}
  };

  ast::Identifier state_name_;
  ast::StructTypeRepr *constructed_type_;
  std::vector<StateInfo> states_;
};

}  // namespace tpl::compiler
