#pragma once
#include <stdlib.h>

#include "common/managed_pointer.h"
#include "execution/ast/builtins.h"
#include "type/type_id.h"

namespace terrier::execution::udf {

/**
 * @brief Stores execution and type information about a stored procedure
 */
class UDFContext {
 public:
  /**
   * Creates a UDFContext object
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param args_type Vector of argument types
   */
  UDFContext(const std::string &func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type)
      : func_name_(func_name), func_ret_type_(func_ret_type), args_type_(args_type), is_builtin_{false} {}
  /**
   * Creates a UDFContext object for a builtin function
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param args_type Vector of argument types
   * @param builtin Which builtin this context refers to
   */
  UDFContext(const std::string &func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
             ast::Builtin builtin)
      : func_name_(func_name),
        func_ret_type_(func_ret_type),
        args_type_(args_type),
        is_builtin_{true},
        builtin_{builtin} {}
  /**
   * @return The name of the function represented by this context object
   */
  const std::string &GetFunctionName() const { return func_name_; }

  /**
   * @return The vector of type arguments of the function represented by this context object
   */
  const std::vector<type::TypeId> &GetFunctionArgsType() const { return args_type_; }

  /**
   * Gets the return type of the function represented by this object
   * @return return type of this function
   */
  type::TypeId GetFunctionReturnType() const { return func_ret_type_; }

  /**
   * @return true iff this represents a builtin function
   */
  bool IsBuiltin() const { return is_builtin_; }

  /**
   * @return returns what builtin function this represents
   */
  ast::Builtin GetBuiltin() const {
    TERRIER_ASSERT(IsBuiltin(), "Getting a builtin from a non-builtin function");
    return builtin_;
  }

 private:
  const std::string &func_name_;
  type::TypeId func_ret_type_;
  std::vector<type::TypeId> args_type_;
  bool is_builtin_;
  ast::Builtin builtin_;
};

}  // namespace terrier::execution::udf
