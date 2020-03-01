#pragma once
#include <stdlib.h>

#include "common/managed_pointer.h"
#include "type/type_id.h"
#include "execution/ast/builtins.h"

namespace terrier::execution::udf {

class UDFContext {
 public:
  UDFContext(const std::string &func_name, type::TypeId func_ret_type,
             std::vector<type::TypeId> &&args_type) : func_name_(func_name),
                                                     func_ret_type_(func_ret_type), args_type_(args_type),
                                                     is_builtin_{false} {}

  UDFContext(const std::string &func_name, type::TypeId func_ret_type,
             std::vector<type::TypeId> &&args_type, ast::Builtin builtin) : func_name_(func_name),
                                                     func_ret_type_(func_ret_type), args_type_(args_type),
                                                     is_builtin_{true}, builtin_{builtin} {}

  const std::string &GetFunctionName() { return func_name_; }

  const std::vector<type::TypeId> &GetFunctionArgsType() { return args_type_; }

  void SetFunctionReturnType(type::TypeId type) { func_ret_type_ = type; }

  type::TypeId GetFunctionReturnType() { return func_ret_type_; }

  bool IsBuiltin() { return is_builtin_; }

  ast::Builtin GetBuiltin() { return builtin_; }

 private:
  const std::string &func_name_;
  type::TypeId func_ret_type_;
  std::vector<type::TypeId> args_type_;
  bool is_builtin_;
  ast::Builtin builtin_;
};

}  // namespace terrier::execution::udf
