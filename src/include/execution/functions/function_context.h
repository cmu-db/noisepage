#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/ast/ast.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/util/region.h"
#include "type/type_id.h"

namespace noisepage::execution::functions {

/**
 * @brief Stores execution and type information about a stored procedure
 */
class FunctionContext {
 public:
  /**
   * Creates a FunctionContext object
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param args_type Vector of argument types
   */
  FunctionContext(std::string func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type)
      : func_name_(std::move(func_name)),
        func_ret_type_(func_ret_type),
        args_type_(std::move(args_type)),
        is_builtin_{false},
        is_exec_ctx_required_{false} {}
  /**
   * Creates a FunctionContext object for a builtin function
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param args_type Vector of argument types
   * @param builtin Which builtin this context refers to
   * @param is_exec_ctx_required true if this function requires an execution context var as its first argument
   */
  FunctionContext(std::string func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
                  ast::Builtin builtin, bool is_exec_ctx_required = false)
      : func_name_(std::move(func_name)),
        func_ret_type_(func_ret_type),
        args_type_(std::move(args_type)),
        is_builtin_{true},
        builtin_{builtin},
        is_exec_ctx_required_{is_exec_ctx_required} {}

  FunctionContext(std::string func_name, type::TypeId func_ret_type, std::vector<type::TypeId> &&args_type,
                  std::unique_ptr<util::Region> ast_region, std::unique_ptr<ast::Context> ast_context, ast::File *file,
                  bool is_exec_ctx_required = true)
      : func_name_(std::move(func_name)),
        func_ret_type_(func_ret_type),
        args_type_(std::move(args_type)),
        is_builtin_{false},
        is_exec_ctx_required_{is_exec_ctx_required},
        ast_region_{std::move(ast_region)},
        ast_context_{std::move(ast_context)},
        file_{file} {}

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
    NOISEPAGE_ASSERT(IsBuiltin(), "Getting a builtin from a non-builtin function");
    return builtin_;
  }

  /**
   * @return returns if this function requires an execution context
   */
  bool IsExecCtxRequired() const {
    NOISEPAGE_ASSERT(IsBuiltin(), "IsExecCtxRequired is only valid or a builtin function");
    return is_exec_ctx_required_;
  }

  /**
   * @return returns the main functiondecl of this udf (to be used only if not builtin)
   */
  common::ManagedPointer<ast::FunctionDecl> GetMainFunctionDecl() const {
    NOISEPAGE_ASSERT(!IsBuiltin(), "Getting a non-builtin from a builtin function");
    return common::ManagedPointer<ast::FunctionDecl>(
        reinterpret_cast<ast::FunctionDecl *>(file_->Declarations().back()));
  }

  /**
   * @return returns the file with the functiondecl and supporting decls (to be used only if not builtin)
   */
  ast::File *GetFile() const {
    NOISEPAGE_ASSERT(!IsBuiltin(), "Getting a non-builtin from a builtin function");
    return file_;
  }

  /**
   * TODO(Kyle): Document.
   */
  ast::Context *GetASTContext() const {
    NOISEPAGE_ASSERT(!IsBuiltin(), "Getting a non-builtin from a builtin function");
    return ast_context_.get();
  }

 private:
  std::string func_name_;
  type::TypeId func_ret_type_;
  std::vector<type::TypeId> args_type_;
  bool is_builtin_;
  ast::Builtin builtin_;
  bool is_exec_ctx_required_;

  std::unique_ptr<util::Region> ast_region_;
  std::unique_ptr<ast::Context> ast_context_;
  ast::File *file_;
};

}  // namespace noisepage::execution::functions
