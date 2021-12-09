#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/ast/ast.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/util/region.h"

namespace noisepage::execution::functions {

/**
 * @brief Stores execution and type information about a stored procedure.
 */
class FunctionContext {
 public:
  /**
   * Construct a FunctionContext instance.
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param arg_types Vector of argument types
   */
  FunctionContext(std::string func_name, execution::sql::SqlTypeId func_ret_type,
                  std::vector<execution::sql::SqlTypeId> &&arg_types)
      : func_name_(std::move(func_name)),
        func_ret_type_(func_ret_type),
        arg_types_(std::move(arg_types)),
        is_builtin_{false},
        is_exec_ctx_required_{false} {}

  /**
   * Construct a FunctionContext instance for a builtin function.
   * @param func_name Name of function
   * @param func_ret_type Return type of function
   * @param arg_types Vector of argument types
   * @param builtin Which builtin this context refers to
   * @param is_exec_ctx_required true if this function requires an execution context var as its first argument
   */
  FunctionContext(std::string func_name, sql::SqlTypeId func_ret_type, std::vector<sql::SqlTypeId> &&arg_types,
                  ast::Builtin builtin, bool is_exec_ctx_required = false)
      : func_name_(std::move(func_name)),
        func_ret_type_(func_ret_type),
        arg_types_(std::move(arg_types)),
        is_builtin_{true},
        builtin_{builtin},
        is_exec_ctx_required_{is_exec_ctx_required} {}

  /**
   * Construct a FunctionContext instance for a non-builtin function.
   * @param func_name Name of function=
   * @param func_ret_type Return type of function
   * @param arg_types Vector of argument types
   * @param ast_region The region associated with the AST context
   * @param ast_context The AST context for the function
   * @param file The AST file
   * @param is_exec_ctx_required Flag indicating whether an
   * execution context is required for this function
   */
  FunctionContext(std::string func_name, sql::SqlTypeId func_ret_type, std::vector<sql::SqlTypeId> &&arg_types,
                  std::unique_ptr<util::Region> ast_region, std::unique_ptr<ast::Context> ast_context, ast::File *file,
                  bool is_exec_ctx_required = true)
      : func_name_(std::move(func_name)),
        func_ret_type_(func_ret_type),
        arg_types_(std::move(arg_types)),
        is_builtin_{false},
        is_exec_ctx_required_{is_exec_ctx_required},
        ast_region_{std::move(ast_region)},
        ast_context_{std::move(ast_context)},
        file_{file} {}

  /** @return The name of the function represented by this context object. */
  const std::string &GetFunctionName() const { return func_name_; }

  /** @return The vector of type arguments of the function represented by this context object */
  const std::vector<execution::sql::SqlTypeId> &GetFunctionArgTypes() const { return arg_types_; }

  /**
   * Gets the return type of the function represented by this object.
   * @return The return type of this function.
   */
  execution::sql::SqlTypeId GetFunctionReturnType() const { return func_ret_type_; }

  /** @return `true` if this represents a builtin function, `false` otherwise. */
  bool IsBuiltin() const { return is_builtin_; }

  /** @return The builtin function this procedure represents. */
  ast::Builtin GetBuiltin() const {
    NOISEPAGE_ASSERT(IsBuiltin(), "Getting a builtin from a non-builtin function");
    return builtin_;
  }

  /** @return `true` if this function requires an execution context, `false` otherwise. */
  bool IsExecCtxRequired() const {
    // TODO(Kyle): Is it valid to query execution context requirement for non-builtins?
    return is_exec_ctx_required_;
  }

  /** @return The main function declaration of this UDF. */
  common::ManagedPointer<ast::FunctionDecl> GetMainFunctionDecl() const {
    NOISEPAGE_ASSERT(!IsBuiltin(), "Getting a non-builtin from a builtin function");
    return common::ManagedPointer<ast::FunctionDecl>(
        reinterpret_cast<ast::FunctionDecl *>(file_->Declarations().back()));
  }

  /** @return The file with the function declaration and supporting declarations. */
  ast::File *GetFile() const {
    NOISEPAGE_ASSERT(!IsBuiltin(), "Getting a non-builtin from a builtin function");
    return file_;
  }

  /** @return The AST context for this procedure. */
  ast::Context *GetASTContext() const {
    NOISEPAGE_ASSERT(!IsBuiltin(), "No AST Context associated with builtin function");
    return ast_context_.get();
  }

 private:
  /** The function name */
  std::string func_name_;
  /** The function return type */
  sql::SqlTypeId func_ret_type_;
  /** The function argument types */
  std::vector<sql::SqlTypeId> arg_types_;
  /** `true` if this function is a builtin */
  bool is_builtin_;
  /** The builtin function, if applicable */
  ast::Builtin builtin_;
  /** `true` if an execution context is required for this function, `false` otherwise */
  bool is_exec_ctx_required_;

  /** The associated AST region */
  std::unique_ptr<util::Region> ast_region_;
  /** The associated AST context */
  std::unique_ptr<ast::Context> ast_context_;

  /** The associated file */
  ast::File *file_;
};

}  // namespace noisepage::execution::functions
