#pragma once

#include <memory>

#include "catalog/schema.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_visitor.h"
#include "execution/ast/builtins.h"
#include "execution/sema/error_reporter.h"
#include "execution/sema/scope.h"

namespace terrier::execution {

namespace ast {
class Context;
}  // namespace ast

namespace sema {

/**
 * This is the main class that performs semantic analysis of TPL programs. It
 * traverses an untyped TPL abstract syntax tree (AST), fills in types based on
 * declarations, derives types of expressions and ensures correctness of all
 * operations in the TPL program.
 *
 * Usage:
 * sema::Sema check(context);
 * bool has_errors = check.Run(ast);
 * if (has_errors) {
 *   handle errors
 * }
 */
class Sema : public ast::AstVisitor<Sema> {
 public:
  /**
   * Construct using the given context
   * @param ctx The context used to acquire memory for new ASTs and the
   *            diagnostic error reporter.
   */
  explicit Sema(ast::Context *ctx);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(Sema);

  /**
   * Run the type checker on the provided AST rooted at \a root. Ensures proper
   * types of all statements and expressions, and also annotates the AST with
   * correct type information.
   * @return True if type-checking found errors; false otherwise
   */
  bool Run(ast::AstNode *root);

  // Declare all node visit methods here
#define DECLARE_AST_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_AST_VISIT_METHOD)
#undef DECLARE_AST_VISIT_METHOD

  /**
   * @return the ast context
   */
  ast::Context *GetContext() const { return ctx_; }

  /**
   * @return the error reporter
   */
  ErrorReporter *GetErrorReporter() const { return error_reporter_; }

 private:
  // Resolve the type of the input expression
  ast::Type *Resolve(ast::Expr *expr) {
    Visit(expr);
    return expr->GetType();
  }

  // Convert the given schema into a row type
  ast::Type *GetRowTypeFromSqlSchema(const catalog::Schema &schema);

  // Create a builtin type
  ast::Type *GetBuiltinType(uint16_t builtin_kind);

  struct CheckResult {
    ast::Type *result_type_;
    ast::Expr *left_;
    ast::Expr *right_;
  };

  void ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, ast::Type *expected);

  void ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, const char *expected);

  // Implicitly cast the input expression into the target type using the
  // provided cast kind, also setting the type of the casted expression result.
  ast::Expr *ImplCastExprToType(ast::Expr *expr, ast::Type *target_type, ast::CastKind cast_kind);

  // Check the number of arguments to the call; true if good, false otherwise
  bool CheckArgCount(ast::CallExpr *call, uint32_t expected_arg_count);
  bool CheckArgCountAtLeast(ast::CallExpr *call, uint32_t expected_arg_count);

  // Check boolean logic operands: and, or
  CheckResult CheckLogicalOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                   ast::Expr *right);

  // Check operands to an arithmetic operation: +, -, *, etc.
  CheckResult CheckArithmeticOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                      ast::Expr *right);

  CheckResult CheckComparisonOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                      ast::Expr *right);

  // Check the assignment of the expression to a variable or the target type.
  // Return true if the assignment is valid, and false otherwise.
  // Will also apply an implicit cast to make the assignment valid.
  bool CheckAssignmentConstraints(ast::Type *target_type, ast::Expr **expr);

  // Dispatched from VisitCall() to handle builtin functions
  void CheckBuiltinCall(ast::CallExpr *call);
  void CheckBuiltinMapCall(ast::CallExpr *call);
  void CheckBuiltinSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinFilterCall(ast::CallExpr *call);
  void CheckBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinJoinHashTableInit(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableInsert(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableIterInit(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableIterHasNext(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableIterGetRow(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableIterClose(ast::CallExpr *call);
  void CheckBuiltinJoinHashTableBuild(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinJoinHashTableFree(ast::CallExpr *call);
  void CheckBuiltinSorterInit(ast::CallExpr *call);
  void CheckBuiltinSorterInsert(ast::CallExpr *call);
  void CheckBuiltinSorterSort(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSorterFree(ast::CallExpr *call);
  void CheckBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckMathTrigCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinSizeOfCall(ast::CallExpr *call);
  void CheckBuiltinPtrCastCall(ast::CallExpr *call);
  void CheckBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinTableIterParCall(ast::CallExpr *call);
  void CheckBuiltinPCICall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinHashCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinOutputAlloc(ast::CallExpr *call);
  void CheckBuiltinOutputFinalize(ast::CallExpr *call);
  void CheckBuiltinIndexIteratorInit(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinIndexIteratorAdvance(ast::CallExpr *call);
  void CheckBuiltinIndexIteratorScanKey(ast::CallExpr *call);
  void CheckBuiltinIndexIteratorFree(ast::CallExpr *call);
  void CheckBuiltinIndexIteratorPRCall(ast::CallExpr *call, ast::Builtin builtin);
  void CheckBuiltinPRCall(ast::CallExpr *call, ast::Builtin builtin);

  // -------------------------------------------------------
  // Scoping
  // -------------------------------------------------------

  Scope *CurrentScope() { return scope_; }

  // Enter a new scope
  void EnterScope(Scope::Kind scope_kind) {
    if (num_cached_scopes_ > 0) {
      Scope *scope = scope_cache_[--num_cached_scopes_].release();
      TERRIER_ASSERT(scope != nullptr, "Cached scope was null");
      scope->Init(CurrentScope(), scope_kind);
      scope_ = scope;
    } else {
      scope_ = new Scope(CurrentScope(), scope_kind);
    }
  }

  // Exit the current scope
  void ExitScope() {
    TERRIER_ASSERT(CurrentScope() != nullptr, "Mismatched scope exit");

    Scope *scope = CurrentScope();
    scope_ = scope->Outer();

    if (num_cached_scopes_ < K_SCOPE_CACHE_SIZE) {
      scope_cache_[num_cached_scopes_++].reset(scope);
    } else {
      delete scope;
    }
  }

  /**
   * RAII scope class to track the current scope
   */
  class SemaScope {
   public:
    SemaScope(Sema *check, Scope::Kind scope_kind) : check_(check), exited_(false) { check->EnterScope(scope_kind); }

    ~SemaScope() { Exit(); }

    void Exit() {
      if (!exited_) {
        check_->ExitScope();
        exited_ = true;
      }
    }

    Sema *Check() { return check_; }

   private:
    Sema *check_;
    bool exited_;
  };

  /**
   * RAII scope class to capture both the current function and its scope
   */
  class FunctionSemaScope {
   public:
    FunctionSemaScope(Sema *check, ast::FunctionLitExpr *func)
        : prev_func_(check->CurrentFunction()), block_scope_(check, Scope::Kind::Function) {
      check->curr_func_ = func;
    }

    ~FunctionSemaScope() { Exit(); }

    void Exit() {
      block_scope_.Exit();
      block_scope_.Check()->curr_func_ = prev_func_;
    }

   private:
    ast::FunctionLitExpr *prev_func_;
    SemaScope block_scope_;
  };

  ast::FunctionLitExpr *CurrentFunction() const { return curr_func_; }

 private:
  // The context
  ast::Context *ctx_;

  // The error reporter
  ErrorReporter *error_reporter_;

  // The current active scope
  Scope *scope_;

  // A cache of scopes to reduce allocations
  static constexpr const uint32_t K_SCOPE_CACHE_SIZE = 4;
  uint64_t num_cached_scopes_;
  std::unique_ptr<Scope> scope_cache_[K_SCOPE_CACHE_SIZE] = {nullptr};

  ast::FunctionLitExpr *curr_func_;
};

}  // namespace sema
}  // namespace terrier::execution
