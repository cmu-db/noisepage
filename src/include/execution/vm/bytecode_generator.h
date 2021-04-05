#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/ast/ast_visitor.h"
#include "execution/ast/builtins.h"
#include "execution/vm/bytecode_emitter.h"

namespace noisepage::execution::ast {
class Context;
class FunctionType;
class Type;
}  // namespace noisepage::execution::ast

namespace noisepage::execution::exec {
class ExecutionSettings;
}  // namespace noisepage::execution::exec

namespace noisepage::execution::vm {

class BytecodeModule;
class LoopBuilder;

/**
 * BytecodeGenerator is responsible for converting a parsed TPL AST into TPL bytecode (TBC).
 * It is assumed that the input TPL program has been type-checked.
 * Once compiled into a TBC unit, all defined functions are fully executable.
 *
 * BytecodeGenerator exposes a single public static function BytecodeGenerator::Compile() that
 * performs the heavy lifting and orchestration involved in the compilation process.
 */
class BytecodeGenerator final : public ast::AstVisitor<BytecodeGenerator> {
 public:
  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(BytecodeGenerator);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  /**
   * Main entry point to convert a valid (i.e., parsed and type-checked) AST into a bytecode module.
   * @param root The root of the AST.
   * @param name The (optional) name of the program.
   * @return A compiled bytecode module.
   */
  static std::unique_ptr<BytecodeModule> Compile(ast::AstNode *root, const std::string &name);

  /**
   * @return The emitter used by this generator to write bytecode.
   */
  BytecodeEmitter *GetEmitter() { return &emitter_; }

 private:
  // Private constructor to force users to call Compile()
  BytecodeGenerator() noexcept;

  class ExpressionResultScope;
  class LValueResultScope;
  class RValueResultScope;
  class BytecodePositionScope;

  // Allocate a new function ID
  FunctionInfo *AllocateFunc(const std::string &func_name, ast::FunctionType *func_type);

  void VisitAbortTxn(ast::CallExpr *call);

  // ONLY FOR TESTING!
  void VisitBuiltinTestCatalogLookup(ast::CallExpr *call);
  void VisitBuiltinTestCatalogIndexLookup(ast::CallExpr *call);

  // Dispatched from VisitBuiltinCallExpr() to handle the various builtin
  // functions, including filtering, hash table interaction, sorting etc.
  void VisitSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitNullValueCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitSqlStringLikeCall(ast::CallExpr *call);
  void VisitBuiltinDateFunctionCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinTableIterParallelCall(ast::CallExpr *call);
  void VisitBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinHashCall(ast::CallExpr *call);
  void VisitBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinVectorFilterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinHashTableEntryIteratorCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinJoinHashTableIteratorCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinSorterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitResultBufferCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitCSVReaderCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinSizeOfCall(ast::CallExpr *call);
  void VisitBuiltinOffsetOfCall(ast::CallExpr *call);
  void VisitBuiltinTrigCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinPRCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinStorageInterfaceCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinIndexIteratorCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinParamCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinStringCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinArithmeticCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinAtomicArithmeticCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinAtomicCompareExchangeCall(ast::CallExpr *call);

  void VisitBuiltinReplicationCall(ast::CallExpr *call, ast::Builtin builtin);

  // Dispatched from VisitCallExpr() for handling builtins
  void VisitBuiltinCallExpr(ast::CallExpr *call);
  void VisitRegularCallExpr(ast::CallExpr *call);

  // Dispatched from VisitBinaryOpExpr() for handling logical boolean expressions and arithmetic expressions
  void VisitLogicalAndOrExpr(ast::BinaryOpExpr *node);
  void VisitArithmeticExpr(ast::BinaryOpExpr *node);

  // Dispatched from VisitArithmeticExpr for or SQL vs. primitive arithmetic
  void VisitPrimitiveArithmeticExpr(ast::BinaryOpExpr *node);
  void VisitSqlArithmeticExpr(ast::BinaryOpExpr *node);

  // Dispatched from VisitUnaryOp()
  void VisitAddressOfExpr(ast::UnaryOpExpr *op);
  void VisitDerefExpr(ast::UnaryOpExpr *op);
  void VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op);
  void VisitLogicalNotExpr(ast::UnaryOpExpr *op);

  // Dispatched from VisitIndexExpr() to distinguish between array and map access
  void VisitArrayIndexExpr(ast::IndexExpr *node);
  void VisitMapIndexExpr(ast::IndexExpr *node);

  // Visit an expression for its L-Value
  LocalVar VisitExpressionForLValue(ast::Expr *expr);

  // Visit an expression for its R-Value and return the local variable holding its result
  LocalVar VisitExpressionForRValue(ast::Expr *expr);

  // Visit an expression for a SQL value.
  LocalVar VisitExpressionForSQLValue(ast::Expr *expr);
  void VisitExpressionForSQLValue(ast::Expr *expr, LocalVar dest);

  // Visit an expression for its R-Value, providing a destination variable where the result should be stored
  void VisitExpressionForRValue(ast::Expr *expr, LocalVar dest);

  enum class TestFallthrough : uint8_t { None, Then, Else };

  void VisitExpressionForTest(ast::Expr *expr, BytecodeLabel *then_label, BytecodeLabel *else_label,
                              TestFallthrough fallthrough);

  // Visit the body of an iteration statement
  void VisitIterationStatement(ast::IterationStmt *iteration, LoopBuilder *loop_builder);

  // Dispatched from VisitCompareOp for SQL vs. primitive comparisons
  void VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare);
  void VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare);

  void BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  void BuildAssign(LocalVar dest, LocalVar val, ast::Type *dest_type);
  LocalVar BuildLoadPointer(LocalVar double_ptr, ast::Type *type);

  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type);
  Bytecode GetFloatTypedBytecode(Bytecode bytecode, ast::Type *type);

  // Lookup a function's ID by its name
  FunctionId LookupFuncIdByName(const std::string &name) const;

  // Create a new static
  LocalVar NewStatic(const std::string &name, ast::Type *type, const void *contents);

  // Create a new static string
  LocalVar NewStaticString(ast::Context *ctx, ast::Identifier string);

  // Access the current execution result scope
  ExpressionResultScope *GetExecutionResult() { return execution_result_; }

  // Set the current execution result scope. We lose any previous scope, so it's the caller's
  // responsibility to restore it, if any.
  void SetExecutionResult(ExpressionResultScope *exec_result) { execution_result_ = exec_result; }

  // Access the current function that's being generated. May be NULL.
  FunctionInfo *GetCurrentFunction() { return &functions_.back(); }

 private:
  // The data section of the module
  std::vector<uint8_t> data_;

  // The bytecode generated during compilation
  std::vector<uint8_t> code_;

  // Constants stored in the data section
  std::vector<LocalInfo> static_locals_;
  std::unordered_map<std::string, uint32_t> static_locals_versions_;
  std::unordered_map<ast::Identifier, LocalVar> static_string_cache_;

  // Information about all generated functions
  std::vector<FunctionInfo> functions_;

  // Cache of function names to IDs for faster lookup
  std::unordered_map<std::string, FunctionId> func_map_;

  // Emitter to write bytecode into the code section
  BytecodeEmitter emitter_;

  // RAII struct to capture semantics of expression evaluation
  ExpressionResultScope *execution_result_{nullptr};
};

}  // namespace noisepage::execution::vm
