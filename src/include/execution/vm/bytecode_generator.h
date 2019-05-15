#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/ast/ast.h"
#include "execution/ast/ast_visitor.h"
#include "execution/ast/builtins.h"
#include "catalog/catalog.h"
#include "execution/exec/execution_context.h"
#include "execution/vm/bytecode_emitter.h"

namespace tpl::vm {
using terrier::catalog::Catalog;
using terrier::catalog::SqlTableRW;
using terrier::type::TypeId;

class BytecodeModule;
class LoopBuilder;

/// This class is responsible for generating and compiling a parsed and
/// type-checked TPL program (as an AST) into TPL bytecode (TBC) as a
/// BytecodeModule. Once compiled, all functions defined in the module are
/// fully executable. BytecodeGenerator exposes a single public static function
/// \a Compile() that performs the heavy lifting and orchestration involved in
/// the compilation process.
class BytecodeGenerator : public ast::AstVisitor<BytecodeGenerator> {
 public:
  DISALLOW_COPY_AND_MOVE(BytecodeGenerator);

  // Declare all node visit methods here
#define DECLARE_VISIT_METHOD(type) void Visit##type(ast::type *node);
  AST_NODES(DECLARE_VISIT_METHOD)
#undef DECLARE_VISIT_METHOD

  static std::unique_ptr<BytecodeModule> Compile(
      ast::AstNode *root, const std::string &name,
      std::shared_ptr<exec::ExecutionContext> exec_context);

 private:
  // Private constructor to force users to call Compile()
  BytecodeGenerator() noexcept;
  explicit BytecodeGenerator(
      std::shared_ptr<exec::ExecutionContext> exec_context) noexcept;

  class ExpressionResultScope;
  class LValueResultScope;
  class RValueResultScope;
  class BytecodePositionScope;

  // Allocate a new function ID
  FunctionInfo *AllocateFunc(const std::string &func_name,
                             ast::FunctionType *func_type);

  // Dispatched from VisitForInStatement() when using tuple-at-time loops to
  // set up the row structure used in the body of the loop
  void VisitRowWiseIteration(ast::ForInStmt *node, LocalVar pci,
                             LoopBuilder *table_loop,
                             SqlTableRW *catalog_table);
  void VisitVectorWiseIteration(ast::ForInStmt *node, LocalVar pci,
                                LoopBuilder *table_loop);
  void VisitIndexedForInStmt(ast::ForInStmt *node, ast::Expr *index_expr,
                             terrier::catalog::SqlTableRW *catalog_table);

  // Dispatched from VisitBuiltinCallExpr() to handle the various builtin
  // functions, including filtering, hash table interaction, sorting etc.
  void VisitSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinFilterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinOutputCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinIndexIteratorCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinSorterCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinRegionCall(ast::CallExpr *call, ast::Builtin builtin);
  void VisitBuiltinSizeOfCall(ast::CallExpr *call);

  // Dispatched from VisitCallExpr() for handling builtins
  void VisitBuiltinCallExpr(ast::CallExpr *call);
  void VisitRegularCallExpr(ast::CallExpr *call);

  // Dispatched from VisitBinaryOpExpr() for handling logical boolean
  // expressions and arithmetic expressions
  void VisitLogicalAndOrExpr(ast::BinaryOpExpr *node);
  void VisitArithmeticExpr(ast::BinaryOpExpr *node);

  // Dispatched from VisitUnaryOp()
  void VisitAddressOfExpr(ast::UnaryOpExpr *op);
  void VisitDerefExpr(ast::UnaryOpExpr *op);
  void VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op);

  // Dispatched from VisitIndexExpr() to distinguish between array and map
  // access.
  void VisitArrayIndexExpr(ast::IndexExpr *node);
  void VisitMapIndexExpr(ast::IndexExpr *node);

  // Visit an expression for its L-Value
  LocalVar VisitExpressionForLValue(ast::Expr *expr);

  // Visit an expression for its R-Value and return the local variable holding
  // its result
  LocalVar VisitExpressionForRValue(ast::Expr *expr);

  // Visit an expression for its R-Value providing a destination variable where
  // the result should be stored
  void VisitExpressionForRValue(ast::Expr *expr, LocalVar dest);

  enum class TestFallthrough : u8 { None, Then, Else };

  void VisitExpressionForTest(ast::Expr *expr, BytecodeLabel *then_label,
                              BytecodeLabel *else_label,
                              TestFallthrough fallthrough);

  // Visit the body of an iteration statement
  void VisitIterationStatement(ast::IterationStmt *iteration,
                               LoopBuilder *loop_builder);

  // Dispatched from VisitCompareOp for SQL vs. primitive comparisons
  void VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare);
  void VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare);

  void BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  void BuildAssign(LocalVar dest, LocalVar ptr, ast::Type *dest_type);
  LocalVar BuildLoadPointer(LocalVar double_ptr, ast::Type *type);

  Bytecode GetIntTypedBytecode(Bytecode bytecode, ast::Type *type);

 public:
  BytecodeEmitter *emitter() { return &emitter_; }

 private:
  // Lookup a function's ID by its name
  FunctionId LookupFuncIdByName(const std::string &name) const;

  // Lookup a function by its name
  const FunctionInfo *LookupFuncInfoByName(const std::string &name) const;

  // -------------------------------------------------------
  // Accessors
  // -------------------------------------------------------

  ExpressionResultScope *execution_result() { return execution_result_; }

  void set_execution_result(ExpressionResultScope *execution_result) {
    execution_result_ = execution_result;
  }

  FunctionInfo *current_function() { return &functions_.back(); }

  /**
   * @param catalog_table the table being processed.
   * @param col_idx the index of the column.
   * @return the byte used to Get an element from this column.
   */
  Bytecode GetPCIColumnCode(SqlTableRW *catalog_table, u32 col_idx);

  /**
   * @param catalog_table the table being processed.
   * @param col_idx the index of the column.
   * @return the byte used to Get an element from this column.
   */
  Bytecode GetIndexIteratorColumnCode(SqlTableRW *catalog_table,
                                      u32 col_idx);

 private:
  // The bytecode generated during compilation
  std::vector<u8> bytecode_;

  // Information about all generated functions
  std::vector<FunctionInfo> functions_;

  // Cache of function names to IDs for faster lookup
  std::unordered_map<std::string, FunctionId> func_map_;

  // Emitter to write bytecode ops
  BytecodeEmitter emitter_;

  // RAII struct to capture semantics of expression evaluation
  ExpressionResultScope *execution_result_{nullptr};

  // Execution Context
  std::shared_ptr<exec::ExecutionContext> exec_context_{nullptr};
};

}  // namespace tpl::vm
