#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/exec/execution_context.h"
#include "execution/vm/bytecode_label.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/control_flow_builders.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::vm {

// ---------------------------------------------------------
// Expression Result Scope
// ---------------------------------------------------------

/**
 * ExpressionResultScope is an RAII class that provides metadata about the
 * usage of an expression and its result. Callers construct one of its
 * subclasses to let children nodes know the context in which the expression's
 * result is needed (i.e., whether the expression is an L-Value or R-Value).
 * It also tracks **where** the result of an expression is, somewhat emulating
 * destination-driven code generation.
 *
 * This is a base class for both LValue and RValue result scope objects
 */
class BytecodeGenerator::ExpressionResultScope {
 public:
  ExpressionResultScope(BytecodeGenerator *generator, ast::Expr::Context kind, LocalVar destination = LocalVar())
      : generator_(generator), outer_scope_(generator->ExecutionResult()), destination_(destination), kind_(kind) {
    generator_->SetExecutionResult(this);
  }

  virtual ~ExpressionResultScope() { generator_->SetExecutionResult(outer_scope_); }

  bool IsLValue() const { return kind_ == ast::Expr::Context::LValue; }
  bool IsRValue() const { return kind_ == ast::Expr::Context::RValue; }

  bool HasDestination() const { return !Destination().IsInvalid(); }

  LocalVar GetOrCreateDestination(ast::Type *type) {
    if (!HasDestination()) {
      destination_ = generator_->CurrentFunction()->NewLocal(type);
    }

    return destination_;
  }

  LocalVar Destination() const { return destination_; }
  void SetDestination(LocalVar destination) { destination_ = destination; }

 private:
  BytecodeGenerator *generator_;
  ExpressionResultScope *outer_scope_;
  LocalVar destination_;
  ast::Expr::Context kind_;
};

/**
 * An expression result scope that indicates the result is used as an L-Value
 */
class BytecodeGenerator::LValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit LValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::LValue, dest) {}
};

/**
 * An expression result scope that indicates the result is used as an R-Value
 */
class BytecodeGenerator::RValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit RValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::RValue, dest) {}
};

/**
 * A handy scoped class that tracks the start and end positions in the bytecode
 * for a given function, automatically setting the range in the function upon
 * going out of scope.
 */
class BytecodeGenerator::BytecodePositionScope {
 public:
  BytecodePositionScope(BytecodeGenerator *generator, FunctionInfo *func)
      : generator_(generator), func_(func), start_offset_(generator->Emitter()->Position()) {}

  ~BytecodePositionScope() {
    const std::size_t end_offset = generator_->Emitter()->Position();
    func_->SetBytecodeRange(start_offset_, end_offset);
  }

 private:
  BytecodeGenerator *generator_;
  FunctionInfo *func_;
  std::size_t start_offset_;
};

// ---------------------------------------------------------
// Bytecode Generator begins
// ---------------------------------------------------------

BytecodeGenerator::BytecodeGenerator() noexcept : BytecodeGenerator(nullptr) {}
BytecodeGenerator::BytecodeGenerator(exec::ExecutionContext *exec_ctx) noexcept
    : emitter_(&bytecode_), execution_result_(nullptr), exec_ctx_(exec_ctx) {}

void BytecodeGenerator::VisitIfStmt(ast::IfStmt *node) {
  IfThenElseBuilder if_builder(this);

  // Generate condition check code
  VisitExpressionForTest(node->Condition(), if_builder.ThenLabel(), if_builder.ElseLabel(), TestFallthrough::Then);

  // Generate code in "then" block
  if_builder.Then();
  Visit(node->ThenStmt());

  // If there's an "else" block, handle it now
  if (node->ElseStmt() != nullptr) {
    if (!ast::Stmt::IsTerminating(node->ThenStmt())) {
      if_builder.JumpToEnd();
    }
    if_builder.Else();
    Visit(node->ElseStmt());
  }
}

void BytecodeGenerator::VisitIterationStatement(ast::IterationStmt *iteration, LoopBuilder *loop_builder) {
  Visit(iteration->Body());
  loop_builder->BindContinueTarget();
}

void BytecodeGenerator::VisitForStmt(ast::ForStmt *node) {
  LoopBuilder loop_builder(this);

  if (node->Init() != nullptr) {
    Visit(node->Init());
  }

  loop_builder.LoopHeader();

  if (node->Condition() != nullptr) {
    BytecodeLabel loop_body_label;
    VisitExpressionForTest(node->Condition(), &loop_body_label, loop_builder.BreakLabel(), TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->Next() != nullptr) {
    Visit(node->Next());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitForInStmt(UNUSED_ATTRIBUTE ast::ForInStmt *node) {
  TERRIER_ASSERT(false, "For-in statements not supported");
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) { AstVisitor::VisitFieldDecl(node); }

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // The function's TPL type
  auto *func_type = node->TypeRepr()->GetType()->As<ast::FunctionType>();

  // Allocate the function
  FunctionInfo *func_info = AllocateFunc(node->Name().Data(), func_type);

  {
    // Visit the body of the function. We use this handy scope object to track
    // the start and end position of this function's bytecode in the module's
    // bytecode array. Upon destruction, the scoped class will set the bytecode
    // range in the function.
    BytecodePositionScope position_scope(this, func_info);
    Visit(node->Function());
  }
}

void BytecodeGenerator::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Lookup the local in the current function. It must be there through a
  // previous variable declaration (or parameter declaration). What is returned
  // is a pointer to the variable.

  const std::string local_name = node->Name().Data();
  LocalVar local = CurrentFunction()->LookupLocal(local_name);

  if (ExecutionResult()->IsLValue()) {
    ExecutionResult()->SetDestination(local);
    return;
  }

  // The caller wants the R-Value of the identifier. So, we need to load it. If
  // the caller did not provide a destination register, we're done. If the
  // caller provided a destination, we need to move the value of the identifier
  // into the provided destination.

  if (!ExecutionResult()->HasDestination()) {
    ExecutionResult()->SetDestination(local.ValueOf());
    return;
  }

  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());

  // If the local we want the R-Value of is a parameter, we can't take its
  // pointer for the deref, so we use an assignment. Otherwise, a deref is good.
  if (auto *local_info = CurrentFunction()->LookupLocalInfoByName(local_name); local_info->IsParameter()) {
    BuildAssign(dest, local.ValueOf(), node->GetType());
  } else {
    BuildDeref(dest, local, node->GetType());
  }

  ExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());
  LocalVar input = VisitExpressionForRValue(node->Input());

  switch (node->GetCastKind()) {
    case ast::CastKind::SqlBoolToBool: {
      Emitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      ExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::CastKind::BoolToSqlBool: {
      Emitter()->Emit(Bytecode::InitBoolVal, dest, input);
      ExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::IntToSqlInt: {
      Emitter()->Emit(Bytecode::InitInteger, dest, input);
      ExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::BitCast:
    case ast::CastKind::IntegralCast: {
      BuildAssign(dest, input, node->GetType());
      ExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::CastKind::FloatToSqlReal: {
      Emitter()->Emit(Bytecode::InitReal, dest, input);
      ExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::SqlIntToSqlReal: {
      Emitter()->Emit(Bytecode::IntegerToReal, dest, input);
      ExecutionResult()->SetDestination(dest);
      break;
    }
    default: {
      // Implement me
      throw std::runtime_error("Implement this cast type");
    }
  }
}

void BytecodeGenerator::VisitArrayIndexExpr(ast::IndexExpr *node) {
  // The type and the element's size
  auto *type = node->Object()->GetType()->As<ast::ArrayType>();
  auto elem_size = type->ElementType()->Size();

  // First, we need to get the base address of the array
  LocalVar arr;
  if (type->HasKnownLength()) {
    arr = VisitExpressionForLValue(node->Object());
  } else {
    arr = VisitExpressionForRValue(node->Object());
  }

  // The next step is to compute the address of the element at the desired index
  // stored in the IndexExpr node. There are two cases we handle:
  //
  // 1. The index is a constant literal
  // 2. The index is variable
  //
  // If the index is a constant literal (e.g., x[4]), then we can immediately
  // compute the offset of the element, and issue a vanilla Lea instruction.
  //
  // If the index is not a constant, we need to evaluate the expression to
  // produce the index, then issue a LeaScaled instruction to compute the
  // address.

  LocalVar elem_ptr = CurrentFunction()->NewLocal(node->GetType()->PointerTo());

  if (node->Index()->IsIntegerLiteral()) {
    const auto index = static_cast<int32_t>(node->Index()->As<ast::LitExpr>()->Int64Val());
    TERRIER_ASSERT(index >= 0, "Array indexes must be non-negative");
    Emitter()->EmitLea(elem_ptr, arr, (elem_size * index));
  } else {
    LocalVar index = VisitExpressionForRValue(node->Index());
    Emitter()->EmitLeaScaled(elem_ptr, arr, index, elem_size, 0);
  }

  elem_ptr = elem_ptr.ValueOf();

  if (ExecutionResult()->IsLValue()) {
    ExecutionResult()->SetDestination(elem_ptr);
    return;
  }

  // The caller wants the value of the array element. We just computed the
  // element's pointer (in element_ptr). Just dereference it into the desired
  // location and be done with it.

  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());
  BuildDeref(dest, elem_ptr, node->GetType());
  ExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitMapIndexExpr(ast::IndexExpr *node) {}

void BytecodeGenerator::VisitIndexExpr(ast::IndexExpr *node) {
  if (node->Object()->GetType()->IsArrayType()) {
    VisitArrayIndexExpr(node);
  } else {
    VisitMapIndexExpr(node);
  }
}

void BytecodeGenerator::VisitBlockStmt(ast::BlockStmt *node) {
  for (auto *stmt : node->Statements()) {
    Visit(stmt);
  }
}

void BytecodeGenerator::VisitVariableDecl(ast::VariableDecl *node) {
  // Register a new local variable in the function. If the variable has an
  // explicit type specifier, prefer using that. Otherwise, use the type of the
  // initial value resolved after semantic analysis.
  ast::Type *type = nullptr;
  if (node->TypeRepr() != nullptr) {
    TERRIER_ASSERT(node->TypeRepr()->GetType() != nullptr,
                   "Variable with explicit type declaration is missing resolved "
                   "type at runtime!");
    type = node->TypeRepr()->GetType();
  } else {
    TERRIER_ASSERT(node->Initial() != nullptr,
                   "Variable without explicit type declaration is missing an "
                   "initialization expression!");
    TERRIER_ASSERT(node->Initial()->GetType() != nullptr, "Variable with initial value is missing resolved type");
    type = node->Initial()->GetType();
  }

  // Register this variable in the function as a local
  LocalVar local = CurrentFunction()->NewLocal(type, node->Name().Data());

  // If there's an initializer, generate code for it now
  if (node->Initial() != nullptr) {
    VisitExpressionForRValue(node->Initial(), local);
  }
}

void BytecodeGenerator::VisitAddressOfExpr(ast::UnaryOpExpr *op) {
  TERRIER_ASSERT(ExecutionResult()->IsRValue(), "Address-of expressions must be R-values!");
  LocalVar addr = VisitExpressionForLValue(op->Expression());
  if (ExecutionResult()->HasDestination()) {
    LocalVar dest = ExecutionResult()->Destination();
    BuildAssign(dest, addr, op->GetType());
  } else {
    ExecutionResult()->SetDestination(addr);
  }
}

void BytecodeGenerator::VisitDerefExpr(ast::UnaryOpExpr *op) {
  LocalVar addr = VisitExpressionForRValue(op->Expression());
  if (ExecutionResult()->IsLValue()) {
    ExecutionResult()->SetDestination(addr);
  } else {
    LocalVar dest = ExecutionResult()->GetOrCreateDestination(op->GetType());
    BuildDeref(dest, addr, op->GetType());
    ExecutionResult()->SetDestination(dest.ValueOf());
  }
}

void BytecodeGenerator::VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(op->GetType());
  LocalVar input = VisitExpressionForRValue(op->Expression());

  Bytecode bytecode;
  switch (op->Op()) {
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Neg), op->GetType());
      break;
    }
    case parsing::Token::Type::BIT_NOT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitNeg), op->GetType());
      break;
    }
    default: {
      UNREACHABLE("Impossible unary operation");
    }
  }

  // Emit
  Emitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
  ExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitLogicalNotExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(op->GetType());
  LocalVar input = VisitExpressionForRValue(op->Expression());
  Emitter()->EmitUnaryOp(Bytecode::Not, dest, input);
  ExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  switch (node->Op()) {
    case parsing::Token::Type::AMPERSAND: {
      VisitAddressOfExpr(node);
      break;
    }
    case parsing::Token::Type::STAR: {
      VisitDerefExpr(node);
      break;
    }
    case parsing::Token::Type::MINUS:
    case parsing::Token::Type::BIT_NOT: {
      VisitArithmeticUnaryExpr(node);
      break;
    }
    case parsing::Token::Type::BANG: {
      VisitLogicalNotExpr(node);
      break;
    }
    default: {
      UNREACHABLE("Impossible unary operation");
    }
  }
}

void BytecodeGenerator::VisitReturnStmt(ast::ReturnStmt *node) {
  if (node->Ret() != nullptr) {
    LocalVar rv = CurrentFunction()->GetReturnValueLocal();
    LocalVar result = VisitExpressionForRValue(node->Ret());
    BuildAssign(rv.ValueOf(), result, node->Ret()->GetType());
  }
  Emitter()->EmitReturn();
}

void BytecodeGenerator::VisitSqlNullCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();
  switch (builtin) {
    case ast::Builtin::IsSqlNull: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto input = VisitExpressionForLValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::ValIsNull, dest, input);
      ExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::IsSqlNotNull: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto input = VisitExpressionForLValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::ValIsNotNull, dest, input);
      ExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::NullToSql: {
      // The type of NULL to be created should have been set in sema.
      // Per discussions with pmenon, the NULL type should be determined during bytecode generation.
      // Currently, all SQL types do not need special behavior for NULLs, and it suffices to create
      // a Val::Null() to handle every use-case. However, if custom NULL objects are required in the
      // future, then the switching on the type of the NULL should also be done in this function.
      // The idea is to avoid the overhead of doing it at runtime.
      auto dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->EmitAll(Bytecode::InitSqlNull, dest);
      break;
    }
    default:
      UNREACHABLE("Unsupported NULL-related builtin.");
  }
}

void BytecodeGenerator::VisitSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();
  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::InitBoolVal, dest, input);
      break;
    }
    case ast::Builtin::IntToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::InitInteger, dest, input);
      break;
    }
    case ast::Builtin::FloatToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::Builtin::SqlToBool: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      ExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::StringToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      // Copy data into the execution context's buffer.
      auto input = call->Arguments()[0]->As<ast::LitExpr>()->RawStringVal();
      if (input.Data() != nullptr) {
        // Assign the pointer to a local variable
        Emitter()->EmitInitString(Bytecode::InitString, dest, input.Length(),
                                  reinterpret_cast<uintptr_t>(input.Data()));
      } else {
        Emitter()->EmitInitString(Bytecode::InitString, dest, 0, reinterpret_cast<uintptr_t>(0UL));
      }
      break;
    }
    case ast::Builtin::VarlenToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::InitVarlen, dest, input);
      break;
    }
    case ast::Builtin::DateToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto year = VisitExpressionForRValue(call->Arguments()[0]);
      auto month = VisitExpressionForRValue(call->Arguments()[1]);
      auto day = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::InitDate, dest, year, month, day);
      break;
    }
    case ast::Builtin::TimestampToSql: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Timestamp));
      auto usec = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::InitTimestamp, dest, usec);
      break;
    }
    case ast::Builtin::TimestampToSqlYMDHMSMU: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Timestamp));
      auto year = VisitExpressionForRValue(call->Arguments()[0]);
      auto month = VisitExpressionForRValue(call->Arguments()[1]);
      auto day = VisitExpressionForRValue(call->Arguments()[2]);
      auto h = VisitExpressionForRValue(call->Arguments()[3]);
      auto m = VisitExpressionForRValue(call->Arguments()[4]);
      auto s = VisitExpressionForRValue(call->Arguments()[5]);
      auto ms = VisitExpressionForRValue(call->Arguments()[6]);
      auto us = VisitExpressionForRValue(call->Arguments()[7]);
      Emitter()->Emit(Bytecode::InitTimestampYMDHMSMU, dest, year, month, day, h, m, s, ms, us);
      break;
    }
    default: {
      UNREACHABLE("Impossible SQL conversion call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinDateFunctionCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();
  switch (builtin) {
    case ast::Builtin::ExtractYear: {
      auto dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::ExtractYear, dest, input);
      break;
    }
    default: {
      UNREACHABLE("Impossible Date function call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();

  // The first argument to all calls is a pointer to the TVI
  LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::TableIterInit: {
      // The second argument should be the execution context
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      // The third argument is an oid integer literal
      auto table_oid = static_cast<uint32_t>(call->Arguments()[2]->As<ast::LitExpr>()->Int64Val());
      // The fourth argument is the array of oids
      auto *arr_type = call->Arguments()[3]->GetType()->As<ast::ArrayType>();
      LocalVar arr = VisitExpressionForLValue(call->Arguments()[3]);
      // Emit the initialization codes
      Emitter()->EmitTableIterInit(Bytecode::TableVectorIteratorInit, iter, exec_ctx, table_oid, arr,
                                   static_cast<uint32_t>(arr_type->Length()));
      Emitter()->Emit(Bytecode::TableVectorIteratorPerformInit, iter);
      break;
    }
    case ast::Builtin::TableIterInitBind: {
      // The second argument should be the execution context
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      // The third argument is the table name
      ast::Identifier table_name = call->Arguments()[2]->As<ast::LitExpr>()->RawStringVal();
      auto ns_oid = exec_ctx_->GetAccessor()->GetDefaultNamespace();
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name.Data());
      TERRIER_ASSERT(table_oid != terrier::catalog::INVALID_TABLE_OID, "Table does not exists");
      // The fourth argument is the array of oids
      auto *arr_type = call->Arguments()[3]->GetType()->As<ast::ArrayType>();
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[3]);
      // Emit the initialization codes
      Emitter()->EmitTableIterInit(Bytecode::TableVectorIteratorInit, iter, exec_ctx, !table_oid, col_oids,
                                   static_cast<uint32_t>(arr_type->Length()));
      Emitter()->Emit(Bytecode::TableVectorIteratorPerformInit, iter);
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(Bytecode::TableVectorIteratorNext, cond, iter);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableIterReset: {
      Emitter()->Emit(Bytecode::TableVectorIteratorReset, iter);
      break;
    }
    case ast::Builtin::TableIterGetPCI: {
      ast::Type *pci_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::ProjectedColumnsIterator);
      LocalVar pci = ExecutionResult()->GetOrCreateDestination(pci_type);
      Emitter()->Emit(Bytecode::TableVectorIteratorGetPCI, pci, iter);
      ExecutionResult()->SetDestination(pci.ValueOf());
      break;
    }
    case ast::Builtin::TableIterClose: {
      Emitter()->Emit(Bytecode::TableVectorIteratorFree, iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterParallelCall(ast::CallExpr *call) {
  UNREACHABLE("Parallel scan is not implemented yet!");
}

void BytecodeGenerator::VisitBuiltinPCICall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();

  // The first argument to all calls is a pointer to the TVI
  LocalVar pci = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::PCIIsFiltered: {
      LocalVar is_filtered =
          ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(Bytecode::PCIIsFiltered, is_filtered, pci);
      ExecutionResult()->SetDestination(is_filtered.ValueOf());
      break;
    }
    case ast::Builtin::PCIHasNext:
    case ast::Builtin::PCIHasNextFiltered: {
      const Bytecode bytecode =
          builtin == ast::Builtin::PCIHasNext ? Bytecode::PCIHasNext : Bytecode::PCIHasNextFiltered;
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(bytecode, cond, pci);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::PCIAdvance:
    case ast::Builtin::PCIAdvanceFiltered: {
      const Bytecode bytecode =
          builtin == ast::Builtin::PCIAdvance ? Bytecode::PCIAdvance : Bytecode::PCIAdvanceFiltered;
      Emitter()->Emit(bytecode, pci);
      break;
    }
    case ast::Builtin::PCIMatch: {
      LocalVar match = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::PCIMatch, pci, match);
      break;
    }
    case ast::Builtin::PCIReset:
    case ast::Builtin::PCIResetFiltered: {
      const Bytecode bytecode = builtin == ast::Builtin::PCIReset ? Bytecode::PCIReset : Bytecode::PCIResetFiltered;
      Emitter()->Emit(bytecode, pci);
      break;
    }
    case ast::Builtin::PCIGetSlot: {
      LocalVar res = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::PCIGetSlot, res, pci);
      break;
    }
    case ast::Builtin::PCIGetBool: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetBool, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetBoolNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetBoolNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetTinyInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetTinyInt, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetTinyIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetTinyIntNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetSmallInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetSmallInt, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetSmallIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetSmallIntNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetInteger, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetIntegerNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetBigInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetBigInt, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetBigIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetBigIntNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetReal: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetReal, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetRealNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetRealNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDouble: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetDouble, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDoubleNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetDoubleNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDate: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetDateVal, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDateNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetDateValNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetTimestamp: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Timestamp));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetTimestampVal, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetTimestampNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Timestamp));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetTimestampValNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetVarlen: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetVarlen, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetVarlenNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPCIGet(Bytecode::PCIGetVarlenNull, val, pci, col_idx);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinHashCall(ast::CallExpr *call, UNUSED_ATTRIBUTE ast::Builtin builtin) {
  TERRIER_ASSERT(call->GetType()->Size() == sizeof(hash_t),
                 "Hash value size (from return type of @hash) doesn't match actual "
                 "size of hash_t type");

  // hash_val is where we accumulate all the hash values passed to the @hash()
  LocalVar hash_val = ExecutionResult()->GetOrCreateDestination(call->GetType());

  // Initialize it to 1
  Emitter()->EmitAssignImm8(hash_val, 1);

  // tmp is a temporary variable we use to store individual hash values. We
  // combine all values into hash_val above
  LocalVar tmp = CurrentFunction()->NewLocal(call->GetType());

  for (uint32_t idx = 0; idx < call->NumArgs(); idx++) {
    LocalVar input = VisitExpressionForLValue(call->Arguments()[idx]);
    TERRIER_ASSERT(call->Arguments()[idx]->GetType()->IsSqlValueType(), "Input to hash must be a SQL value type");
    auto *type = call->Arguments()[idx]->GetType()->As<ast::BuiltinType>();
    switch (type->GetKind()) {
      case ast::BuiltinType::Integer: {
        Emitter()->Emit(Bytecode::HashInt, tmp, input);
        break;
      }
      case ast::BuiltinType::Real: {
        Emitter()->Emit(Bytecode::HashReal, tmp, input);
        break;
      }
      case ast::BuiltinType::StringVal: {
        Emitter()->Emit(Bytecode::HashString, tmp, input);
        break;
      }
      default: {
        UNREACHABLE("Hashing this type isn't supported!");
      }
    }
    Emitter()->Emit(Bytecode::HashCombine, hash_val, tmp.ValueOf());
  }
  ExecutionResult()->SetDestination(hash_val.ValueOf());
}

void BytecodeGenerator::VisitBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar filter_manager = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::FilterManagerInit: {
      Emitter()->Emit(Bytecode::FilterManagerInit, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      Emitter()->Emit(Bytecode::FilterManagerStartNewClause, filter_manager);

      // Insert all flavors
      for (uint32_t arg_idx = 1; arg_idx < call->NumArgs(); arg_idx++) {
        const std::string func_name = call->Arguments()[arg_idx]->As<ast::IdentifierExpr>()->Name().Data();
        const FunctionId func_id = LookupFuncIdByName(func_name);
        Emitter()->EmitFilterManagerInsertFlavor(filter_manager, func_id);
      }
      break;
    }
    case ast::Builtin::FilterManagerFinalize: {
      Emitter()->Emit(Bytecode::FilterManagerFinalize, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      LocalVar pci = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::FilterManagerRunFilters, filter_manager, pci);
      break;
    }
    case ast::Builtin::FilterManagerFree: {
      Emitter()->Emit(Bytecode::FilterManagerFree, filter_manager);
      break;
    }
    default: {
      UNREACHABLE("Impossible filter manager call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinFilterCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar ret_val;
  if (ExecutionResult() != nullptr) {
    ret_val = ExecutionResult()->GetOrCreateDestination(call->GetType());
    ExecutionResult()->SetDestination(ret_val.ValueOf());
  } else {
    ret_val = CurrentFunction()->NewLocal(call->GetType());
  }

  // Collect the three call arguments
  // Projected Column Iterator
  LocalVar pci = VisitExpressionForRValue(call->Arguments()[0]);
  // Column index
  auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
  auto col_type = static_cast<int8_t>(call->Arguments()[2]->As<ast::LitExpr>()->Int64Val());
  // Filter value
  int64_t val = call->Arguments()[3]->As<ast::LitExpr>()->Int64Val();

  Bytecode bytecode;
  switch (builtin) {
    case ast::Builtin::FilterEq: {
      bytecode = Bytecode::PCIFilterEqual;
      break;
    }
    case ast::Builtin::FilterGt: {
      bytecode = Bytecode::PCIFilterGreaterThan;
      break;
    }
    case ast::Builtin::FilterGe: {
      bytecode = Bytecode::PCIFilterGreaterThanEqual;
      break;
    }
    case ast::Builtin::FilterLt: {
      bytecode = Bytecode::PCIFilterLessThan;
      break;
    }
    case ast::Builtin::FilterLe: {
      bytecode = Bytecode::PCIFilterLessThanEqual;
      break;
    }
    case ast::Builtin::FilterNe: {
      bytecode = Bytecode::PCIFilterNotEqual;
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
  Emitter()->EmitPCIVectorFilter(bytecode, ret_val, pci, col_idx, col_type, val);
}

void BytecodeGenerator::VisitBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableInit: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::AggregationHashTableInit, agg_ht, memory, entry_size);
      break;
    }
    case ast::Builtin::AggHashTableInsert: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::AggregationHashTableInsert, dest, agg_ht, hash);
      break;
    }
    case ast::Builtin::AggHashTableLookup: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[1]);
      auto key_eq_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().Data());
      LocalVar arg = VisitExpressionForRValue(call->Arguments()[3]);
      Emitter()->EmitAggHashTableLookup(dest, agg_ht, hash, key_eq_fn, arg);
      break;
    }
    case ast::Builtin::AggHashTableProcessBatch: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar iters = VisitExpressionForRValue(call->Arguments()[1]);
      auto hash_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().Data());
      auto key_eq_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().Data());
      auto init_agg_fn = LookupFuncIdByName(call->Arguments()[4]->As<ast::IdentifierExpr>()->Name().Data());
      auto merge_agg_fn = LookupFuncIdByName(call->Arguments()[5]->As<ast::IdentifierExpr>()->Name().Data());
      Emitter()->EmitAggHashTableProcessBatch(agg_ht, iters, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn);
      break;
    }
    case ast::Builtin::AggHashTableMovePartitions: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar aht_offset = VisitExpressionForRValue(call->Arguments()[2]);
      auto merge_part_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().Data());
      Emitter()->EmitAggHashTableMovePartitions(agg_ht, tls, aht_offset, merge_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableParallelPartitionedScan: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar ctx = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[2]);
      auto scan_part_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().Data());
      Emitter()->EmitAggHashTableParallelPartitionedScan(agg_ht, ctx, tls, scan_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableFree: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationHashTableFree, agg_ht);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation hash table bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableIterInit: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::AggregationHashTableIteratorInit, agg_ht_iter, agg_ht);
      break;
    }
    case ast::Builtin::AggHashTableIterHasNext: {
      LocalVar has_more = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationHashTableIteratorHasNext, has_more, agg_ht_iter);
      ExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterNext: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationHashTableIteratorNext, agg_ht_iter);
      break;
    }
    case ast::Builtin::AggHashTableIterGetRow: {
      LocalVar row_ptr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationHashTableIteratorGetRow, row_ptr, agg_ht_iter);
      ExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterClose: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationHashTableIteratorFree, agg_ht_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation hash table iteration bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggPartIterHasNext: {
      LocalVar has_more = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorHasNext, has_more, iter);
      ExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterNext: {
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorNext, iter);
      break;
    }
    case ast::Builtin::AggPartIterGetRow: {
      LocalVar row = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRow, row, iter);
      ExecutionResult()->SetDestination(row.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      LocalVar hash = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetHash, hash, iter);
      ExecutionResult()->SetDestination(hash.ValueOf());
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation partition iterator bytecode");
    }
  }
}

namespace {

#define AGG_CODES(F)                                                                                                   \
  F(CountAggregate, CountAggregateInit, CountAggregateAdvance, CountAggregateGetResult, CountAggregateMerge,           \
    CountAggregateReset)                                                                                               \
  F(CountStarAggregate, CountStarAggregateInit, CountStarAggregateAdvance, CountStarAggregateGetResult,                \
    CountStarAggregateMerge, CountStarAggregateReset)                                                                  \
  F(IntegerAvgAggregate, AvgAggregateInit, IntegerAvgAggregateAdvance, AvgAggregateGetResult, AvgAggregateMerge,       \
    AvgAggregateReset)                                                                                                 \
  F(RealAvgAggregate, AvgAggregateInit, RealAvgAggregateAdvance, AvgAggregateGetResult, AvgAggregateMerge,             \
    AvgAggregateReset)                                                                                                 \
  F(IntegerMaxAggregate, IntegerMaxAggregateInit, IntegerMaxAggregateAdvance, IntegerMaxAggregateGetResult,            \
    IntegerMaxAggregateMerge, IntegerMaxAggregateReset)                                                                \
  F(IntegerMinAggregate, IntegerMinAggregateInit, IntegerMinAggregateAdvance, IntegerMinAggregateGetResult,            \
    IntegerMinAggregateMerge, IntegerMinAggregateReset)                                                                \
  F(IntegerSumAggregate, IntegerSumAggregateInit, IntegerSumAggregateAdvance, IntegerSumAggregateGetResult,            \
    IntegerSumAggregateMerge, IntegerSumAggregateReset)                                                                \
  F(RealMaxAggregate, RealMaxAggregateInit, RealMaxAggregateAdvance, RealMaxAggregateGetResult, RealMaxAggregateMerge, \
    RealMaxAggregateReset)                                                                                             \
  F(RealMinAggregate, RealMinAggregateInit, RealMinAggregateAdvance, RealMinAggregateGetResult, RealMinAggregateMerge, \
    RealMinAggregateReset)                                                                                             \
  F(RealSumAggregate, RealSumAggregateInit, RealSumAggregateAdvance, RealSumAggregateGetResult, RealSumAggregateMerge, \
    RealSumAggregateReset)

enum class AggOpKind : uint8_t { Init = 0, Advance = 1, GetResult = 2, Merge = 3, Reset = 4 };

// Given an aggregate kind and the operation to perform on it, determine the
// appropriate bytecode
template <AggOpKind OpKind>
Bytecode OpForAgg(ast::BuiltinType::Kind agg_kind);

template <>
Bytecode OpForAgg<AggOpKind::Init>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Init;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Advance>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Advance;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::GetResult>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::GetResult;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Merge>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Merge;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

template <>
Bytecode OpForAgg<AggOpKind::Reset>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: {
      UNREACHABLE("Impossible aggregate type");
    }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Reset;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

#undef AGG_CODES

}  // namespace

void BytecodeGenerator::VisitBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggInit:
    case ast::Builtin::AggReset: {
      for (const auto &arg : call->Arguments()) {
        const auto agg_kind = arg->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
        LocalVar input = VisitExpressionForRValue(arg);
        Bytecode bytecode;
        if (builtin == ast::Builtin::AggInit) {
          bytecode = OpForAgg<AggOpKind::Init>(agg_kind);
        } else {
          bytecode = OpForAgg<AggOpKind::Reset>(agg_kind);
        }
        Emitter()->Emit(bytecode, input);
      }
      break;
    }
    case ast::Builtin::AggAdvance: {
      const auto &args = call->Arguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar agg = VisitExpressionForRValue(args[0]);
      LocalVar input = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Advance>(agg_kind);
      Emitter()->Emit(bytecode, agg, input);
      break;
    }
    case ast::Builtin::AggMerge: {
      const auto &args = call->Arguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar agg_1 = VisitExpressionForRValue(args[0]);
      LocalVar agg_2 = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Merge>(agg_kind);
      Emitter()->Emit(bytecode, agg_1, agg_2);
      break;
    }
    case ast::Builtin::AggResult: {
      const auto &args = call->Arguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar result = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg = VisitExpressionForRValue(args[0]);
      Bytecode bytecode = OpForAgg<AggOpKind::GetResult>(agg_kind);
      Emitter()->Emit(bytecode, result, agg);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregator call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::JoinHashTableInit: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::JoinHashTableInit, join_hash_table, memory, entry_size);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::JoinHashTableAllocTuple, dest, join_hash_table, hash);
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::JoinHashTableBuild, join_hash_table);
      break;
    }
    case ast::Builtin::JoinHashTableIterInit: {
      LocalVar iterator = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::JoinHashTableIterInit, iterator, join_hash_table, hash);
      break;
    }
    case ast::Builtin::JoinHashTableIterHasNext: {
      LocalVar has_more = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iterator = VisitExpressionForRValue(call->Arguments()[0]);
      const std::string key_eq_name = call->Arguments()[1]->As<ast::IdentifierExpr>()->Name().Data();
      LocalVar opaque_ctx = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar probe_tuple = VisitExpressionForRValue(call->Arguments()[3]);
      Emitter()->EmitJoinHashTableIterHasNext(has_more, iterator, LookupFuncIdByName(key_eq_name), opaque_ctx,
                                              probe_tuple);
      ExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableIterGetRow: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iterator = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::JoinHashTableIterGetRow, dest, iterator);
      break;
    }
    case ast::Builtin::JoinHashTableIterClose: {
      LocalVar iterator = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::JoinHashTableIterClose, iterator);
      break;
    }
    case ast::Builtin::JoinHashTableBuildParallel: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar jht_offset = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::JoinHashTableBuildParallel, join_hash_table, tls, jht_offset);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::JoinHashTableFree, join_hash_table);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinSorterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::SorterInit: {
      // TODO(pmenon): Fix me so that the comparison function doesn't have be
      // listed by name.
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->Arguments()[1]);
      const std::string cmp_func_name = call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().Data();
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[3]);
      Emitter()->EmitSorterInit(Bytecode::SorterInit, sorter, memory, LookupFuncIdByName(cmp_func_name), entry_size);
      break;
    }
    case ast::Builtin::SorterInsert: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::SorterAllocTuple, dest, sorter);
      break;
    }
    case ast::Builtin::SorterInsertTopK: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::SorterAllocTupleTopK, dest, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterInsertTopKFinish: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::SorterAllocTupleTopKFinish, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterSort: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::SorterSort, sorter);
      break;
    }
    case ast::Builtin::SorterSortParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::SorterSortParallel, sorter, tls, sorter_offset);
      break;
    }
    case ast::Builtin::SorterSortTopKParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar top_k = VisitExpressionForRValue(call->Arguments()[3]);
      Emitter()->Emit(Bytecode::SorterSortTopKParallel, sorter, tls, sorter_offset, top_k);
      break;
    }
    case ast::Builtin::SorterFree: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      Emitter()->Emit(Bytecode::SorterFree, sorter);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();

  // The first argument to all calls is the sorter iterator instance
  const LocalVar sorter_iter = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::SorterIteratorInit, sorter_iter, sorter);
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(Bytecode::SorterIteratorHasNext, cond, sorter_iter);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterNext: {
      Emitter()->Emit(Bytecode::SorterIteratorNext, sorter_iter);
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      LocalVar row_ptr =
          ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Uint8)->PointerTo());
      Emitter()->Emit(Bytecode::SorterIteratorGetRow, row_ptr, sorter_iter);
      ExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      Emitter()->Emit(Bytecode::SorterIteratorFree, sorter_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitExecutionContextCall(ast::CallExpr *call, UNUSED_ATTRIBUTE ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();

  // The execution context pointer
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::ExecutionContextStartResourceTracker: {
      LocalVar cmp = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::ExecutionContextStartResourceTracker, exec_ctx, cmp);
      break;
    }
    case ast::Builtin::ExecutionContextEndResourceTracker: {
      LocalVar name = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::ExecutionContextEndResourceTracker, exec_ctx, name);
      break;
    }
    case ast::Builtin::ExecutionContextEndPipelineTracker: {
      LocalVar query_id = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar pipeline_id = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::ExecutionContextEndPipelineTracker, exec_ctx, query_id, pipeline_id);
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      // The memory pool pointer
      LocalVar mem_pool = ExecutionResult()->GetOrCreateDestination(
          ast::BuiltinType::Get(ctx, ast::BuiltinType::MemoryPool)->PointerTo());

      // Emit bytecode
      Emitter()->Emit(Bytecode::ExecutionContextGetMemoryPool, mem_pool, exec_ctx);

      // Indicate where the result is
      ExecutionResult()->SetDestination(mem_pool.ValueOf());

      break;
    }
    default: {
      UNREACHABLE("Impossible execution context call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar tls = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::ThreadStateContainerInit: {
      LocalVar memory = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::ThreadStateContainerInit, tls, memory);
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      LocalVar ctx = VisitExpressionForRValue(call->Arguments()[1]);
      FunctionId iterate_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().Data());
      Emitter()->EmitThreadStateContainerIterate(tls, ctx, iterate_fn);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[1]);
      FunctionId init_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().Data());
      FunctionId destroy_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().Data());
      LocalVar ctx = VisitExpressionForRValue(call->Arguments()[4]);
      Emitter()->EmitThreadStateContainerReset(tls, entry_size, init_fn, destroy_fn, ctx);
      break;
    }
    case ast::Builtin::ThreadStateContainerFree: {
      Emitter()->Emit(Bytecode::ThreadStateContainerFree, tls);
      break;
    }
    default: {
      UNREACHABLE("Impossible thread state container call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
  LocalVar src = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::ACos: {
      Emitter()->Emit(Bytecode::Acos, dest, src);
      break;
    }
    case ast::Builtin::ASin: {
      Emitter()->Emit(Bytecode::Asin, dest, src);
      break;
    }
    case ast::Builtin::ATan: {
      Emitter()->Emit(Bytecode::Atan, dest, src);
      break;
    }
    case ast::Builtin::ATan2: {
      Emitter()->Emit(Bytecode::Atan2, dest, src);
      break;
    }
    case ast::Builtin::Cos: {
      Emitter()->Emit(Bytecode::Cos, dest, src);
      break;
    }
    case ast::Builtin::Cot: {
      Emitter()->Emit(Bytecode::Cot, dest, src);
      break;
    }
    case ast::Builtin::Sin: {
      Emitter()->Emit(Bytecode::Sin, dest, src);
      break;
    }
    case ast::Builtin::Tan: {
      Emitter()->Emit(Bytecode::Tan, dest, src);
      break;
    }
    default: {
      UNREACHABLE("Impossible trigonometric bytecode");
    }
  }

  ExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitBuiltinSizeOfCall(ast::CallExpr *call) {
  ast::Type *target_type = call->Arguments()[0]->GetType();
  LocalVar size_var = ExecutionResult()->GetOrCreateDestination(
      ast::BuiltinType::Get(target_type->GetContext(), ast::BuiltinType::Uint32));
  Emitter()->EmitAssignImm4(size_var, target_type->Size());
  ExecutionResult()->SetDestination(size_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinOutputCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::OutputAlloc: {
      LocalVar dest = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->EmitOutputAlloc(Bytecode::OutputAlloc, exec_ctx, dest);
      break;
    }
    case ast::Builtin::OutputFinalize: {
      Emitter()->EmitOutputCall(Bytecode::OutputFinalize, exec_ctx);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinIndexIteratorCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar iterator = VisitExpressionForRValue(call->Arguments()[0]);
  ast::Context *ctx = call->GetType()->GetContext();

  switch (builtin) {
    case ast::Builtin::IndexIteratorInit: {
      // Execution context
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      // Num attrs
      auto num_attrs = static_cast<uint32_t>(call->Arguments()[2]->As<ast::LitExpr>()->Int64Val());
      // Table OID
      auto table_oid = static_cast<uint32_t>(call->Arguments()[3]->As<ast::LitExpr>()->Int64Val());
      // Index OID
      auto index_oid = static_cast<uint32_t>(call->Arguments()[4]->As<ast::LitExpr>()->Int64Val());
      // Col OIDs
      auto *arr_type = call->Arguments()[5]->GetType()->As<ast::ArrayType>();
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[5]);
      // Emit the initialization codes
      Emitter()->EmitIndexIteratorInit(Bytecode::IndexIteratorInit, iterator, exec_ctx, num_attrs, table_oid, index_oid,
                                       col_oids, static_cast<uint32_t>(arr_type->Length()));
      Emitter()->Emit(Bytecode::IndexIteratorPerformInit, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorInitBind: {
      // Exec Ctx
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      // Num attrs
      auto num_attrs = static_cast<uint32_t>(call->Arguments()[2]->As<ast::LitExpr>()->Int64Val());
      // Table Name
      std::string table_name(call->Arguments()[3]->As<ast::LitExpr>()->RawStringVal().Data());
      auto ns_oid = exec_ctx_->GetAccessor()->GetDefaultNamespace();
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name);
      TERRIER_ASSERT(table_oid != terrier::catalog::INVALID_TABLE_OID, "Table does not exists");
      // Index Name
      std::string index_name(call->Arguments()[4]->As<ast::LitExpr>()->RawStringVal().Data());
      auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(ns_oid, index_name);
      TERRIER_ASSERT(index_oid != terrier::catalog::INVALID_INDEX_OID, "Index does not exists");
      // Col OIDs
      auto *arr_type = call->Arguments()[5]->GetType()->As<ast::ArrayType>();
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[5]);
      // Emit the initialization codes
      Emitter()->EmitIndexIteratorInit(Bytecode::IndexIteratorInit, iterator, exec_ctx, num_attrs, !table_oid,
                                       !index_oid, col_oids, static_cast<uint32_t>(arr_type->Length()));
      Emitter()->Emit(Bytecode::IndexIteratorPerformInit, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorScanKey: {
      Emitter()->Emit(Bytecode::IndexIteratorScanKey, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorScanAscending: {
      auto asc_type = VisitExpressionForRValue(call->Arguments()[1]);
      auto limit = VisitExpressionForRValue(call->Arguments()[2]);
      Emitter()->Emit(Bytecode::IndexIteratorScanAscending, iterator, asc_type, limit);
      break;
    }
    case ast::Builtin::IndexIteratorScanDescending: {
      Emitter()->Emit(Bytecode::IndexIteratorScanDescending, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorScanLimitDescending: {
      auto limit = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::IndexIteratorScanLimitDescending, iterator, limit);
      break;
    }
    case ast::Builtin::IndexIteratorAdvance: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(Bytecode::IndexIteratorAdvance, cond, iterator);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      Emitter()->Emit(Bytecode::IndexIteratorFree, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetPR: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::IndexIteratorGetPR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetLoPR: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::IndexIteratorGetLoPR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetHiPR: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::IndexIteratorGetHiPR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetTablePR: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::IndexIteratorGetTablePR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetSlot: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::IndexIteratorGetSlot, pr, iterator);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinPRCall(ast::CallExpr *call, ast::Builtin builtin) {
  // First argument is always a projected row
  LocalVar pr = VisitExpressionForRValue(call->Arguments()[0]);
  ast::Context *ctx = call->GetType()->GetContext();
  switch (builtin) {
    case ast::Builtin::PRSetBool: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetBool, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTinyInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetTinyInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetSmallInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetSmallInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetBigInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetBigInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetReal: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetReal, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDouble: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetDouble, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDate: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetDateVal, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTimestamp: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetTimestampVal, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetVarlen: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      LocalVar own = VisitExpressionForRValue(call->Arguments()[3]);
      Emitter()->EmitPRSetVarlen(Bytecode::PRSetVarlen, pr, col_idx, val, own);
      break;
    }
    case ast::Builtin::PRSetBoolNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetBoolNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTinyIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetTinyIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetSmallIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetSmallIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetBigIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetBigIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetRealNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetRealNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDoubleNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetDoubleNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDateNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetDateValNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTimestampNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      Emitter()->EmitPRSet(Bytecode::PRSetTimestampValNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetVarlenNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      LocalVar own = VisitExpressionForRValue(call->Arguments()[3]);
      Emitter()->EmitPRSetVarlen(Bytecode::PRSetVarlenNull, pr, col_idx, val, own);
      break;
    }
    case ast::Builtin::PRGetBool: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetBool, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTinyInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetTinyInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetSmallInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetSmallInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetBigInt: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetBigInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetReal: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetReal, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDouble: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetDouble, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDate: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetDateVal, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTimestamp: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Timestamp));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetTimestampVal, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetVarlen: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetVarlen, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetBoolNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetBoolNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTinyIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetTinyIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetSmallIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetSmallIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetBigIntNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetBigIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetRealNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetRealNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDoubleNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetDoubleNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDateNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetDateValNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTimestampNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetTimestampValNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetVarlenNull: {
      LocalVar val = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitPRGet(Bytecode::PRGetVarlenNull, val, pr, col_idx);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinStorageInterfaceCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->GetType()->GetContext();
  LocalVar storage_interface = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::StorageInterfaceInit: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      auto table_oid = static_cast<uint32_t>(call->Arguments()[2]->As<ast::LitExpr>()->Int64Val());
      auto *arr_type = call->Arguments()[3]->GetType()->As<ast::ArrayType>();
      auto num_oids = static_cast<uint32_t>(arr_type->Length());
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[3]);
      LocalVar is_index_key_update = VisitExpressionForRValue(call->Arguments()[4]);
      Emitter()->EmitStorageInterfaceInit(Bytecode::StorageInterfaceInit, storage_interface, exec_ctx, table_oid,
                                          col_oids, num_oids, is_index_key_update);
      break;
    }
    case ast::Builtin::StorageInterfaceInitBind: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      ast::Identifier table_name = call->Arguments()[2]->As<ast::LitExpr>()->RawStringVal();
      auto ns_oid = exec_ctx_->GetAccessor()->GetDefaultNamespace();
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name.Data());
      auto *arr_type = call->Arguments()[3]->GetType()->As<ast::ArrayType>();
      auto num_oids = static_cast<uint32_t>(arr_type->Length());
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[3]);
      LocalVar is_index_key_update = VisitExpressionForRValue(call->Arguments()[4]);
      Emitter()->EmitStorageInterfaceInit(Bytecode::StorageInterfaceInit, storage_interface, exec_ctx, !table_oid,
                                          col_oids, num_oids, is_index_key_update);
      break;
    }
    case ast::Builtin::GetTablePR: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      Emitter()->Emit(Bytecode::StorageInterfaceGetTablePR, pr, storage_interface);
      break;
    }
    case ast::Builtin::TableInsert: {
      ast::Type *tuple_slot_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::TupleSlot);
      LocalVar tuple_slot = ExecutionResult()->GetOrCreateDestination(tuple_slot_type);
      Emitter()->Emit(Bytecode::StorageInterfaceTableInsert, tuple_slot, storage_interface);
      break;
    }
    case ast::Builtin::TableDelete: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::StorageInterfaceTableDelete, cond, storage_interface, tuple_slot);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableUpdate: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::StorageInterfaceTableUpdate, cond, storage_interface, tuple_slot);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::GetIndexPR: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      auto index_oid = static_cast<uint32_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      Emitter()->EmitStorageInterfaceGetIndexPR(Bytecode::StorageInterfaceGetIndexPR, pr, storage_interface, index_oid);
      break;
    }
    case ast::Builtin::GetIndexPRBind: {
      LocalVar pr = ExecutionResult()->GetOrCreateDestination(call->GetType());
      ast::Identifier index_name = call->Arguments()[1]->As<ast::LitExpr>()->RawStringVal();
      auto ns_oid = exec_ctx_->GetAccessor()->GetDefaultNamespace();
      auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(ns_oid, index_name.Data());
      Emitter()->EmitStorageInterfaceGetIndexPR(Bytecode::StorageInterfaceGetIndexPR, pr, storage_interface,
                                                !index_oid);
      break;
    }
    case ast::Builtin::IndexInsert: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(Bytecode::StorageInterfaceIndexInsert, cond, storage_interface);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexInsertUnique: {
      LocalVar cond = ExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      Emitter()->Emit(Bytecode::StorageInterfaceIndexInsertUnique, cond, storage_interface);
      ExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexDelete: {
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::StorageInterfaceIndexDelete, storage_interface, tuple_slot);
      break;
    }

    case ast::Builtin::StorageInterfaceFree: {
      Emitter()->Emit(Bytecode::StorageInterfaceFree, storage_interface);
      break;
    }
    default:
      UNREACHABLE("Undefined storage_interface call!");
  }
}

void BytecodeGenerator::VisitBuiltinParamCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  LocalVar param_idx = VisitExpressionForRValue(call->Arguments()[1]);
  LocalVar ret = ExecutionResult()->GetOrCreateDestination(call->GetType());
  switch (builtin) {
    case ast::Builtin::GetParamBool:
      Emitter()->Emit(Bytecode::GetParamBool, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamTinyInt:
      Emitter()->Emit(Bytecode::GetParamTinyInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamSmallInt:
      Emitter()->Emit(Bytecode::GetParamSmallInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamInt:
      Emitter()->Emit(Bytecode::GetParamInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamBigInt:
      Emitter()->Emit(Bytecode::GetParamBigInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamReal:
      Emitter()->Emit(Bytecode::GetParamReal, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamDouble:
      Emitter()->Emit(Bytecode::GetParamDouble, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamDate:
      Emitter()->Emit(Bytecode::GetParamDateVal, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamTimestamp:
      Emitter()->Emit(Bytecode::GetParamTimestampVal, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamString:
      Emitter()->Emit(Bytecode::GetParamString, ret, exec_ctx, param_idx);
      break;
    default:
      UNREACHABLE("Impossible parameter call!");
  }
}

void BytecodeGenerator::VisitBuiltinStringCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  LocalVar ret = ExecutionResult()->GetOrCreateDestination(call->GetType());
  switch (builtin) {
    case ast::Builtin::Lower: {
      LocalVar input_string = VisitExpressionForRValue(call->Arguments()[1]);
      Emitter()->Emit(Bytecode::Lower, exec_ctx, ret, input_string);
      break;
    }
    case ast::Builtin::Version: {
      Emitter()->Emit(Bytecode::Version, exec_ctx, ret);
      break;
    }
    default:
      UNREACHABLE("Unimplemented string function!");
  }
}

void BytecodeGenerator::VisitBuiltinCallExpr(ast::CallExpr *call) {
  ast::Builtin builtin;

  ast::Context *ctx = call->GetType()->GetContext();
  ctx->IsBuiltinFunction(call->GetFuncName(), &builtin);

  switch (builtin) {
    case ast::Builtin::IsSqlNull:
    case ast::Builtin::IsSqlNotNull:
    case ast::Builtin::NullToSql: {
      VisitSqlNullCall(call, builtin);
      break;
    }
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::TimestampToSql:
    case ast::Builtin::TimestampToSqlYMDHMSMU:
    case ast::Builtin::VarlenToSql:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool: {
      VisitSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::ExtractYear: {
      VisitBuiltinDateFunctionCall(call, builtin);
      break;
    }
    case ast::Builtin::FilterEq:
    case ast::Builtin::FilterGt:
    case ast::Builtin::FilterGe:
    case ast::Builtin::FilterLt:
    case ast::Builtin::FilterLe:
    case ast::Builtin::FilterNe: {
      VisitBuiltinFilterCall(call, builtin);
      break;
    }
    case ast::Builtin::ExecutionContextStartResourceTracker:
    case ast::Builtin::ExecutionContextEndResourceTracker:
    case ast::Builtin::ExecutionContextEndPipelineTracker:
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      VisitExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ThreadStateContainerInit:
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerFree: {
      VisitBuiltinThreadStateContainerCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterInit:
    case ast::Builtin::TableIterInitBind:
    case ast::Builtin::TableIterAdvance:
    case ast::Builtin::TableIterReset:
    case ast::Builtin::TableIterGetPCI:
    case ast::Builtin::TableIterClose: {
      VisitBuiltinTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterParallel: {
      VisitBuiltinTableIterParallelCall(call);
      break;
    }
    case ast::Builtin::PCIIsFiltered:
    case ast::Builtin::PCIHasNext:
    case ast::Builtin::PCIHasNextFiltered:
    case ast::Builtin::PCIAdvance:
    case ast::Builtin::PCIAdvanceFiltered:
    case ast::Builtin::PCIMatch:
    case ast::Builtin::PCIReset:
    case ast::Builtin::PCIResetFiltered:
    case ast::Builtin::PCIGetSlot:
    case ast::Builtin::PCIGetBool:
    case ast::Builtin::PCIGetBoolNull:
    case ast::Builtin::PCIGetTinyInt:
    case ast::Builtin::PCIGetTinyIntNull:
    case ast::Builtin::PCIGetSmallInt:
    case ast::Builtin::PCIGetSmallIntNull:
    case ast::Builtin::PCIGetInt:
    case ast::Builtin::PCIGetIntNull:
    case ast::Builtin::PCIGetBigInt:
    case ast::Builtin::PCIGetBigIntNull:
    case ast::Builtin::PCIGetReal:
    case ast::Builtin::PCIGetRealNull:
    case ast::Builtin::PCIGetDouble:
    case ast::Builtin::PCIGetDoubleNull:
    case ast::Builtin::PCIGetDate:
    case ast::Builtin::PCIGetDateNull:
    case ast::Builtin::PCIGetTimestamp:
    case ast::Builtin::PCIGetTimestampNull:
    case ast::Builtin::PCIGetVarlen:
    case ast::Builtin::PCIGetVarlenNull: {
      VisitBuiltinPCICall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      VisitBuiltinHashCall(call, builtin);
      break;
    };
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
    case ast::Builtin::FilterManagerFinalize:
    case ast::Builtin::FilterManagerRunFilters:
    case ast::Builtin::FilterManagerFree: {
      VisitBuiltinFilterManagerCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableInit:
    case ast::Builtin::AggHashTableInsert:
    case ast::Builtin::AggHashTableLookup:
    case ast::Builtin::AggHashTableProcessBatch:
    case ast::Builtin::AggHashTableMovePartitions:
    case ast::Builtin::AggHashTableParallelPartitionedScan:
    case ast::Builtin::AggHashTableFree: {
      VisitBuiltinAggHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::AggPartIterHasNext:
    case ast::Builtin::AggPartIterNext:
    case ast::Builtin::AggPartIterGetRow:
    case ast::Builtin::AggPartIterGetHash: {
      VisitBuiltinAggPartIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableIterInit:
    case ast::Builtin::AggHashTableIterHasNext:
    case ast::Builtin::AggHashTableIterNext:
    case ast::Builtin::AggHashTableIterGetRow:
    case ast::Builtin::AggHashTableIterClose: {
      VisitBuiltinAggHashTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggInit:
    case ast::Builtin::AggAdvance:
    case ast::Builtin::AggMerge:
    case ast::Builtin::AggReset:
    case ast::Builtin::AggResult: {
      VisitBuiltinAggregatorCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableInit:
    case ast::Builtin::JoinHashTableInsert:
    case ast::Builtin::JoinHashTableIterInit:
    case ast::Builtin::JoinHashTableIterGetRow:
    case ast::Builtin::JoinHashTableIterHasNext:
    case ast::Builtin::JoinHashTableIterClose:
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableBuildParallel:
    case ast::Builtin::JoinHashTableFree: {
      VisitBuiltinJoinHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterInit:
    case ast::Builtin::SorterInsert:
    case ast::Builtin::SorterInsertTopK:
    case ast::Builtin::SorterInsertTopKFinish:
    case ast::Builtin::SorterSort:
    case ast::Builtin::SorterSortParallel:
    case ast::Builtin::SorterSortTopKParallel:
    case ast::Builtin::SorterFree: {
      VisitBuiltinSorterCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterIterInit:
    case ast::Builtin::SorterIterHasNext:
    case ast::Builtin::SorterIterNext:
    case ast::Builtin::SorterIterGetRow:
    case ast::Builtin::SorterIterClose: {
      VisitBuiltinSorterIterCall(call, builtin);
      break;
    }
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::ATan2:
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan: {
      VisitBuiltinTrigCall(call, builtin);
      break;
    }
    case ast::Builtin::SizeOf: {
      VisitBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::PtrCast: {
      Visit(call->Arguments()[1]);
      break;
    }
    case ast::Builtin::OutputAlloc:
    case ast::Builtin::OutputFinalize:
      VisitBuiltinOutputCall(call, builtin);
      break;
    case ast::Builtin::IndexIteratorInit:
    case ast::Builtin::IndexIteratorInitBind:
    case ast::Builtin::IndexIteratorScanKey:
    case ast::Builtin::IndexIteratorScanAscending:
    case ast::Builtin::IndexIteratorScanDescending:
    case ast::Builtin::IndexIteratorScanLimitDescending:
    case ast::Builtin::IndexIteratorAdvance:
    case ast::Builtin::IndexIteratorFree:
    case ast::Builtin::IndexIteratorGetPR:
    case ast::Builtin::IndexIteratorGetLoPR:
    case ast::Builtin::IndexIteratorGetHiPR:
    case ast::Builtin::IndexIteratorGetTablePR:
    case ast::Builtin::IndexIteratorGetSlot:
      VisitBuiltinIndexIteratorCall(call, builtin);
      break;
    case ast::Builtin::PRSetBool:
    case ast::Builtin::PRSetTinyInt:
    case ast::Builtin::PRSetSmallInt:
    case ast::Builtin::PRSetInt:
    case ast::Builtin::PRSetBigInt:
    case ast::Builtin::PRSetReal:
    case ast::Builtin::PRSetDouble:
    case ast::Builtin::PRSetDate:
    case ast::Builtin::PRSetTimestamp:
    case ast::Builtin::PRSetVarlen:
    case ast::Builtin::PRSetBoolNull:
    case ast::Builtin::PRSetTinyIntNull:
    case ast::Builtin::PRSetSmallIntNull:
    case ast::Builtin::PRSetIntNull:
    case ast::Builtin::PRSetBigIntNull:
    case ast::Builtin::PRSetRealNull:
    case ast::Builtin::PRSetDoubleNull:
    case ast::Builtin::PRSetDateNull:
    case ast::Builtin::PRSetTimestampNull:
    case ast::Builtin::PRSetVarlenNull:
    case ast::Builtin::PRGetBool:
    case ast::Builtin::PRGetTinyInt:
    case ast::Builtin::PRGetSmallInt:
    case ast::Builtin::PRGetInt:
    case ast::Builtin::PRGetBigInt:
    case ast::Builtin::PRGetReal:
    case ast::Builtin::PRGetDouble:
    case ast::Builtin::PRGetDate:
    case ast::Builtin::PRGetTimestamp:
    case ast::Builtin::PRGetVarlen:
    case ast::Builtin::PRGetBoolNull:
    case ast::Builtin::PRGetTinyIntNull:
    case ast::Builtin::PRGetSmallIntNull:
    case ast::Builtin::PRGetIntNull:
    case ast::Builtin::PRGetBigIntNull:
    case ast::Builtin::PRGetRealNull:
    case ast::Builtin::PRGetDoubleNull:
    case ast::Builtin::PRGetDateNull:
    case ast::Builtin::PRGetTimestampNull:
    case ast::Builtin::PRGetVarlenNull: {
      VisitBuiltinPRCall(call, builtin);
      break;
    }
    case ast::Builtin::StorageInterfaceInit:
    case ast::Builtin::StorageInterfaceInitBind:
    case ast::Builtin::GetTablePR:
    case ast::Builtin::TableInsert:
    case ast::Builtin::TableDelete:
    case ast::Builtin::TableUpdate:
    case ast::Builtin::GetIndexPR:
    case ast::Builtin::GetIndexPRBind:
    case ast::Builtin::IndexInsert:
    case ast::Builtin::IndexInsertUnique:
    case ast::Builtin::IndexDelete:
    case ast::Builtin::StorageInterfaceFree: {
      VisitBuiltinStorageInterfaceCall(call, builtin);
      break;
    }
    case ast::Builtin::GetParamBool:
    case ast::Builtin::GetParamTinyInt:
    case ast::Builtin::GetParamSmallInt:
    case ast::Builtin::GetParamInt:
    case ast::Builtin::GetParamBigInt:
    case ast::Builtin::GetParamReal:
    case ast::Builtin::GetParamDouble:
    case ast::Builtin::GetParamDate:
    case ast::Builtin::GetParamTimestamp:
    case ast::Builtin::GetParamString: {
      VisitBuiltinParamCall(call, builtin);
      break;
    }

    case ast::Builtin::Lower:
    case ast::Builtin::Version: {
      VisitBuiltinStringCall(call, builtin);
      break;
    }

    case ast::Builtin::NpRunnersEmitInt:
    case ast::Builtin::NpRunnersEmitReal: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar num_tuple = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar num_col = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar int_col = VisitExpressionForRValue(call->Arguments()[3]);
      LocalVar real_col = VisitExpressionForRValue(call->Arguments()[4]);
      if (builtin == ast::Builtin::NpRunnersEmitInt) {
        Emitter()->Emit(Bytecode::NpRunnersEmitInt, exec_ctx, num_tuple, num_col, int_col, real_col);
      } else {
        Emitter()->Emit(Bytecode::NpRunnersEmitReal, exec_ctx, num_tuple, num_col, int_col, real_col);
      }

      break;
    }

    case ast::Builtin::NpRunnersDummyInt:
    case ast::Builtin::NpRunnersDummyReal: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
      if (builtin == ast::Builtin::NpRunnersDummyInt) {
        Emitter()->Emit(Bytecode::NpRunnersDummyInt, exec_ctx);
      } else {
        Emitter()->Emit(Bytecode::NpRunnersDummyReal, exec_ctx);
      }

      break;
    }

    default: {
      UNREACHABLE("Builtin not supported!");
    }
  }
}

void BytecodeGenerator::VisitRegularCallExpr(ast::CallExpr *call) {
  bool caller_wants_result = ExecutionResult() != nullptr;
  TERRIER_ASSERT(!caller_wants_result || ExecutionResult()->IsRValue(), "Calls can only be R-Values!");

  std::vector<LocalVar> params;

  auto *func_type = call->Function()->GetType()->As<ast::FunctionType>();

  if (!func_type->ReturnType()->IsNilType()) {
    LocalVar ret_val;
    if (caller_wants_result) {
      ret_val = ExecutionResult()->GetOrCreateDestination(func_type->ReturnType());

      // Let the caller know where the result value is
      ExecutionResult()->SetDestination(ret_val.ValueOf());
    } else {
      ret_val = CurrentFunction()->NewLocal(func_type->ReturnType());
    }

    // Push return value address into parameter list
    params.push_back(ret_val);
  }

  // Collect non-return-value parameters as usual
  for (uint32_t i = 0; i < func_type->NumParams(); i++) {
    params.push_back(VisitExpressionForRValue(call->Arguments()[i]));
  }

  // Emit call
  const auto func_id = LookupFuncIdByName(call->GetFuncName().Data());
  TERRIER_ASSERT(func_id != FunctionInfo::K_INVALID_FUNC_ID, "Function not found!");
  Emitter()->EmitCall(func_id, params);
}

void BytecodeGenerator::VisitCallExpr(ast::CallExpr *node) {
  ast::CallExpr::CallKind call_kind = node->GetCallKind();

  if (call_kind == ast::CallExpr::CallKind::Builtin) {
    VisitBuiltinCallExpr(node);
  } else {
    VisitRegularCallExpr(node);
  }
}

void BytecodeGenerator::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  LocalVar dest = VisitExpressionForLValue(node->Destination());
  VisitExpressionForRValue(node->Source(), dest);
}

void BytecodeGenerator::VisitFile(ast::File *node) {
  for (auto *decl : node->Declarations()) {
    Visit(decl);
  }
}

void BytecodeGenerator::VisitLitExpr(ast::LitExpr *node) {
  TERRIER_ASSERT(ExecutionResult()->IsRValue(), "Literal expressions should be R-Values!");

  LocalVar target = ExecutionResult()->GetOrCreateDestination(node->GetType());
  switch (node->LiteralKind()) {
    case ast::LitExpr::LitKind::Nil: {
      // Do nothing
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      Emitter()->EmitAssignImm1(target, static_cast<int8_t>(node->BoolVal()));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      Emitter()->EmitAssignImm8(target, node->Int64Val());
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      Emitter()->EmitAssignImm8F(target, node->Float64Val());
      break;
    }
    default: {
      EXECUTION_LOG_ERROR("Non-bool or non-integer literals not supported in bytecode");
      break;
    }
  }

  ExecutionResult()->SetDestination(target.ValueOf());
}

void BytecodeGenerator::VisitStructDecl(UNUSED_ATTRIBUTE ast::StructDecl *node) {
  // Nothing to do
}

void BytecodeGenerator::VisitLogicalAndOrExpr(ast::BinaryOpExpr *node) {
  // TERRIER_ASSERT(ExecutionResult()->IsRValue(), "Binary expressions must be R-Values!");
  TERRIER_ASSERT(node->GetType()->IsBoolType(), "Boolean binary operation must be of type bool");

  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());

  // Execute left child
  VisitExpressionForRValue(node->Left(), dest);

  Bytecode conditional_jump;
  BytecodeLabel end_label;

  switch (node->Op()) {
    case parsing::Token::Type::OR: {
      conditional_jump = Bytecode::JumpIfTrue;
      break;
    }
    case parsing::Token::Type::AND: {
      conditional_jump = Bytecode::JumpIfFalse;
      break;
    }
    default: {
      UNREACHABLE("Impossible logical operation type");
    }
  }

  // Do a conditional jump
  Emitter()->EmitConditionalJump(conditional_jump, dest.ValueOf(), &end_label);

  // Execute the right child
  VisitExpressionForRValue(node->Right(), dest);

  // Bind the end label
  Emitter()->Bind(&end_label);

  // Mark where the result is
  ExecutionResult()->SetDestination(dest.ValueOf());
}

#define MATH_BYTECODE(code, math_op, arg_type)                                                  \
  if (arg_type->IsIntegerType()) {                                                              \
    code = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::math_op), arg_type);            \
  } else {                                                                                      \
    TERRIER_ASSERT(arg_type->IsFloatType(), "Only integer and floating point math operations"); \
    code = GetFloatTypedBytecode(GET_BASE_FOR_FLOAT_TYPES(Bytecode::math_op), arg_type);        \
  }

void BytecodeGenerator::VisitPrimitiveArithmeticExpr(ast::BinaryOpExpr *node) {
  // TERRIER_ASSERT(ExecutionResult()->IsRValue(), "Arithmetic expressions must be R-Values!");

  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());
  LocalVar left = VisitExpressionForRValue(node->Left());
  LocalVar right = VisitExpressionForRValue(node->Right());

  Bytecode bytecode;
  switch (node->Op()) {
    case parsing::Token::Type::PLUS: {
      MATH_BYTECODE(bytecode, Add, node->GetType());
      break;
    }
    case parsing::Token::Type::MINUS: {
      MATH_BYTECODE(bytecode, Sub, node->GetType());
      break;
    }
    case parsing::Token::Type::STAR: {
      MATH_BYTECODE(bytecode, Mul, node->GetType());
      break;
    }
    case parsing::Token::Type::SLASH: {
      MATH_BYTECODE(bytecode, Div, node->GetType());
      break;
    }
    case parsing::Token::Type::PERCENT: {
      MATH_BYTECODE(bytecode, Rem, node->GetType());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitAnd), node->GetType());
      break;
    }
    case parsing::Token::Type::BIT_OR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitOr), node->GetType());
      break;
    }
    case parsing::Token::Type::BIT_XOR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitXor), node->GetType());
      break;
    }
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  Emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  ExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitSqlArithmeticExpr(ast::BinaryOpExpr *node) {
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());
  LocalVar left = VisitExpressionForLValue(node->Left());
  LocalVar right = VisitExpressionForLValue(node->Right());

  const bool is_integer_math = node->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer);

  Bytecode bytecode;
  switch (node->Op()) {
    case parsing::Token::Type::PLUS: {
      bytecode = (is_integer_math ? Bytecode::AddInteger : Bytecode::AddReal);
      break;
    }
    case parsing::Token::Type::MINUS: {
      bytecode = (is_integer_math ? Bytecode::SubInteger : Bytecode::SubReal);
      break;
    }
    case parsing::Token::Type::STAR: {
      bytecode = (is_integer_math ? Bytecode::MulInteger : Bytecode::MulReal);
      break;
    }
    case parsing::Token::Type::SLASH: {
      bytecode = (is_integer_math ? Bytecode::DivInteger : Bytecode::DivReal);
      break;
    }
    case parsing::Token::Type::PERCENT: {
      bytecode = (is_integer_math ? Bytecode::RemInteger : Bytecode::RemReal);
      break;
    }
    default: {
      UNREACHABLE("Impossible arithmetic SQL operation");
    }
  }

  // Emit
  Emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  ExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitArithmeticExpr(ast::BinaryOpExpr *node) {
  if (node->GetType()->IsSqlValueType()) {
    VisitSqlArithmeticExpr(node);
  } else {
    VisitPrimitiveArithmeticExpr(node);
  }
}

void BytecodeGenerator::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  switch (node->Op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      VisitLogicalAndOrExpr(node);
      break;
    }
    default: {
      VisitArithmeticExpr(node);
      break;
    }
  }
}

#define COMPARISON_BYTECODE(code, comp_type, kind) \
  switch (kind) {                                  \
    case ast::BuiltinType::Kind::Boolean:          \
      code = Bytecode::comp_type##BoolVal;         \
      break;                                       \
    case ast::BuiltinType::Kind::Integer:          \
      code = Bytecode::comp_type##Integer;         \
      break;                                       \
    case ast::BuiltinType::Kind::Real:             \
      code = Bytecode::comp_type##Real;            \
      break;                                       \
    case ast::BuiltinType::Kind::Date:             \
      code = Bytecode::comp_type##DateVal;         \
      break;                                       \
    case ast::BuiltinType::Kind::Timestamp:        \
      code = Bytecode::comp_type##TimestampVal;    \
      break;                                       \
    case ast::BuiltinType::Kind::StringVal:        \
      code = Bytecode::comp_type##StringVal;       \
      break;                                       \
    default:                                       \
      UNREACHABLE("Undefined SQL comparison!");    \
  }

void BytecodeGenerator::VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare) {
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(compare->GetType());
  LocalVar left = VisitExpressionForLValue(compare->Left());
  LocalVar right = VisitExpressionForLValue(compare->Right());

  TERRIER_ASSERT(compare->Left()->GetType() == compare->Right()->GetType(),
                 "Left and right input types to comparison are not equal");
  TERRIER_ASSERT(compare->Left()->GetType()->IsBuiltinType(), "Sql comparison must be done on sql types");

  auto builtin_kind = compare->Left()->GetType()->As<ast::BuiltinType>()->GetKind();

  Bytecode code;
  switch (compare->Op()) {
    case parsing::Token::Type::GREATER: {
      COMPARISON_BYTECODE(code, GreaterThan, builtin_kind);
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      COMPARISON_BYTECODE(code, GreaterThanEqual, builtin_kind);
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      COMPARISON_BYTECODE(code, Equal, builtin_kind);
      break;
    }
    case parsing::Token::Type::LESS: {
      COMPARISON_BYTECODE(code, LessThan, builtin_kind);
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      COMPARISON_BYTECODE(code, LessThanEqual, builtin_kind);
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      COMPARISON_BYTECODE(code, NotEqual, builtin_kind);
      break;
    }
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  Emitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  ExecutionResult()->SetDestination(dest);
}

#undef COMPARISON_BYTECODE

#define COMPARISON_BYTECODE(code, comp_type, arg_type)                                                         \
  if (arg_type->IsIntegerType()) {                                                                             \
    code = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::comp_type), arg_type);                         \
  } else if (arg_type->IsFloatType()) {                                                                        \
    code = GetFloatTypedBytecode(GET_BASE_FOR_FLOAT_TYPES(Bytecode::comp_type), arg_type);                     \
  } else {                                                                                                     \
    TERRIER_ASSERT(arg_type->IsBoolType(), "Only integer, floating point, and boolean comparisons supported"); \
    code = Bytecode::comp_type##_##bool;                                                                       \
  }

void BytecodeGenerator::VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare) {
  LocalVar dest = ExecutionResult()->GetOrCreateDestination(compare->GetType());

  // nil comparison
  if (ast::Expr * input_expr; compare->IsLiteralCompareNil(&input_expr)) {
    LocalVar input = VisitExpressionForRValue(input_expr);
    Bytecode bytecode =
        compare->Op() == parsing::Token::Type ::EQUAL_EQUAL ? Bytecode::IsNullPtr : Bytecode::IsNotNullPtr;
    Emitter()->Emit(bytecode, dest, input);
    ExecutionResult()->SetDestination(dest.ValueOf());
    return;
  }

  // regular comparison

  LocalVar left = VisitExpressionForRValue(compare->Left());
  LocalVar right = VisitExpressionForRValue(compare->Right());

  TERRIER_ASSERT(compare->Left()->GetType() == compare->Right()->GetType(), "Mismatched input types");
  ast::Type *arg_type = compare->Left()->GetType();

  Bytecode bytecode;
  switch (compare->Op()) {
    case parsing::Token::Type::GREATER: {
      COMPARISON_BYTECODE(bytecode, GreaterThan, arg_type);
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      COMPARISON_BYTECODE(bytecode, GreaterThanEqual, arg_type);
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      COMPARISON_BYTECODE(bytecode, Equal, arg_type);
      break;
    }
    case parsing::Token::Type::LESS: {
      COMPARISON_BYTECODE(bytecode, LessThan, arg_type);
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      COMPARISON_BYTECODE(bytecode, LessThanEqual, arg_type);
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      COMPARISON_BYTECODE(bytecode, NotEqual, arg_type);
      break;
    }
    default: {
      UNREACHABLE("Impossible primitive comparison operation");
    }
  }

  // Emit
  Emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  ExecutionResult()->SetDestination(dest.ValueOf());
}

#undef COMPARISON_BYTECODE

void BytecodeGenerator::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  const bool is_primitive_comparison = node->GetType()->IsSpecificBuiltin(ast::BuiltinType::Bool);

  if (!is_primitive_comparison) {
    VisitSqlCompareOpExpr(node);
  } else {
    VisitPrimitiveCompareOpExpr(node);
  }
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) { Visit(node->Body()); }

void BytecodeGenerator::BuildAssign(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate assignment
  const uint32_t size = dest_type->Size();
  if (size == 1) {
    Emitter()->EmitAssign(Bytecode::Assign1, dest, ptr);
  } else if (size == 2) {
    Emitter()->EmitAssign(Bytecode::Assign2, dest, ptr);
  } else if (size == 4) {
    Emitter()->EmitAssign(Bytecode::Assign4, dest, ptr);
  } else {
    Emitter()->EmitAssign(Bytecode::Assign8, dest, ptr);
  }
}

void BytecodeGenerator::BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate deref
  const uint32_t size = dest_type->Size();
  if (size == 1) {
    Emitter()->EmitDeref(Bytecode::Deref1, dest, ptr);
  } else if (size == 2) {
    Emitter()->EmitDeref(Bytecode::Deref2, dest, ptr);
  } else if (size == 4) {
    Emitter()->EmitDeref(Bytecode::Deref4, dest, ptr);
  } else if (size == 8) {
    Emitter()->EmitDeref(Bytecode::Deref8, dest, ptr);
  } else {
    Emitter()->EmitDerefN(dest, ptr, size);
  }
}

LocalVar BytecodeGenerator::BuildLoadPointer(LocalVar double_ptr, ast::Type *type) {
  if (double_ptr.GetAddressMode() == LocalVar::AddressMode::Address) {
    return double_ptr.ValueOf();
  }

  // Need to Deref
  LocalVar ptr = CurrentFunction()->NewLocal(type);
  Emitter()->EmitDeref(Bytecode::Deref8, ptr, double_ptr);
  return ptr.ValueOf();
}

void BytecodeGenerator::VisitMemberExpr(ast::MemberExpr *node) {
  // We first need to compute the address of the object we're selecting into.
  // Thus, we get the L-Value of the object below.

  LocalVar obj_ptr = VisitExpressionForLValue(node->Object());

  // We now need to compute the offset of the field in the composite type. TPL
  // unifies C's arrow and dot syntax for field/member access. Thus, the type
  // of the object may be either a pointer to a struct or the actual struct. If
  // the type is a pointer, then the L-Value of the object is actually a double
  // pointer and we need to dereference it; otherwise, we can use the address
  // as is.

  ast::StructType *obj_type = nullptr;
  if (auto *type = node->Object()->GetType(); node->IsSugaredArrow()) {
    // Double pointer, need to dereference
    obj_ptr = BuildLoadPointer(obj_ptr, type);
    obj_type = type->As<ast::PointerType>()->Base()->As<ast::StructType>();
  } else {
    obj_type = type->As<ast::StructType>();
  }

  // We're now ready to compute offset. Let's lookup the field's offset in the
  // struct type.

  auto *field_name = node->Member()->As<ast::IdentifierExpr>();
  auto offset = obj_type->GetOffsetOfFieldByName(field_name->Name());

  // Now that we have a pointer to the composite object, we need to compute a
  // pointer to the field within the object. If the offset of the field in the
  // object is zero, we needn't do anything - we can just reinterpret the object
  // pointer. If the field offset is greater than zero, we generate a LEA.

  LocalVar field_ptr;
  if (offset == 0) {
    field_ptr = obj_ptr;
  } else {
    field_ptr = CurrentFunction()->NewLocal(node->GetType()->PointerTo());
    Emitter()->EmitLea(field_ptr, obj_ptr, offset);
    field_ptr = field_ptr.ValueOf();
  }

  if (ExecutionResult()->IsLValue()) {
    TERRIER_ASSERT(!ExecutionResult()->HasDestination(), "L-Values produce their destination");
    ExecutionResult()->SetDestination(field_ptr);
    return;
  }

  // The caller wants the actual value of the field. We just computed a pointer
  // to the field in the object, so we need to load/dereference it. If the
  // caller provided a destination variable, use that; otherwise, create a new
  // temporary variable to store the value.

  LocalVar dest = ExecutionResult()->GetOrCreateDestination(node->GetType());
  BuildDeref(dest, field_ptr, node->GetType());
  ExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->Declaration()); }

void BytecodeGenerator::VisitExpressionStmt(ast::ExpressionStmt *node) { Visit(node->Expression()); }

void BytecodeGenerator::VisitBadExpr(ast::BadExpr *node) {
  TERRIER_ASSERT(false, "Visiting bad expression during code generation!");
}

void BytecodeGenerator::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  TERRIER_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  TERRIER_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  TERRIER_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  TERRIER_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  TERRIER_ASSERT(false, "Should not visit type-representation nodes!");
}

FunctionInfo *BytecodeGenerator::AllocateFunc(const std::string &func_name, ast::FunctionType *const func_type) {
  // Allocate function
  const auto func_id = static_cast<FunctionId>(functions_.size());
  functions_.emplace_back(func_id, func_name, func_type);
  FunctionInfo *func = &functions_.back();

  // Register return type
  if (auto *return_type = func_type->ReturnType(); !return_type->IsNilType()) {
    func->NewParameterLocal(return_type->PointerTo(), "hiddenRv");
  }

  // Register parameters
  for (const auto &param : func_type->Params()) {
    func->NewParameterLocal(param.type_, param.name_.Data());
  }

  // Cache
  func_map_[func->Name()] = func->Id();

  return func;
}

FunctionId BytecodeGenerator::LookupFuncIdByName(const std::string &name) const {
  auto iter = func_map_.find(name);
  if (iter == func_map_.end()) {
    return FunctionInfo::K_INVALID_FUNC_ID;
  }
  return iter->second;
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expr *expr) {
  LValueResultScope scope(this);
  Visit(expr);
  return scope.Destination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr) {
  RValueResultScope scope(this);
  Visit(expr);
  return scope.Destination();
}

void BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr, LocalVar dest) {
  RValueResultScope scope(this, dest);
  Visit(expr);
}

void BytecodeGenerator::VisitExpressionForTest(ast::Expr *expr, BytecodeLabel *then_label, BytecodeLabel *else_label,
                                               TestFallthrough fallthrough) {
  // Evaluate the expression
  LocalVar cond = VisitExpressionForRValue(expr);

  switch (fallthrough) {
    case TestFallthrough::Then: {
      Emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      break;
    }
    case TestFallthrough::Else: {
      Emitter()->EmitConditionalJump(Bytecode::JumpIfTrue, cond, then_label);
      break;
    }
    case TestFallthrough::None: {
      Emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      Emitter()->EmitJump(Bytecode::Jump, then_label);
      break;
    }
  }
}

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode, ast::Type *type) {
  TERRIER_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto int_kind = type->SafeAs<ast::BuiltinType>()->GetKind();
  auto kind_idx = static_cast<uint8_t>(int_kind - ast::BuiltinType::Int8);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

Bytecode BytecodeGenerator::GetFloatTypedBytecode(Bytecode bytecode, ast::Type *type) {
  TERRIER_ASSERT(type->IsFloatType(), "Type must be floating-point type");
  auto float_kind = type->SafeAs<ast::BuiltinType>()->GetKind();
  auto kind_idx = static_cast<uint8_t>(float_kind - ast::BuiltinType::Float32);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

// static
std::unique_ptr<BytecodeModule> BytecodeGenerator::Compile(ast::AstNode *root, exec::ExecutionContext *exec_ctx,
                                                           const std::string &name) {
  BytecodeGenerator generator{exec_ctx};
  generator.Visit(root);

  // Create the bytecode module. Note that we move the bytecode and functions
  // array from the generator into the module.
  return std::make_unique<BytecodeModule>(name, std::move(generator.bytecode_), std::move(generator.functions_));
}

}  // namespace terrier::execution::vm
