#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/exec/execution_context.h"
#include "execution/util/macros.h"
#include "execution/vm/bytecode_label.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/control_flow_builders.h"
#include "loggers/execution_logger.h"

namespace tpl::vm {

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
      : generator_(generator), outer_scope_(generator->execution_result()), destination_(destination), kind_(kind) {
    generator_->set_execution_result(this);
  }

  virtual ~ExpressionResultScope() { generator_->set_execution_result(outer_scope_); }

  bool IsLValue() const { return kind_ == ast::Expr::Context::LValue; }
  bool IsRValue() const { return kind_ == ast::Expr::Context::RValue; }

  bool HasDestination() const { return !destination().IsInvalid(); }

  LocalVar GetOrCreateDestination(ast::Type *type) {
    if (!HasDestination()) {
      destination_ = generator_->current_function()->NewLocal(type);
    }

    return destination_;
  }

  LocalVar destination() const { return destination_; }
  void set_destination(LocalVar destination) { destination_ = destination; }

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
      : generator_(generator), func_(func), start_offset_(generator->emitter()->position()) {}

  ~BytecodePositionScope() {
    const std::size_t end_offset = generator_->emitter()->position();
    func_->set_bytecode_range(start_offset_, end_offset);
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
  VisitExpressionForTest(node->condition(), if_builder.then_label(), if_builder.else_label(), TestFallthrough::Then);

  // Generate code in "then" block
  if_builder.Then();
  Visit(node->then_stmt());

  // If there's an "else" block, handle it now
  if (node->else_stmt() != nullptr) {
    if (!ast::Stmt::IsTerminating(node->then_stmt())) {
      if_builder.JumpToEnd();
    }
    if_builder.Else();
    Visit(node->else_stmt());
  }
}

void BytecodeGenerator::VisitIterationStatement(ast::IterationStmt *iteration, LoopBuilder *loop_builder) {
  Visit(iteration->body());
  loop_builder->BindContinueTarget();
}

void BytecodeGenerator::VisitForStmt(ast::ForStmt *node) {
  LoopBuilder loop_builder(this);

  if (node->init() != nullptr) {
    Visit(node->init());
  }

  loop_builder.LoopHeader();

  if (node->condition() != nullptr) {
    BytecodeLabel loop_body_label;
    VisitExpressionForTest(node->condition(), &loop_body_label, loop_builder.break_label(), TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitForInStmt(UNUSED ast::ForInStmt *node) {
  TPL_ASSERT(false, "For-in statements not supported");
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) { AstVisitor::VisitFieldDecl(node); }

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // The function's TPL type
  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();

  // Allocate the function
  FunctionInfo *func_info = AllocateFunc(node->name().data(), func_type);

  {
    // Visit the body of the function. We use this handy scope object to track
    // the start and end position of this function's bytecode in the module's
    // bytecode array. Upon destruction, the scoped class will set the bytecode
    // range in the function.
    BytecodePositionScope position_scope(this, func_info);
    Visit(node->function());
  }
}

void BytecodeGenerator::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Lookup the local in the current function. It must be there through a
  // previous variable declaration (or parameter declaration). What is returned
  // is a pointer to the variable.

  const std::string local_name = node->name().data();
  LocalVar local = current_function()->LookupLocal(local_name);

  if (execution_result()->IsLValue()) {
    execution_result()->set_destination(local);
    return;
  }

  // The caller wants the R-Value of the identifier. So, we need to load it. If
  // the caller did not provide a destination register, we're done. If the
  // caller provided a destination, we need to move the value of the identifier
  // into the provided destination.

  if (!execution_result()->HasDestination()) {
    execution_result()->set_destination(local.ValueOf());
    return;
  }

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());

  // If the local we want the R-Value of is a parameter, we can't take its
  // pointer for the deref, so we use an assignment. Otherwise, a deref is good.
  if (auto *local_info = current_function()->LookupLocalInfoByName(local_name); local_info->is_parameter()) {
    BuildAssign(dest, local.ValueOf(), node->type());
  } else {
    BuildDeref(dest, local, node->type());
  }

  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar input = VisitExpressionForRValue(node->input());

  switch (node->cast_kind()) {
    case ast::CastKind::SqlBoolToBool: {
      emitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      execution_result()->set_destination(dest.ValueOf());
      break;
    }
    case ast::CastKind::IntToSqlInt: {
      emitter()->Emit(Bytecode::InitInteger, dest, input);
      execution_result()->set_destination(dest);
      break;
    }
    case ast::CastKind::BitCast:
    case ast::CastKind::IntegralCast: {
      BuildAssign(dest, input, node->type());
      execution_result()->set_destination(dest.ValueOf());
      break;
    }
    case ast::CastKind::FloatToSqlReal: {
      emitter()->Emit(Bytecode::InitReal, dest, input);
      execution_result()->set_destination(dest);
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
  auto *type = node->object()->type()->As<ast::ArrayType>();
  auto elem_size = type->element_type()->size();

  // First, we need to get the base address of the array
  LocalVar arr;
  if (type->HasKnownLength()) {
    arr = VisitExpressionForLValue(node->object());
  } else {
    arr = VisitExpressionForRValue(node->object());
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

  LocalVar elem_ptr = current_function()->NewLocal(node->type()->PointerTo());

  if (node->index()->IsIntegerLiteral()) {
    const i32 index = static_cast<i32>(node->index()->As<ast::LitExpr>()->int64_val());
    TPL_ASSERT(index >= 0, "Array indexes must be non-negative");
    emitter()->EmitLea(elem_ptr, arr, (elem_size * index));
  } else {
    LocalVar index = VisitExpressionForRValue(node->index());
    emitter()->EmitLeaScaled(elem_ptr, arr, index, elem_size, 0);
  }

  elem_ptr = elem_ptr.ValueOf();

  if (execution_result()->IsLValue()) {
    execution_result()->set_destination(elem_ptr);
    return;
  }

  // The caller wants the value of the array element. We just computed the
  // element's pointer (in element_ptr). Just dereference it into the desired
  // location and be done with it.

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  BuildDeref(dest, elem_ptr, node->type());
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitMapIndexExpr(ast::IndexExpr *node) {}

void BytecodeGenerator::VisitIndexExpr(ast::IndexExpr *node) {
  if (node->object()->type()->IsArrayType()) {
    VisitArrayIndexExpr(node);
  } else {
    VisitMapIndexExpr(node);
  }
}

void BytecodeGenerator::VisitBlockStmt(ast::BlockStmt *node) {
  for (auto *stmt : node->statements()) {
    Visit(stmt);
  }
}

void BytecodeGenerator::VisitVariableDecl(ast::VariableDecl *node) {
  // Register a new local variable in the function. If the variable has an
  // explicit type specifier, prefer using that. Otherwise, use the type of the
  // initial value resolved after semantic analysis.
  ast::Type *type = nullptr;
  if (node->type_repr() != nullptr) {
    TPL_ASSERT(node->type_repr()->type() != nullptr,
               "Variable with explicit type declaration is missing resolved "
               "type at runtime!");
    type = node->type_repr()->type();
  } else {
    TPL_ASSERT(node->initial() != nullptr,
               "Variable without explicit type declaration is missing an "
               "initialization expression!");
    TPL_ASSERT(node->initial()->type() != nullptr, "Variable with initial value is missing resolved type");
    type = node->initial()->type();
  }

  // Register this variable in the function as a local
  LocalVar local = current_function()->NewLocal(type, node->name().data());

  // If there's an initializer, generate code for it now
  if (node->initial() != nullptr) {
    VisitExpressionForRValue(node->initial(), local);
  }
}

void BytecodeGenerator::VisitAddressOfExpr(ast::UnaryOpExpr *op) {
  TPL_ASSERT(execution_result()->IsRValue(), "Address-of expressions must be R-values!");
  LocalVar addr = VisitExpressionForLValue(op->expr());
  if (execution_result()->HasDestination()) {
    LocalVar dest = execution_result()->destination();
    BuildAssign(dest, addr, op->type());
  } else {
    execution_result()->set_destination(addr);
  }
}

void BytecodeGenerator::VisitDerefExpr(ast::UnaryOpExpr *op) {
  LocalVar addr = VisitExpressionForRValue(op->expr());
  if (execution_result()->IsLValue()) {
    execution_result()->set_destination(addr);
  } else {
    LocalVar dest = execution_result()->GetOrCreateDestination(op->type());
    BuildDeref(dest, addr, op->type());
    execution_result()->set_destination(dest.ValueOf());
  }
}

void BytecodeGenerator::VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = execution_result()->GetOrCreateDestination(op->type());
  LocalVar input = VisitExpressionForRValue(op->expr());

  Bytecode bytecode;
  switch (op->op()) {
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Neg), op->type());
      break;
    }
    case parsing::Token::Type::BIT_NOT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitNeg), op->type());
      break;
    }
    default: { UNREACHABLE("Impossible unary operation"); }
  }

  // Emit
  emitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitLogicalNotExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = execution_result()->GetOrCreateDestination(op->type());
  LocalVar input = VisitExpressionForRValue(op->expr());
  emitter()->EmitUnaryOp(Bytecode::Not, dest, input);
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  switch (node->op()) {
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
    default: { UNREACHABLE("Impossible unary operation"); }
  }
}

void BytecodeGenerator::VisitReturnStmt(ast::ReturnStmt *node) {
  if (node->ret() != nullptr) {
    LocalVar rv = current_function()->GetReturnValueLocal();
    LocalVar result = VisitExpressionForRValue(node->ret());
    BuildAssign(rv.ValueOf(), result, node->ret()->type());
  }
  emitter()->EmitReturn();
}

void BytecodeGenerator::VisitSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();
  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitBool, dest, input);
      break;
    }
    case ast::Builtin::IntToSql: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitInteger, dest, input);
      break;
    }
    case ast::Builtin::FloatToSql: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::Builtin::SqlToBool: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      execution_result()->set_destination(dest.ValueOf());
      break;
    }
    case ast::Builtin::StringToSql: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      // Copy data into the execution context's buffer.
      auto input = call->arguments()[0]->As<ast::LitExpr>()->raw_string_val();
      auto input_length = input.length() + 1;
      auto *data = exec_ctx_->GetStringAllocator()->Allocate(input_length);
      std::memcpy(data, input.data(), input_length);
      // Assign the pointer to a local variable
      emitter()->EmitInitString(Bytecode::InitString, dest, input_length, reinterpret_cast<uintptr_t>(data));
      break;
    }
    case ast::Builtin::VarlenToSql: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitVarlen, dest, input);
      break;
    }
    case ast::Builtin::DateToSql: {
      auto dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto year = VisitExpressionForRValue(call->arguments()[0]);
      auto month = VisitExpressionForRValue(call->arguments()[1]);
      auto day = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::InitDate, dest, year, month, day);
      break;
    }
    default: { UNREACHABLE("Impossible SQL conversion call"); }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();

  // The first argument to all calls is a pointer to the TVI
  LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::TableIterConstructBind: {
      // The second argument is the ns name
      ast::Identifier ns_name = call->arguments()[1]->As<ast::LitExpr>()->raw_string_val();
      auto ns_oid = exec_ctx_->GetAccessor()->GetNamespaceOid(ns_name.data());
      TPL_ASSERT(ns_oid != terrier::catalog::INVALID_NAMESPACE_OID, "Namespace does not exists");
      // The third argument is the table name
      ast::Identifier table_name = call->arguments()[2]->As<ast::LitExpr>()->raw_string_val();
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name.data());
      TPL_ASSERT(table_oid != terrier::catalog::INVALID_TABLE_OID, "Table does not exists");
      // The fourth argument should be the execution context
      LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[3]);
      // Emit the initialization codes
      emitter()->EmitTableIterConstruct(Bytecode::TableVectorIteratorConstruct, iter, !table_oid, exec_ctx);
      break;
    }
    case ast::Builtin::TableIterConstruct: {
      // The second argument is the table as a literal string or as an oid integer literal
      auto table_oid = static_cast<u32>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      // The third argument should be the execution context
      LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[2]);
      // Emit the initialization codes
      emitter()->EmitTableIterConstruct(Bytecode::TableVectorIteratorConstruct, iter, table_oid, exec_ctx);
      break;
    }
    case ast::Builtin::TableIterPerformInit: {
      emitter()->Emit(Bytecode::TableVectorIteratorPerformInit, iter);
      break;
    }
    case ast::Builtin::TableIterAddCol: {
      auto col_oid = static_cast<u32>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());;
      emitter()->EmitAddCol(Bytecode::TableVectorIteratorAddCol, iter, col_oid);
      break;
    }
    case ast::Builtin::TableIterAddColBind: {
      std::string ns_name(call->arguments()[1]->As<ast::LitExpr>()->raw_string_val().data());
      auto ns_oid = exec_ctx_->GetAccessor()->GetNamespaceOid(ns_name);
      TPL_ASSERT(ns_oid != terrier::catalog::INVALID_NAMESPACE_OID, "Namespace does not exists");
      std::string table_name(call->arguments()[2]->As<ast::LitExpr>()->raw_string_val().data());
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name);
      TPL_ASSERT(table_oid != terrier::catalog::INVALID_TABLE_OID, "Table does not exists");
      std::string col_name(call->arguments()[3]->As<ast::LitExpr>()->raw_string_val().data());
      auto col_oid = exec_ctx_->GetAccessor()->GetSchema(table_oid).GetColumn(col_name).Oid();
      TPL_ASSERT(col_oid != terrier::catalog::INVALID_COLUMN_OID, "Column does not exists");
      emitter()->EmitAddCol(Bytecode::TableVectorIteratorAddCol, iter, !col_oid);
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      LocalVar cond = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      emitter()->Emit(Bytecode::TableVectorIteratorNext, cond, iter);
      execution_result()->set_destination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableIterGetPCI: {
      ast::Type *pci_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::ProjectedColumnsIterator);
      LocalVar pci = execution_result()->GetOrCreateDestination(pci_type);
      emitter()->Emit(Bytecode::TableVectorIteratorGetPCI, pci, iter);
      execution_result()->set_destination(pci.ValueOf());
      break;
    }
    case ast::Builtin::TableIterClose: {
      emitter()->Emit(Bytecode::TableVectorIteratorFree, iter);
      break;
    }
    default: { UNREACHABLE("Impossible table iteration call"); }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterParallelCall(ast::CallExpr *call) {
  UNREACHABLE("Parallel scan is not implemented yet!");
}

void BytecodeGenerator::VisitBuiltinPCICall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();

  // The first argument to all calls is a pointer to the TVI
  LocalVar pci = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::PCIIsFiltered: {
      LocalVar is_filtered =
          execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      emitter()->Emit(Bytecode::PCIIsFiltered, is_filtered, pci);
      execution_result()->set_destination(is_filtered.ValueOf());
      break;
    }
    case ast::Builtin::PCIHasNext:
    case ast::Builtin::PCIHasNextFiltered: {
      const Bytecode bytecode =
          builtin == ast::Builtin::PCIHasNext ? Bytecode::PCIHasNext : Bytecode::PCIHasNextFiltered;
      LocalVar cond = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      emitter()->Emit(bytecode, cond, pci);
      execution_result()->set_destination(cond.ValueOf());
      break;
    }
    case ast::Builtin::PCIAdvance:
    case ast::Builtin::PCIAdvanceFiltered: {
      const Bytecode bytecode =
          builtin == ast::Builtin::PCIAdvance ? Bytecode::PCIAdvance : Bytecode::PCIAdvanceFiltered;
      emitter()->Emit(bytecode, pci);
      break;
    }
    case ast::Builtin::PCIMatch: {
      LocalVar match = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::PCIMatch, pci, match);
      break;
    }
    case ast::Builtin::PCIReset:
    case ast::Builtin::PCIResetFiltered: {
      const Bytecode bytecode = builtin == ast::Builtin::PCIReset ? Bytecode::PCIReset : Bytecode::PCIResetFiltered;
      emitter()->Emit(bytecode, pci);
      break;
    }
    case ast::Builtin::PCIGetTinyInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetTinyInt, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetTinyIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetTinyIntNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetSmallInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetSmallInt, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetSmallIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetSmallIntNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetInteger, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetIntegerNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetBigInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetBigInt, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetBigIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetBigIntNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetReal: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetReal, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetRealNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetRealNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDouble: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetDouble, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDoubleNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetDoubleNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDate: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetDate, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetDateNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetDateNull, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetVarlen: {
      LocalVar val =
          execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetVarlen, val, pci, col_idx);
      break;
    }
    case ast::Builtin::PCIGetVarlenNull: {
      LocalVar val =
          execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitPCIGet(Bytecode::PCIGetVarlenNull, val, pci, col_idx);
      break;
    }
    default: { UNREACHABLE("Impossible table iteration call"); }
  }
}

void BytecodeGenerator::VisitBuiltinHashCall(ast::CallExpr *call, UNUSED ast::Builtin builtin) {
  TPL_ASSERT(call->type()->size() == sizeof(hash_t),
             "Hash value size (from return type of @hash) doesn't match actual "
             "size of hash_t type");

  // hash_val is where we accumulate all the hash values passed to the @hash()
  LocalVar hash_val = execution_result()->GetOrCreateDestination(call->type());

  // Initialize it to 1
  emitter()->EmitAssignImm8(hash_val, 1);

  // tmp is a temporary variable we use to store individual hash values. We
  // combine all values into hash_val above
  LocalVar tmp = current_function()->NewLocal(call->type());

  for (u32 idx = 0; idx < call->num_args(); idx++) {
    LocalVar input = VisitExpressionForLValue(call->arguments()[idx]);
    TPL_ASSERT(call->arguments()[idx]->type()->IsSqlValueType(), "Input to hash must be a SQL value type");
    auto *type = call->arguments()[idx]->type()->As<ast::BuiltinType>();
    switch (type->kind()) {
      case ast::BuiltinType::Integer: {
        emitter()->Emit(Bytecode::HashInt, tmp, input);
        break;
      }
      case ast::BuiltinType::Real: {
        emitter()->Emit(Bytecode::HashReal, tmp, input);
        break;
      }
      case ast::BuiltinType::StringVal: {
        emitter()->Emit(Bytecode::HashString, tmp, input);
        break;
      }
      default: { UNREACHABLE("Hashing this type isn't supported!"); }
    }
    emitter()->Emit(Bytecode::HashCombine, hash_val, tmp.ValueOf());
  }
  execution_result()->set_destination(hash_val.ValueOf());
}

void BytecodeGenerator::VisitBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar filter_manager = VisitExpressionForRValue(call->arguments()[0]);
  switch (builtin) {
    case ast::Builtin::FilterManagerInit: {
      emitter()->Emit(Bytecode::FilterManagerInit, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      emitter()->Emit(Bytecode::FilterManagerStartNewClause, filter_manager);

      // Insert all flavors
      for (u32 arg_idx = 1; arg_idx < call->num_args(); arg_idx++) {
        const std::string func_name = call->arguments()[arg_idx]->As<ast::IdentifierExpr>()->name().data();
        const FunctionId func_id = LookupFuncIdByName(func_name);
        emitter()->EmitFilterManagerInsertFlavor(filter_manager, func_id);
      }
      break;
    }
    case ast::Builtin::FilterManagerFinalize: {
      emitter()->Emit(Bytecode::FilterManagerFinalize, filter_manager);
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      LocalVar pci = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::FilterManagerRunFilters, filter_manager, pci);
      break;
    }
    case ast::Builtin::FilterManagerFree: {
      emitter()->Emit(Bytecode::FilterManagerFree, filter_manager);
      break;
    }
    default: { UNREACHABLE("Impossible filter manager call"); }
  }
}

void BytecodeGenerator::VisitBuiltinFilterCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar ret_val;
  if (execution_result() != nullptr) {
    ret_val = execution_result()->GetOrCreateDestination(call->type());
    execution_result()->set_destination(ret_val.ValueOf());
  } else {
    ret_val = current_function()->NewLocal(call->type());
  }

  // Collect the three call arguments
  // Projected Column Iterator
  LocalVar pci = VisitExpressionForRValue(call->arguments()[0]);
  // Column index
  u16 col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
  i8 col_type = static_cast<i8>(call->arguments()[2]->As<ast::LitExpr>()->int64_val());
  // Filter value
  i64 val = call->arguments()[3]->As<ast::LitExpr>()->int64_val();

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
    default: { UNREACHABLE("Impossible bytecode"); }
  }
  emitter()->EmitPCIVectorFilter(bytecode, ret_val, pci, col_idx, col_type, val);
}

void BytecodeGenerator::VisitBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableInit: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::AggregationHashTableInit, agg_ht, memory, entry_size);
      break;
    }
    case ast::Builtin::AggHashTableInsert: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::AggregationHashTableInsert, dest, agg_ht, hash);
      break;
    }
    case ast::Builtin::AggHashTableLookup: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      auto key_eq_fn = LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      LocalVar arg = VisitExpressionForRValue(call->arguments()[3]);
      emitter()->EmitAggHashTableLookup(dest, agg_ht, hash, key_eq_fn, arg);
      break;
    }
    case ast::Builtin::AggHashTableProcessBatch: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar iters = VisitExpressionForRValue(call->arguments()[1]);
      auto hash_fn = LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      auto key_eq_fn = LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      auto init_agg_fn = LookupFuncIdByName(call->arguments()[4]->As<ast::IdentifierExpr>()->name().data());
      auto merge_agg_fn = LookupFuncIdByName(call->arguments()[5]->As<ast::IdentifierExpr>()->name().data());
      emitter()->EmitAggHashTableProcessBatch(agg_ht, iters, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn);
      break;
    }
    case ast::Builtin::AggHashTableMovePartitions: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar aht_offset = VisitExpressionForRValue(call->arguments()[2]);
      auto merge_part_fn = LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      emitter()->EmitAggHashTableMovePartitions(agg_ht, tls, aht_offset, merge_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableParallelPartitionedScan: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[2]);
      auto scan_part_fn = LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      emitter()->EmitAggHashTableParallelPartitionedScan(agg_ht, ctx, tls, scan_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableFree: {
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationHashTableFree, agg_ht);
      break;
    }
    default: { UNREACHABLE("Impossible aggregation hash table bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinAggHashTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableIterInit: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar agg_ht = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::AggregationHashTableIteratorInit, agg_ht_iter, agg_ht);
      break;
    }
    case ast::Builtin::AggHashTableIterHasNext: {
      LocalVar has_more = execution_result()->GetOrCreateDestination(call->type());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationHashTableIteratorHasNext, has_more, agg_ht_iter);
      execution_result()->set_destination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterNext: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationHashTableIteratorNext, agg_ht_iter);
      break;
    }
    case ast::Builtin::AggHashTableIterGetRow: {
      LocalVar row_ptr = execution_result()->GetOrCreateDestination(call->type());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationHashTableIteratorGetRow, row_ptr, agg_ht_iter);
      execution_result()->set_destination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterClose: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationHashTableIteratorFree, agg_ht_iter);
      break;
    }
    default: { UNREACHABLE("Impossible aggregation hash table iteration bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinAggPartIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggPartIterHasNext: {
      LocalVar has_more = execution_result()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorHasNext, has_more, iter);
      execution_result()->set_destination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterNext: {
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorNext, iter);
      break;
    }
    case ast::Builtin::AggPartIterGetRow: {
      LocalVar row = execution_result()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRow, row, iter);
      execution_result()->set_destination(row.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      LocalVar hash = execution_result()->GetOrCreateDestination(call->type());
      LocalVar iter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetHash, hash, iter);
      execution_result()->set_destination(hash.ValueOf());
      break;
    }
    default: { UNREACHABLE("Impossible aggregation partition iterator bytecode"); }
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

enum class AggOpKind : u8 { Init = 0, Advance = 1, GetResult = 2, Merge = 3, Reset = 4 };

// Given an aggregate kind and the operation to perform on it, determine the
// appropriate bytecode
template <AggOpKind OpKind>
Bytecode OpForAgg(ast::BuiltinType::Kind agg_kind);

template <>
Bytecode OpForAgg<AggOpKind::Init>(const ast::BuiltinType::Kind agg_kind) {
  switch (agg_kind) {
    default: { UNREACHABLE("Impossible aggregate type"); }
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
    default: { UNREACHABLE("Impossible aggregate type"); }
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
    default: { UNREACHABLE("Impossible aggregate type"); }
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
    default: { UNREACHABLE("Impossible aggregate type"); }
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
    default: { UNREACHABLE("Impossible aggregate type"); }
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset) \
  case ast::BuiltinType::Type:                              \
    return Bytecode::Reset;
      AGG_CODES(ENTRY)
#undef ENTRY
  }
}

}  // namespace

void BytecodeGenerator::VisitBuiltinAggregatorCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggInit:
    case ast::Builtin::AggReset: {
      for (const auto &arg : call->arguments()) {
        const auto agg_kind = arg->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
        LocalVar input = VisitExpressionForRValue(arg);
        Bytecode bytecode;
        if (builtin == ast::Builtin::AggInit) {
          bytecode = OpForAgg<AggOpKind::Init>(agg_kind);
        } else {
          bytecode = OpForAgg<AggOpKind::Reset>(agg_kind);
        }
        emitter()->Emit(bytecode, input);
      }
      break;
    }
    case ast::Builtin::AggAdvance: {
      const auto &args = call->arguments();
      const auto agg_kind = args[0]->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
      LocalVar agg = VisitExpressionForRValue(args[0]);
      LocalVar input = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Advance>(agg_kind);
      emitter()->Emit(bytecode, agg, input);
      break;
    }
    case ast::Builtin::AggMerge: {
      const auto &args = call->arguments();
      const auto agg_kind = args[0]->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
      LocalVar agg_1 = VisitExpressionForRValue(args[0]);
      LocalVar agg_2 = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Merge>(agg_kind);
      emitter()->Emit(bytecode, agg_1, agg_2);
      break;
    }
    case ast::Builtin::AggResult: {
      const auto &args = call->arguments();
      const auto agg_kind = args[0]->type()->GetPointeeType()->As<ast::BuiltinType>()->kind();
      LocalVar result = execution_result()->GetOrCreateDestination(call->type());
      LocalVar agg = VisitExpressionForRValue(args[0]);
      Bytecode bytecode = OpForAgg<AggOpKind::GetResult>(agg_kind);
      emitter()->Emit(bytecode, result, agg);
      break;
    }
    default: { UNREACHABLE("Impossible aggregator call"); }
  }
}

void BytecodeGenerator::VisitBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::JoinHashTableInit: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::JoinHashTableInit, join_hash_table, memory, entry_size);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::JoinHashTableAllocTuple, dest, join_hash_table, hash);
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::JoinHashTableBuild, join_hash_table);
      break;
    }
    case ast::Builtin::JoinHashTableIterInit: {
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::JoinHashTableIterInit, iterator, join_hash_table, hash);
      break;
    }
    case ast::Builtin::JoinHashTableIterHasNext: {
      LocalVar has_more = execution_result()->GetOrCreateDestination(call->type());
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      const std::string key_eq_name = call->arguments()[1]->As<ast::IdentifierExpr>()->name().data();
      LocalVar opaque_ctx = VisitExpressionForRValue(call->arguments()[2]);
      LocalVar probe_tuple = VisitExpressionForRValue(call->arguments()[3]);
      emitter()->EmitJoinHashTableIterHasNext(has_more, iterator, LookupFuncIdByName(key_eq_name), opaque_ctx,
                                              probe_tuple);
      execution_result()->set_destination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableIterGetRow: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::JoinHashTableIterGetRow, dest, iterator);
      break;
    }
    case ast::Builtin::JoinHashTableIterClose: {
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::JoinHashTableIterClose, iterator);
      break;
    }
    case ast::Builtin::JoinHashTableBuildParallel: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar jht_offset = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::JoinHashTableBuildParallel, join_hash_table, tls, jht_offset);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::JoinHashTableFree, join_hash_table);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinSorterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::SorterInit: {
      // TODO(pmenon): Fix me so that the comparison function doesn't have be
      // listed by name.
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      const std::string cmp_func_name = call->arguments()[2]->As<ast::IdentifierExpr>()->name().data();
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[3]);
      emitter()->EmitSorterInit(Bytecode::SorterInit, sorter, memory, LookupFuncIdByName(cmp_func_name), entry_size);
      break;
    }
    case ast::Builtin::SorterInsert: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::SorterAllocTuple, dest, sorter);
      break;
    }
    case ast::Builtin::SorterSort: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::SorterSort, sorter);
      break;
    }
    case ast::Builtin::SorterSortParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::SorterSortParallel, sorter, tls, sorter_offset);
      break;
    }
    case ast::Builtin::SorterSortTopKParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->arguments()[2]);
      LocalVar top_k = VisitExpressionForRValue(call->arguments()[3]);
      emitter()->Emit(Bytecode::SorterSortTopKParallel, sorter, tls, sorter_offset, top_k);
      break;
    }
    case ast::Builtin::SorterFree: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::SorterFree, sorter);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();

  // The first argument to all calls is the sorter iterator instance
  const LocalVar sorter_iter = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::SorterIteratorInit, sorter_iter, sorter);
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      LocalVar cond = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      emitter()->Emit(Bytecode::SorterIteratorHasNext, cond, sorter_iter);
      execution_result()->set_destination(cond.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterNext: {
      emitter()->Emit(Bytecode::SorterIteratorNext, sorter_iter);
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      LocalVar row_ptr =
          execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Uint8)->PointerTo());
      emitter()->Emit(Bytecode::SorterIteratorGetRow, row_ptr, sorter_iter);
      execution_result()->set_destination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      emitter()->Emit(Bytecode::SorterIteratorFree, sorter_iter);
      break;
    }
    default: { UNREACHABLE("Impossible table iteration call"); }
  }
}

void BytecodeGenerator::VisitExecutionContextCall(ast::CallExpr *call, UNUSED ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();

  // The memory pool pointer
  LocalVar mem_pool =
      execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::MemoryPool)->PointerTo());

  // The execution context pointer
  LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[0]);

  // Emit bytecode
  emitter()->Emit(Bytecode::ExecutionContextGetMemoryPool, mem_pool, exec_ctx);

  // Indicate where the result is
  execution_result()->set_destination(mem_pool.ValueOf());
}

void BytecodeGenerator::VisitBuiltinThreadStateContainerCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar tls = VisitExpressionForRValue(call->arguments()[0]);
  switch (builtin) {
    case ast::Builtin::ThreadStateContainerInit: {
      LocalVar memory = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::ThreadStateContainerInit, tls, memory);
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[1]);
      FunctionId iterate_fn = LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      emitter()->EmitThreadStateContainerIterate(tls, ctx, iterate_fn);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[1]);
      FunctionId init_fn = LookupFuncIdByName(call->arguments()[2]->As<ast::IdentifierExpr>()->name().data());
      FunctionId destroy_fn = LookupFuncIdByName(call->arguments()[3]->As<ast::IdentifierExpr>()->name().data());
      LocalVar ctx = VisitExpressionForRValue(call->arguments()[4]);
      emitter()->EmitThreadStateContainerReset(tls, entry_size, init_fn, destroy_fn, ctx);
      break;
    }
    case ast::Builtin::ThreadStateContainerFree: {
      emitter()->Emit(Bytecode::ThreadStateContainerFree, tls);
      break;
    }
    default: { UNREACHABLE("Impossible thread state container call"); }
  }
}

void BytecodeGenerator::VisitBuiltinTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();
  LocalVar dest = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
  LocalVar src = VisitExpressionForRValue(call->arguments()[0]);

  switch (builtin) {
    case ast::Builtin::ACos: {
      emitter()->Emit(Bytecode::Acos, dest, src);
      break;
    }
    case ast::Builtin::ASin: {
      emitter()->Emit(Bytecode::Asin, dest, src);
      break;
    }
    case ast::Builtin::ATan: {
      emitter()->Emit(Bytecode::Atan, dest, src);
      break;
    }
    case ast::Builtin::ATan2: {
      emitter()->Emit(Bytecode::Atan2, dest, src);
      break;
    }
    case ast::Builtin::Cos: {
      emitter()->Emit(Bytecode::Cos, dest, src);
      break;
    }
    case ast::Builtin::Cot: {
      emitter()->Emit(Bytecode::Cot, dest, src);
      break;
    }
    case ast::Builtin::Sin: {
      emitter()->Emit(Bytecode::Sin, dest, src);
      break;
    }
    case ast::Builtin::Tan: {
      emitter()->Emit(Bytecode::Tan, dest, src);
    }
    default: { UNREACHABLE("Impossible trigonometric bytecode"); }
  }

  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitBuiltinSizeOfCall(ast::CallExpr *call) {
  ast::Type *target_type = call->arguments()[0]->type();
  LocalVar size_var = execution_result()->GetOrCreateDestination(
      ast::BuiltinType::Get(target_type->context(), ast::BuiltinType::Uint32));
  emitter()->EmitAssignImm4(size_var, target_type->size());
  execution_result()->set_destination(size_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinOutputCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[0]);
  switch (builtin) {
    case ast::Builtin::OutputAlloc: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      emitter()->EmitOutputAlloc(Bytecode::OutputAlloc, exec_ctx, dest);
      break;
    }
    case ast::Builtin::OutputAdvance: {
      emitter()->EmitOutputCall(Bytecode::OutputAdvance, exec_ctx);
      break;
    }
    case ast::Builtin::OutputSetNull: {
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->EmitOutputSetNull(Bytecode::OutputSetNull, exec_ctx, entry_size);
      break;
    }
    case ast::Builtin::OutputFinalize: {
      emitter()->EmitOutputCall(Bytecode::OutputFinalize, exec_ctx);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinInsertCall(ast::CallExpr *call, ast::Builtin builtin) {
  UNREACHABLE("Implement Insert after adding a ProjectedRow type");
}

void BytecodeGenerator::VisitBuiltinIndexIteratorCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
  ast::Context *ctx = call->type()->context();

  switch (builtin) {
    case ast::Builtin::IndexIteratorConstructBind: {
      std::string ns_name(call->arguments()[1]->As<ast::LitExpr>()->raw_string_val().data());
      auto ns_oid = exec_ctx_->GetAccessor()->GetNamespaceOid(ns_name);
      TPL_ASSERT(ns_oid != terrier::catalog::INVALID_NAMESPACE_OID, "Namespace does not exists");
      std::string table_name(call->arguments()[2]->As<ast::LitExpr>()->raw_string_val().data());
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name);
      TPL_ASSERT(table_oid != terrier::catalog::INVALID_TABLE_OID, "Table does not exists");
      std::string index_name(call->arguments()[3]->As<ast::LitExpr>()->raw_string_val().data());
      auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(ns_oid, index_name);
      TPL_ASSERT(index_oid != terrier::catalog::INVALID_INDEX_OID, "Index does not exists");
      LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[4]);
      emitter()->EmitIndexIteratorConstruct(Bytecode::IndexIteratorConstruct, iterator, !table_oid, !index_oid, exec_ctx);
      break;
    }
    case ast::Builtin::IndexIteratorConstruct: {
      auto table_oid = static_cast<u32>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());;
      auto index_oid = static_cast<u32>(call->arguments()[2]->As<ast::LitExpr>()->int64_val());
      LocalVar exec_ctx = VisitExpressionForRValue(call->arguments()[3]);
      emitter()->EmitIndexIteratorConstruct(Bytecode::IndexIteratorConstruct, iterator, table_oid, index_oid, exec_ctx);
      break;
    }
    case ast::Builtin::IndexIteratorPerformInit: {
      emitter()->Emit(Bytecode::IndexIteratorPerformInit, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorAddCol: {
      auto col_oid = static_cast<u32>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());;
      emitter()->EmitAddCol(Bytecode::IndexIteratorAddCol, iterator, col_oid);
      break;
    }
    case ast::Builtin::IndexIteratorAddColBind: {
      std::string ns_name(call->arguments()[1]->As<ast::LitExpr>()->raw_string_val().data());
      auto ns_oid = exec_ctx_->GetAccessor()->GetNamespaceOid(ns_name);
      TPL_ASSERT(ns_oid != terrier::catalog::INVALID_NAMESPACE_OID, "Namespace does not exists");
      std::string table_name(call->arguments()[2]->As<ast::LitExpr>()->raw_string_val().data());
      auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(ns_oid, table_name);
      TPL_ASSERT(table_oid != terrier::catalog::INVALID_TABLE_OID, "Table does not exists");
      std::string col_name(call->arguments()[3]->As<ast::LitExpr>()->raw_string_val().data());
      auto col_oid = exec_ctx_->GetAccessor()->GetSchema(table_oid).GetColumn(col_name).Oid();
      TPL_ASSERT(col_oid != terrier::catalog::INVALID_COLUMN_OID, "Column does not exists");
      emitter()->EmitAddCol(Bytecode::IndexIteratorAddCol, iterator, !col_oid);
      break;
    }
    case ast::Builtin::IndexIteratorScanKey: {
      emitter()->Emit(Bytecode::IndexIteratorScanKey, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorAdvance: {
      LocalVar cond = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      emitter()->Emit(Bytecode::IndexIteratorAdvance, cond, iterator);
      execution_result()->set_destination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      emitter()->Emit(Bytecode::IndexIteratorFree, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetTinyInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetTinyInt, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetSmallInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetSmallInt, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetInteger, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetBigInt: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetBigInt, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetReal: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetReal, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetDouble: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetDouble, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetTinyIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetTinyIntNull, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetSmallIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetSmallIntNull, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetIntegerNull, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetBigIntNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetBigIntNull, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetRealNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetRealNull, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorGetDoubleNull: {
      LocalVar val = execution_result()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      emitter()->EmitIndexIteratorGet(Bytecode::IndexIteratorGetDoubleNull, val, iterator, col_idx);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeyTinyInt: {
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      LocalVar val = VisitExpressionForLValue(call->arguments()[2]);
      emitter()->EmitIndexIteratorSetKey(Bytecode::IndexIteratorSetKeyTinyInt, iterator, col_idx, val);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeySmallInt: {
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      LocalVar val = VisitExpressionForLValue(call->arguments()[2]);
      emitter()->EmitIndexIteratorSetKey(Bytecode::IndexIteratorSetKeySmallInt, iterator, col_idx, val);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeyInt: {
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      LocalVar val = VisitExpressionForLValue(call->arguments()[2]);
      emitter()->EmitIndexIteratorSetKey(Bytecode::IndexIteratorSetKeyInt, iterator, col_idx, val);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeyBigInt: {
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      LocalVar val = VisitExpressionForLValue(call->arguments()[2]);
      emitter()->EmitIndexIteratorSetKey(Bytecode::IndexIteratorSetKeyBigInt, iterator, col_idx, val);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeyReal: {
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      LocalVar val = VisitExpressionForLValue(call->arguments()[2]);
      emitter()->EmitIndexIteratorSetKey(Bytecode::IndexIteratorSetKeyReal, iterator, col_idx, val);
      break;
    }
    case ast::Builtin::IndexIteratorSetKeyDouble: {
      auto col_idx = static_cast<u16>(call->arguments()[1]->As<ast::LitExpr>()->int64_val());
      LocalVar val = VisitExpressionForLValue(call->arguments()[2]);
      emitter()->EmitIndexIteratorSetKey(Bytecode::IndexIteratorSetKeyDouble, iterator, col_idx, val);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinCallExpr(ast::CallExpr *call) {
  ast::Builtin builtin;

  ast::Context *ctx = call->type()->context();
  ctx->IsBuiltinFunction(call->GetFuncName(), &builtin);

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::VarlenToSql:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool: {
      VisitSqlConversionCall(call, builtin);
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
    case ast::Builtin::TableIterConstruct:
    case ast::Builtin::TableIterConstructBind:
    case ast::Builtin::TableIterPerformInit:
    case ast::Builtin::TableIterAddCol:
    case ast::Builtin::TableIterAddColBind:
    case ast::Builtin::TableIterAdvance:
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
      Visit(call->arguments()[1]);
      break;
    }
    case ast::Builtin::OutputAlloc:
    case ast::Builtin::OutputAdvance:
    case ast::Builtin::OutputFinalize:
    case ast::Builtin::OutputSetNull:
      VisitBuiltinOutputCall(call, builtin);
      break;
    case ast::Builtin::Insert:
      VisitBuiltinInsertCall(call, builtin);
      break;
    case ast::Builtin::IndexIteratorConstruct:
    case ast::Builtin::IndexIteratorConstructBind:
    case ast::Builtin::IndexIteratorAddCol:
    case ast::Builtin::IndexIteratorAddColBind:
    case ast::Builtin::IndexIteratorPerformInit:
    case ast::Builtin::IndexIteratorScanKey:
    case ast::Builtin::IndexIteratorAdvance:
    case ast::Builtin::IndexIteratorGetTinyInt:
    case ast::Builtin::IndexIteratorGetSmallInt:
    case ast::Builtin::IndexIteratorGetInt:
    case ast::Builtin::IndexIteratorGetBigInt:
    case ast::Builtin::IndexIteratorGetReal:
    case ast::Builtin::IndexIteratorGetDouble:
    case ast::Builtin::IndexIteratorGetTinyIntNull:
    case ast::Builtin::IndexIteratorGetSmallIntNull:
    case ast::Builtin::IndexIteratorGetIntNull:
    case ast::Builtin::IndexIteratorGetBigIntNull:
    case ast::Builtin::IndexIteratorGetRealNull:
    case ast::Builtin::IndexIteratorGetDoubleNull:
    case ast::Builtin::IndexIteratorFree:
    case ast::Builtin::IndexIteratorSetKeyTinyInt:
    case ast::Builtin::IndexIteratorSetKeySmallInt:
    case ast::Builtin::IndexIteratorSetKeyInt:
    case ast::Builtin::IndexIteratorSetKeyBigInt:
      VisitBuiltinIndexIteratorCall(call, builtin);
      break;
    default: { UNREACHABLE("Builtin not supported!"); }
  }
}

void BytecodeGenerator::VisitRegularCallExpr(ast::CallExpr *call) {
  bool caller_wants_result = execution_result() != nullptr;
  TPL_ASSERT(!caller_wants_result || execution_result()->IsRValue(), "Calls can only be R-Values!");

  std::vector<LocalVar> params;

  auto *func_type = call->function()->type()->As<ast::FunctionType>();

  if (!func_type->return_type()->IsNilType()) {
    LocalVar ret_val;
    if (caller_wants_result) {
      ret_val = execution_result()->GetOrCreateDestination(func_type->return_type());

      // Let the caller know where the result value is
      execution_result()->set_destination(ret_val.ValueOf());
    } else {
      ret_val = current_function()->NewLocal(func_type->return_type());
    }

    // Push return value address into parameter list
    params.push_back(ret_val);
  }

  // Collect non-return-value parameters as usual
  for (u32 i = 0; i < func_type->num_params(); i++) {
    params.push_back(VisitExpressionForRValue(call->arguments()[i]));
  }

  // Emit call
  const auto func_id = LookupFuncIdByName(call->GetFuncName().data());
  TPL_ASSERT(func_id != FunctionInfo::kInvalidFuncId, "Function not found!");
  emitter()->EmitCall(func_id, params);
}

void BytecodeGenerator::VisitCallExpr(ast::CallExpr *node) {
  ast::CallExpr::CallKind call_kind = node->call_kind();

  if (call_kind == ast::CallExpr::CallKind::Builtin) {
    VisitBuiltinCallExpr(node);
  } else {
    VisitRegularCallExpr(node);
  }
}

void BytecodeGenerator::VisitAssignmentStmt(ast::AssignmentStmt *node) {
  LocalVar dest = VisitExpressionForLValue(node->destination());
  VisitExpressionForRValue(node->source(), dest);
}

void BytecodeGenerator::VisitFile(ast::File *node) {
  for (auto *decl : node->declarations()) {
    Visit(decl);
  }
}

void BytecodeGenerator::VisitLitExpr(ast::LitExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(), "Literal expressions cannot be R-Values!");

  LocalVar target = execution_result()->GetOrCreateDestination(node->type());
  std::cout << "LITERAL KIND: " << i32(node->literal_kind()) << std::endl;
  switch (node->literal_kind()) {
    case ast::LitExpr::LitKind::Nil: {
      // Do nothing
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      emitter()->EmitAssignImm1(target, static_cast<i8>(node->bool_val()));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      emitter()->EmitAssignImm8(target, node->int64_val());
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      emitter()->EmitAssignImm8F(target, node->float64_val());
      break;
    }
    default: {
      EXECUTION_LOG_ERROR("Non-bool or non-integer literals not supported in bytecode");
      break;
    }
  }

  execution_result()->set_destination(target.ValueOf());
}

void BytecodeGenerator::VisitStructDecl(UNUSED ast::StructDecl *node) {
  // Nothing to do
}

void BytecodeGenerator::VisitLogicalAndOrExpr(ast::BinaryOpExpr *node) {
  // TPL_ASSERT(execution_result()->IsRValue(), "Binary expressions must be R-Values!");
  TPL_ASSERT(node->type()->IsBoolType(), "Boolean binary operation must be of type bool");

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());

  // Execute left child
  VisitExpressionForRValue(node->left(), dest);

  Bytecode conditional_jump;
  BytecodeLabel end_label;

  switch (node->op()) {
    case parsing::Token::Type::OR: {
      conditional_jump = Bytecode::JumpIfTrue;
      break;
    }
    case parsing::Token::Type::AND: {
      conditional_jump = Bytecode::JumpIfFalse;
      break;
    }
    default: { UNREACHABLE("Impossible logical operation type"); }
  }

  // Do a conditional jump
  emitter()->EmitConditionalJump(conditional_jump, dest.ValueOf(), &end_label);

  // Execute the right child
  VisitExpressionForRValue(node->right(), dest);

  // Bind the end label
  emitter()->Bind(&end_label);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitPrimitiveArithmeticExpr(ast::BinaryOpExpr *node) {
  // TPL_ASSERT(execution_result()->IsRValue(), "Arithmetic expressions must be R-Values!");

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForRValue(node->left());
  LocalVar right = VisitExpressionForRValue(node->right());

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::PLUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Add), node->type());
      break;
    }
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Sub), node->type());
      break;
    }
    case parsing::Token::Type::STAR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Mul), node->type());
      break;
    }
    case parsing::Token::Type::SLASH: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Div), node->type());
      break;
    }
    case parsing::Token::Type::PERCENT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Rem), node->type());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitAnd), node->type());
      break;
    }
    case parsing::Token::Type::BIT_OR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitOr), node->type());
      break;
    }
    case parsing::Token::Type::BIT_XOR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitXor), node->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitSqlArithmeticExpr(ast::BinaryOpExpr *node) {
  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForLValue(node->left());
  LocalVar right = VisitExpressionForLValue(node->right());

  const bool is_integer_math = node->type()->IsSpecificBuiltin(ast::BuiltinType::Integer);

  Bytecode bytecode;
  switch (node->op()) {
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
    default: { UNREACHABLE("Impossible arithmetic SQL operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitArithmeticExpr(ast::BinaryOpExpr *node) {
  if (node->type()->IsSqlValueType()) {
    VisitSqlArithmeticExpr(node);
  } else {
    VisitPrimitiveArithmeticExpr(node);
  }
}

void BytecodeGenerator::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  switch (node->op()) {
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
    case ast::BuiltinType::Kind::Integer:          \
      code = Bytecode::comp_type##Integer;         \
      break;                                       \
    case ast::BuiltinType::Kind::Real:             \
      code = Bytecode::comp_type##Real;            \
      break;                                       \
    case ast::BuiltinType::Kind::Date:             \
      code = Bytecode::comp_type##Date;            \
      break;                                       \
    case ast::BuiltinType::Kind::StringVal:        \
      code = Bytecode::comp_type##StringVal;       \
      break;                                       \
    default:                                       \
      UNREACHABLE("Undefined sql comparison!");    \
  }

void BytecodeGenerator::VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare) {
  LocalVar dest = execution_result()->GetOrCreateDestination(compare->type());
  LocalVar left = VisitExpressionForLValue(compare->left());
  LocalVar right = VisitExpressionForLValue(compare->right());

  TPL_ASSERT(compare->left()->type() == compare->right()->type(),
             "Left and right input types to comparison are not equal");
  TPL_ASSERT(compare->left()->type()->IsBuiltinType(), "Sql comparison must be done on sql types");

  auto builtin_kind = compare->left()->type()->As<ast::BuiltinType>()->kind();

  Bytecode code;
  switch (compare->op()) {
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
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare) {
  LocalVar dest = execution_result()->GetOrCreateDestination(compare->type());

  // nil comparison
  if (ast::Expr * input_expr; compare->IsLiteralCompareNil(&input_expr)) {
    LocalVar input = VisitExpressionForRValue(input_expr);
    Bytecode bytecode =
        compare->op() == parsing::Token::Type ::EQUAL_EQUAL ? Bytecode::IsNullPtr : Bytecode::IsNotNullPtr;
    emitter()->Emit(bytecode, dest, input);
    execution_result()->set_destination(dest.ValueOf());
    return;
  }

  // regular comparison

  LocalVar left = VisitExpressionForRValue(compare->left());
  LocalVar right = VisitExpressionForRValue(compare->right());

  Bytecode bytecode;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThan), compare->left()->type());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThanEqual), compare->left()->type());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Equal), compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThan), compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThanEqual), compare->left()->type());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::NotEqual), compare->left()->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  const bool is_primitive_comparison = node->type()->IsSpecificBuiltin(ast::BuiltinType::Bool);

  if (!is_primitive_comparison) {
    VisitSqlCompareOpExpr(node);
  } else {
    VisitPrimitiveCompareOpExpr(node);
  }
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) { Visit(node->body()); }

void BytecodeGenerator::BuildAssign(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate assignment
  const u32 size = dest_type->size();
  if (size == 1) {
    emitter()->EmitAssign(Bytecode::Assign1, dest, ptr);
  } else if (size == 2) {
    emitter()->EmitAssign(Bytecode::Assign2, dest, ptr);
  } else if (size == 4) {
    emitter()->EmitAssign(Bytecode::Assign4, dest, ptr);
  } else {
    emitter()->EmitAssign(Bytecode::Assign8, dest, ptr);
  }
}

void BytecodeGenerator::BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate deref
  const u32 size = dest_type->size();
  if (size == 1) {
    emitter()->EmitDeref(Bytecode::Deref1, dest, ptr);
  } else if (size == 2) {
    emitter()->EmitDeref(Bytecode::Deref2, dest, ptr);
  } else if (size == 4) {
    emitter()->EmitDeref(Bytecode::Deref4, dest, ptr);
  } else if (size == 8) {
    emitter()->EmitDeref(Bytecode::Deref8, dest, ptr);
  } else {
    emitter()->EmitDerefN(dest, ptr, size);
  }
}

LocalVar BytecodeGenerator::BuildLoadPointer(LocalVar double_ptr, ast::Type *type) {
  if (double_ptr.GetAddressMode() == LocalVar::AddressMode::Address) {
    return double_ptr.ValueOf();
  }

  // Need to Deref
  LocalVar ptr = current_function()->NewLocal(type);
  emitter()->EmitDeref(Bytecode::Deref8, ptr, double_ptr);
  return ptr.ValueOf();
}

void BytecodeGenerator::VisitMemberExpr(ast::MemberExpr *node) {
  // We first need to compute the address of the object we're selecting into.
  // Thus, we get the L-Value of the object below.

  LocalVar obj_ptr = VisitExpressionForLValue(node->object());

  // We now need to compute the offset of the field in the composite type. TPL
  // unifies C's arrow and dot syntax for field/member access. Thus, the type
  // of the object may be either a pointer to a struct or the actual struct. If
  // the type is a pointer, then the L-Value of the object is actually a double
  // pointer and we need to dereference it; otherwise, we can use the address
  // as is.

  ast::StructType *obj_type = nullptr;
  if (auto *type = node->object()->type(); node->IsSugaredArrow()) {
    // Double pointer, need to dereference
    obj_ptr = BuildLoadPointer(obj_ptr, type);
    obj_type = type->As<ast::PointerType>()->base()->As<ast::StructType>();
  } else {
    obj_type = type->As<ast::StructType>();
  }

  // We're now ready to compute offset. Let's lookup the field's offset in the
  // struct type.

  auto *field_name = node->member()->As<ast::IdentifierExpr>();
  auto offset = obj_type->GetOffsetOfFieldByName(field_name->name());

  // Now that we have a pointer to the composite object, we need to compute a
  // pointer to the field within the object. If the offset of the field in the
  // object is zero, we needn't do anything - we can just reinterpret the object
  // pointer. If the field offset is greater than zero, we generate a LEA.

  LocalVar field_ptr;
  if (offset == 0) {
    field_ptr = obj_ptr;
  } else {
    field_ptr = current_function()->NewLocal(node->type()->PointerTo());
    emitter()->EmitLea(field_ptr, obj_ptr, offset);
    field_ptr = field_ptr.ValueOf();
  }

  if (execution_result()->IsLValue()) {
    TPL_ASSERT(!execution_result()->HasDestination(), "L-Values produce their destination");
    execution_result()->set_destination(field_ptr);
    return;
  }

  // The caller wants the actual value of the field. We just computed a pointer
  // to the field in the object, so we need to load/dereference it. If the
  // caller provided a destination variable, use that; otherwise, create a new
  // temporary variable to store the value.

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  BuildDeref(dest, field_ptr, node->type());
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->declaration()); }

void BytecodeGenerator::VisitExpressionStmt(ast::ExpressionStmt *node) { Visit(node->expression()); }

void BytecodeGenerator::VisitBadExpr(ast::BadExpr *node) {
  TPL_ASSERT(false, "Visiting bad expression during code generation!");
}

void BytecodeGenerator::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  TPL_ASSERT(false, "Should not visit type-representation nodes!");
}

FunctionInfo *BytecodeGenerator::AllocateFunc(const std::string &func_name, ast::FunctionType *const func_type) {
  // Allocate function
  const auto func_id = static_cast<FunctionId>(functions_.size());
  functions_.emplace_back(func_id, func_name, func_type);
  FunctionInfo *func = &functions_.back();

  // Register return type
  if (auto *return_type = func_type->return_type(); !return_type->IsNilType()) {
    func->NewParameterLocal(return_type->PointerTo(), "hiddenRv");
  }

  // Register parameters
  for (const auto &param : func_type->params()) {
    func->NewParameterLocal(param.type, param.name.data());
  }

  // Cache
  func_map_[func->name()] = func->id();

  return func;
}

FunctionId BytecodeGenerator::LookupFuncIdByName(const std::string &name) const {
  auto iter = func_map_.find(name);
  if (iter == func_map_.end()) {
    return FunctionInfo::kInvalidFuncId;
  }
  return iter->second;
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expr *expr) {
  LValueResultScope scope(this);
  Visit(expr);
  return scope.destination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr) {
  RValueResultScope scope(this);
  Visit(expr);
  return scope.destination();
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
      emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      break;
    }
    case TestFallthrough::Else: {
      emitter()->EmitConditionalJump(Bytecode::JumpIfTrue, cond, then_label);
      break;
    }
    case TestFallthrough::None: {
      emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      emitter()->EmitJump(Bytecode::Jump, then_label);
      break;
    }
  }
}

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode, ast::Type *type) {
  TPL_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto int_kind = type->SafeAs<ast::BuiltinType>()->kind();
  auto kind_idx = static_cast<u8>(int_kind - ast::BuiltinType::Int8);
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

}  // namespace tpl::vm
