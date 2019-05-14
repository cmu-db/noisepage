#include "execution/vm/bytecode_generator.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sql/execution_structures.h"
#include "execution/util/macros.h"
#include "execution/vm/bytecode_label.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/control_flow_builders.h"

#include "loggers/execution_logger.h"

namespace tpl::vm {

// ---------------------------------------------------------
// Expression Result Scope
// ---------------------------------------------------------

/// ExpressionResultScope is an RAII class that provides metadata about the
/// usage of an expression and its result. Callers construct one of its
/// subclasses to let children nodes know the context in which the expression's
/// result is needed (i.e., whether the expression is an L-Value or R-Value).
/// It also tracks **where** the result of an expression is, somewhat emulating
/// destination-driven code generation.
///
/// This is a base class for both LValue and RValue result scope objects
class BytecodeGenerator::ExpressionResultScope {
 public:
  ExpressionResultScope(BytecodeGenerator *generator, ast::Expr::Context kind,
                        LocalVar destination = LocalVar())
      : generator_(generator),
        outer_scope_(generator->execution_result()),
        destination_(destination),
        kind_(kind) {
    generator_->set_execution_result(this);
  }

  virtual ~ExpressionResultScope() {
    generator_->set_execution_result(outer_scope_);
  }

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

// ---------------------------------------------------------
// LValue Result Scope
// ---------------------------------------------------------

/// An expression result scope that indicates the result is used as an L-Value
class BytecodeGenerator::LValueResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit LValueResultScope(BytecodeGenerator *generator,
                             LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::LValue, dest) {}
};

// ---------------------------------------------------------
// RValue Result Scope
// ---------------------------------------------------------

/// An expression result scope that indicates the result is used as an R-Value
class BytecodeGenerator::RValueResultScope
    : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit RValueResultScope(BytecodeGenerator *generator,
                             LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::RValue, dest) {}
};

// ---------------------------------------------------------
// Bytecode Position Scope
// ---------------------------------------------------------

/// A handy scoped class that tracks the start and end positions in the bytecode
/// for a given function, automatically setting the range in the function upon
/// going out of scope.
class BytecodeGenerator::BytecodePositionScope {
 public:
  BytecodePositionScope(BytecodeGenerator *generator, FunctionInfo *func)
      : generator_(generator),
        func_(func),
        start_offset_(generator->emitter()->position()) {}

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

BytecodeGenerator::BytecodeGenerator() noexcept
    : emitter_(&bytecode_), execution_result_(nullptr) {}

BytecodeGenerator::BytecodeGenerator(
    std::shared_ptr<exec::ExecutionContext> &exec_context) noexcept
    : emitter_(&bytecode_),
      execution_result_(nullptr),
      exec_context_(exec_context) {}

void BytecodeGenerator::VisitIfStmt(ast::IfStmt *node) {
  IfThenElseBuilder if_builder(this);

  // Generate condition check code
  VisitExpressionForTest(node->condition(), if_builder.then_label(),
                         if_builder.else_label(), TestFallthrough::Then);

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

void BytecodeGenerator::VisitIterationStatement(ast::IterationStmt *iteration,
                                                LoopBuilder *loop_builder) {
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
    VisitExpressionForTest(node->condition(), &loop_body_label,
                           loop_builder.break_label(), TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->next() != nullptr) {
    Visit(node->next());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitRowWiseIteration(
    ast::ForInStmt *node, LocalVar pci, LoopBuilder *table_loop,
    terrier::catalog::SqlTableRW *catalog_table) {
  // Allocate the row iteration variable
  auto *row_type = node->target()->type()->As<ast::StructType>();
  LocalVar row = current_function()->NewLocal(
      row_type, node->target()->As<ast::IdentifierExpr>()->name().data());

  //
  // Now, we generate a loop over every element in the PCI. In the beginning of
  // each iteration, we pull out the column members into the allocated row
  // structure in preparation for the body of the loop that expects rows.
  //

  {
    LoopBuilder pci_loop(this);
    pci_loop.LoopHeader();

    ast::Context *ctx = row_type->context();
    LocalVar cond = current_function()->NewLocal(
        ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
    emitter()->Emit(Bytecode::PCIHasNext, cond, pci);
    emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond.ValueOf(),
                                   pci_loop.break_label());

    // Load fields
    const auto &fields = row_type->fields();
    for (u32 col_idx = 0, offset = 0; col_idx < fields.size(); col_idx++) {
      LocalVar col_ptr =
          current_function()->NewLocal(fields[col_idx].type->PointerTo());
      emitter()->EmitLea(col_ptr, row, offset);
      // The storage layer may reorder columns, so we need to get the right
      // index.
      auto storage_idx = catalog_table->ColNumToOffset(col_idx);
      emitter()->EmitPCIGet(GetPCIColumnCode(catalog_table, col_idx),
                            col_ptr.ValueOf(), pci, storage_idx);
      offset += fields[col_idx].type->size();
    }

    // Generate body
    VisitIterationStatement(node, table_loop);

    // Advance the PCI one row
    emitter()->Emit(Bytecode::PCIAdvance, pci);

    // Finish, loop back around
    pci_loop.JumpToHeader();
  }

  // When we're done with one iteration of the loop, we reset the vector
  // projection iterator
  emitter()->Emit(Bytecode::PCIReset, pci);
}

void BytecodeGenerator::VisitVectorWiseIteration(ast::ForInStmt *node,
                                                 LocalVar pci,
                                                 LoopBuilder *table_loop) {
  //
  // When iterating vector-wise, we need to allocate a PCI* with the same name
  // as the target variable for the loop. We copy the given PCI instance for
  // each iteration
  //

  // Get the name and type of the target VPI iteration variable
  auto *iter_type = node->target()->type();
  auto *iter_name = node->target()->As<ast::IdentifierExpr>()->name().data();

  // Create the variable and assign it the value of the given VPI
  LocalVar iter = current_function()->NewLocal(iter_type, iter_name);
  BuildAssign(iter, pci, iter_type);

  // Generate body
  VisitIterationStatement(node, table_loop);
}

void BytecodeGenerator::VisitIndexedForInStmt(
    ast::ForInStmt *node, ast::Expr *index_expr,
    terrier::catalog::SqlTableRW *catalog_table) {
  // Allocate the row iteration variable
  LocalVar iterator = VisitExpressionForLValue(index_expr);
  auto *row_type = node->target()->type()->As<ast::StructType>();
  LocalVar row = current_function()->NewLocal(
      row_type, node->target()->As<ast::IdentifierExpr>()->name().data());

  {
    LoopBuilder index_loop(this);
    index_loop.LoopHeader();

    ast::Context *ctx = row_type->context();
    LocalVar cond = current_function()->NewLocal(
        ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
    emitter()->Emit(Bytecode::IndexIteratorHasNext, cond, iterator);
    emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond.ValueOf(),
                                   index_loop.break_label());

    // Advance the iterator one row
    // This calls SqlTable::Select
    emitter()->Emit(Bytecode::IndexIteratorAdvance, iterator);
    // Load fields
    const auto &fields = row_type->fields();
    for (u32 col_idx = 0, offset = 0; col_idx < fields.size(); col_idx++) {
      LocalVar col_ptr =
          current_function()->NewLocal(fields[col_idx].type->PointerTo());
      emitter()->EmitLea(col_ptr, row, offset);
      // The storage layer may reorder columns, so we need to get the right
      // index.
      auto storage_idx = catalog_table->ColNumToOffset(col_idx);
      emitter()->EmitIndexIteratorGet(
          GetIndexIteratorColumnCode(catalog_table, col_idx), col_ptr.ValueOf(),
          iterator, storage_idx);
      offset += fields[col_idx].type->size();
    }

    // Generate body
    VisitIterationStatement(node, &index_loop);


    // Finish, loop back around
    index_loop.JumpToHeader();
  }
}

void BytecodeGenerator::VisitForInStmt(ast::ForInStmt *node) {
  //
  // For both tuple-at-a-time iteration and vector-at-a-time iteration, we need
  // a TableVectorIterator which we allocate in the function first. We also need
  // a ProjectedColumnsIterator (PCI) pointer to read individual rows; PCIs are
  // also needed for vectorized processing because they allow consecutive
  // iterations and track filtered tuples. Thus, we allocate a PCI* in the
  // function, too, that we populate with the instance inside the TVI.
  //

  ast::Context *ctx = node->target()->type()->context();

  bool vectorized = false;
  bool indexed UNUSED_ATTRIBUTE = false;
  if (auto *attributes = node->attributes(); attributes != nullptr) {
    if (attributes->Contains(ctx->GetIdentifier("batch"))) {
      vectorized = true;
    }
    if (attributes->Contains(ctx->GetIdentifier("index"))) {
      indexed = true;
    }
  }

  //
  // We first initialize the TableVectorIterator and then pull out the PCI*
  // from the iterator for use in the body of the loop.
  //
  auto *exec = sql::ExecutionStructures::Instance();
  ast::Identifier table_name = node->iter()->As<ast::IdentifierExpr>()->name();
  // Get the table from the catalog.
  std::shared_ptr<SqlTableRW> catalog_table;
  if(node->GetHasOid()) {
    terrier::catalog::table_oid_t table_oid = static_cast<terrier::catalog::table_oid_t>(std::stoi(table_name.data()));
    catalog_table = exec->GetCatalog()->GetCatalogTable(
        terrier::catalog::DEFAULT_DATABASE_OID, table_oid);
  } else {
    catalog_table = exec->GetCatalog()->GetCatalogTable(
        terrier::catalog::DEFAULT_DATABASE_OID, table_name.data());
  }
  TPL_ASSERT(catalog_table != nullptr, "Table does not exist!");

  if (indexed) {
    VisitIndexedForInStmt(node,
                          node->attributes()->Find(ctx->GetIdentifier("index")),
                          catalog_table.get());
    return;
  }
  // Create TVI variable
  ast::Type *table_iter_type =
      ast::BuiltinType::Get(ctx, ast::BuiltinType::TableVectorIterator);
  LocalVar table_iter =
      current_function()->NewLocal(table_iter_type, table_name.data());

  // Bytecode for the constructor
  exec::ExecutionContext *exec_context = exec_context_.get();
  emitter()->EmitTableIteratorInit(
      Bytecode::TableVectorIteratorInit, table_iter,
      uint32_t(terrier::catalog::DEFAULT_DATABASE_OID),
      uint32_t(catalog_table->Oid()),
      reinterpret_cast<uintptr_t>(exec_context));

  // Bytecode for init.
  emitter()->Emit(Bytecode::TableVectorIteratorPerformInit, table_iter);

  //
  // Pull out the PCI from the TableVectorIterator we just initialized
  //

  ast::Type *pci_type =
      ast::BuiltinType::Get(ctx, ast::BuiltinType::ProjectedColumnsIterator);
  LocalVar pci = current_function()->NewLocal(pci_type->PointerTo(), "pci");

  emitter()->Emit(Bytecode::TableVectorIteratorGetPCI, pci, table_iter);

  //
  // Now, we generate a loop while TableVectorIterator::Advance() returns true,
  // indicating that  there is more input data. If the loop is non-vectorized,
  // then we call into VisitRowWiseIteration() to handle iteration over the
  // PCI, setting up the row pointer, resetting the PCI etc.
  //

  {
    LoopBuilder table_loop(this);
    table_loop.LoopHeader();

    LocalVar cond = current_function()->NewLocal(
        ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
    emitter()->Emit(Bytecode::TableVectorIteratorNext, cond, table_iter);
    emitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond.ValueOf(),
                                   table_loop.break_label());

    if (vectorized) {
      VisitVectorWiseIteration(node, pci.ValueOf(), &table_loop);
    } else {
      VisitRowWiseIteration(node, pci.ValueOf(), &table_loop,
                            catalog_table.get());
    }

    // Finish, loop back around
    table_loop.JumpToHeader();
  }

  // Cleanup
  emitter()->Emit(Bytecode::TableVectorIteratorFree, table_iter);
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) {
  AstVisitor::VisitFieldDecl(node);
}

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
  //
  // Lookup the local in the current function. It must be there through a
  // previous variable declaration (or parameter declaration). What is returned
  // is a pointer to the variable.
  //

  LocalVar local = current_function()->LookupLocal(node->name().data());

  if (execution_result()->IsLValue()) {
    execution_result()->set_destination(local);
    return;
  }

  //
  // The caller wants the R-Value of the identifier. So, we need to load it. If
  // the caller did not provide a destination register, we're done. If the
  // caller provided a destination, we need to move the value of the identifier
  // into the provided destination.
  //

  if (!execution_result()->HasDestination()) {
    execution_result()->set_destination(local.ValueOf());
    return;
  }

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  BuildDeref(dest, local, node->type());
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
    case ast::CastKind::IntegralCast: {
      BuildAssign(dest, input, node->type());
      break;
    }
    default: {
      // Implement me
      throw std::runtime_error("Implement me");
    }
  }
}

void BytecodeGenerator::VisitArrayIndexExpr(ast::IndexExpr *node) {
  //
  // First, we need to get the base address of the array
  //

  LocalVar arr = VisitExpressionForLValue(node->object());

  //
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
  //

  auto *type = node->object()->type()->As<ast::ArrayType>();
  auto elem_size = type->element_type()->size();

  LocalVar elem_ptr = current_function()->NewLocal(node->type()->PointerTo());

  if (auto *literal_index = node->index()->SafeAs<ast::LitExpr>()) {
    i32 index = literal_index->int32_val();
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

  //
  // The caller wants the value of the array element. We just computed the
  // element's pointer (in element_ptr). Just dereference it into the desired
  // location and be done with it.
  //

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
    TPL_ASSERT(node->initial()->type() != nullptr,
               "Variable with initial value is missing resolved type");
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
  TPL_ASSERT(execution_result()->IsRValue(),
             "Address-of expressions must be R-values!");
  LocalVar addr = VisitExpressionForLValue(op->expr());
  if (execution_result()->HasDestination()) {
    // Despite the below function's name, we're just getting the destination
    LocalVar dest = execution_result()->GetOrCreateDestination(op->type());
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
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Neg),
                                     op->type());
      break;
    }
    case parsing::Token::Type::BIT_NOT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitNeg),
                                     op->type());
      break;
    }
    default: { UNREACHABLE("Impossible unary operation"); }
  }

  // Emit
  emitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
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

void BytecodeGenerator::VisitSqlConversionCall(ast::CallExpr *call,
                                               ast::Builtin builtin) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "SQL conversions must be R-Values");

  auto *ctx = call->type()->context();

  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      auto dest = execution_result()->GetOrCreateDestination(
          ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitBool, dest, input);
      break;
    }
    case ast::Builtin::IntToSql: {
      auto dest = execution_result()->GetOrCreateDestination(
          ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitInteger, dest, input);
      break;
    }
    case ast::Builtin::FloatToSql: {
      auto dest = execution_result()->GetOrCreateDestination(
          ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::Builtin::SqlToBool: {
      auto dest = execution_result()->GetOrCreateDestination(
          ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      auto input = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      break;
    }
    default: { UNREACHABLE("Impossible SQL conversion call"); }
  }
}

void BytecodeGenerator::VisitBuiltinFilterCall(ast::CallExpr *call,
                                               ast::Builtin builtin) {
  ast::Context *ctx = call->type()->context();
  ast::Type *ret_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::Int32);

  LocalVar ret_val;
  if (execution_result() != nullptr) {
    ret_val = execution_result()->GetOrCreateDestination(ret_type);
    execution_result()->set_destination(ret_val.ValueOf());
  } else {
    ret_val = current_function()->NewLocal(ret_type);
  }

  // Collect the three call arguments
  // Projected Column Iterator
  LocalVar pci = VisitExpressionForRValue(call->arguments()[0]);
  // Filter Value
  i64 val = call->arguments()[2]->As<ast::LitExpr>()->int32_val();
  // Table and column name.
  std::string table_col(
      call->arguments()[1]->As<ast::LitExpr>()->raw_string_val().data());
  size_t split_idx = table_col.find('.');
  std::string table_name(table_col.substr(0, split_idx));
  std::string col_name(table_col.substr(split_idx + 1));

  // Get the index and type of the column.
  auto *exec = sql::ExecutionStructures::Instance();
  auto catalog_table = exec->GetCatalog()->GetCatalogTable(
      terrier::catalog::DEFAULT_DATABASE_OID, table_name);
  TypeId col_type = TypeId::INVALID;
  uint32_t col_idx = 0;
  for (const auto &col :
       catalog_table->GetSqlTable()->GetSchema().GetColumns()) {
    if (col.GetName() == col_name) {
      col_type = col.GetType();
      col_idx = catalog_table->ColNumToOffset(col_idx);
      break;
    }
    col_idx++;
  }

  Bytecode bytecode = Bytecode::PCIFilterEqual;
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

  emitter()->EmitPCIVectorFilter(bytecode, ret_val, pci, col_idx,
                                 static_cast<i8>(col_type), val);
}

void BytecodeGenerator::VisitBuiltinJoinHashTableCall(ast::CallExpr *call,
                                                      ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::JoinHashTableInit: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar region = VisitExpressionForRValue(call->arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[2]);
      emitter()->Emit(Bytecode::JoinHashTableInit, join_hash_table, region,
                      entry_size);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->Emit(Bytecode::JoinHashTableAllocTuple, dest, join_hash_table,
                      hash);
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      LocalVar join_hash_table = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::JoinHashTableBuild, join_hash_table);
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

void BytecodeGenerator::VisitBuiltinOutputCall(ast::CallExpr *call,
                                               ast::Builtin builtin) {
  exec::ExecutionContext *exec_context = exec_context_.get();
  // TODO(Amadou) Find a more elegant way to pass in the execution context
  auto exec_context_addr = reinterpret_cast<uintptr_t>(exec_context);
  switch (builtin) {
    case ast::Builtin::OutputAlloc: {
      LocalVar dest = execution_result()->GetOrCreateDestination(call->type());
      emitter()->EmitOutputAlloc(Bytecode::OutputAlloc, exec_context_addr,
                                 dest);
      break;
    }
    case ast::Builtin::OutputAdvance: {
      emitter()->EmitOutputCall(Bytecode::OutputAdvance, exec_context_addr);
      break;
    }
    case ast::Builtin::OutputSetNull: {
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->EmitOutputSetNull(Bytecode::OutputSetNull, exec_context_addr,
                                   entry_size);
      break;
    }
    case ast::Builtin::OutputFinalize: {
      emitter()->EmitOutputCall(Bytecode::OutputFinalize, exec_context_addr);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinIndexIteratorCall(ast::CallExpr *call,
                                                      ast::Builtin builtin) {
  exec::ExecutionContext *exec_context = exec_context_.get();
  Catalog *catalog =
      sql::ExecutionStructures::Instance()->GetCatalog();
  // TODO(Amadou) Find a more elegant way to pass in the execution context
  auto exec_context_addr = reinterpret_cast<uintptr_t>(exec_context);
  switch (builtin) {
    case ast::Builtin::IndexIteratorInit: {
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      std::string index_name(
          call->arguments()[1]->As<ast::LitExpr>()->raw_string_val().data());
      auto index_oid = catalog->GetCatalogIndexOid(index_name);
      emitter()->EmitIndexIteratorInit(Bytecode::IndexIteratorInit, iterator,
                                       !index_oid, exec_context_addr);
      break;
    }
    case ast::Builtin::IndexIteratorScanKey: {
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar key = VisitExpressionForRValue(call->arguments()[1]);
      emitter()->EmitIndexIteratorScanKey(Bytecode::IndexIteratorScanKey,
                                          iterator, key);
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      LocalVar iterator = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->EmitIndexIteratorFree(Bytecode::IndexIteratorFree, iterator);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinSorterCall(ast::CallExpr *call,
                                               ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::SorterInit: {
      // TODO(pmenon): Fix me so that the comparison function doesn't have be
      // listed by name.
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      LocalVar region = VisitExpressionForRValue(call->arguments()[1]);
      // LocalVar cmp_fn = VisitExpressionForRValue(call->arguments()[2]);
      const std::string cmp_func_name =
          call->arguments()[2]->As<ast::IdentifierExpr>()->name().data();
      auto cmp_fn = current_function()->NewLocal(ast::BuiltinType::Get(
          call->type()->context(), ast::BuiltinType::Uint16));
      emitter()->EmitAssignImm2(cmp_fn, LookupFuncIdByName(cmp_func_name));
      LocalVar entry_size = VisitExpressionForRValue(call->arguments()[3]);
      emitter()->Emit(Bytecode::SorterInit, sorter, region, cmp_fn.ValueOf(),
                      entry_size);
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
    case ast::Builtin::SorterFree: {
      LocalVar sorter = VisitExpressionForRValue(call->arguments()[0]);
      emitter()->Emit(Bytecode::SorterFree, sorter);
      break;
    }
    default: { UNREACHABLE("Impossible bytecode"); }
  }
}

void BytecodeGenerator::VisitBuiltinRegionCall(ast::CallExpr *call,
                                               ast::Builtin builtin) {
  LocalVar region = VisitExpressionForRValue(call->arguments()[0]);
  auto region_op = builtin == ast::Builtin::RegionInit ? Bytecode::RegionInit
                                                       : Bytecode::RegionFree;
  emitter()->Emit(region_op, region);
}

void BytecodeGenerator::VisitBuiltinSizeOfCall(ast::CallExpr *call) {
  ast::Type *target_type = call->arguments()[0]->type();
  LocalVar size_var = execution_result()->GetOrCreateDestination(
      ast::BuiltinType::Get(target_type->context(), ast::BuiltinType::Uint32));
  emitter()->EmitAssignImm4(size_var, target_type->size());
  execution_result()->set_destination(size_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinCallExpr(ast::CallExpr *call) {
  ast::Builtin builtin;

  ast::Context *ctx = call->type()->context();
  ctx->IsBuiltinFunction(call->GetFuncName(), &builtin);

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
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
    case ast::Builtin::RegionInit:
    case ast::Builtin::RegionFree: {
      VisitBuiltinRegionCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableInit:
    case ast::Builtin::JoinHashTableInsert:
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableFree: {
      VisitBuiltinJoinHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::OutputAlloc:
    case ast::Builtin::OutputAdvance:
    case ast::Builtin::OutputFinalize:
    case ast::Builtin::OutputSetNull:
      VisitBuiltinOutputCall(call, builtin);
      break;
    case ast::Builtin::IndexIteratorInit:
    case ast::Builtin::IndexIteratorScanKey:
    case ast::Builtin::IndexIteratorFree:
      VisitBuiltinIndexIteratorCall(call, builtin);
      break;
    case ast::Builtin::SorterInit:
    case ast::Builtin::SorterInsert:
    case ast::Builtin::SorterSort:
    case ast::Builtin::SorterFree: {
      VisitBuiltinSorterCall(call, builtin);
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
    default: { UNREACHABLE("Builtin not supported!"); }
  }
}

void BytecodeGenerator::VisitRegularCallExpr(ast::CallExpr *call) {
  bool caller_wants_result = execution_result() != nullptr;
  TPL_ASSERT(!caller_wants_result || execution_result()->IsRValue(),
             "Calls can only be R-Values!");

  std::vector<LocalVar> params;

  auto *func_type = call->function()->type()->As<ast::FunctionType>();

  if (!func_type->return_type()->IsNilType()) {
    LocalVar ret_val;
    if (caller_wants_result) {
      ret_val =
          execution_result()->GetOrCreateDestination(func_type->return_type());

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
  TPL_ASSERT(execution_result()->IsRValue(),
             "Literal expressions cannot be R-Values!");

  LocalVar target = execution_result()->GetOrCreateDestination(node->type());

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
      emitter()->EmitAssignImm4(target, node->int32_val());
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
  TPL_ASSERT(execution_result()->IsRValue(),
             "Binary expressions must be R-Values!");
  TPL_ASSERT(node->type()->IsBoolType(),
             "Boolean binary operation must be of type bool");

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

void BytecodeGenerator::VisitArithmeticExpr(ast::BinaryOpExpr *node) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Arithmetic expressions must be R-Values!");

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  LocalVar left = VisitExpressionForRValue(node->left());
  LocalVar right = VisitExpressionForRValue(node->right());

  Bytecode bytecode;
  switch (node->op()) {
    case parsing::Token::Type::PLUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Add),
                                     node->type());
      break;
    }
    case parsing::Token::Type::MINUS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Sub),
                                     node->type());
      break;
    }
    case parsing::Token::Type::STAR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Mul),
                                     node->type());
      break;
    }
    case parsing::Token::Type::SLASH: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Div),
                                     node->type());
      break;
    }
    case parsing::Token::Type::PERCENT: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Rem),
                                     node->type());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitAnd),
                                     node->type());
      break;
    }
    case parsing::Token::Type::BIT_OR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitOr),
                                     node->type());
      break;
    }
    case parsing::Token::Type::BIT_XOR: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::BitXor),
                                     node->type());
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest.ValueOf());
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

void BytecodeGenerator::VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "SQL comparison expressions must be R-Values!");

  LocalVar dest = execution_result()->GetOrCreateDestination(compare->type());
  LocalVar left = VisitExpressionForLValue(compare->left());
  LocalVar right = VisitExpressionForLValue(compare->right());
  Bytecode code;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      code = Bytecode::GreaterThanInteger;
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      code = Bytecode::GreaterThanEqualInteger;
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      code = Bytecode::EqualInteger;
      break;
    }
    case parsing::Token::Type::LESS: {
      code = Bytecode::LessThanInteger;
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      code = Bytecode::LessThanEqualInteger;
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      code = Bytecode::NotEqualInteger;
      break;
    }
    default: { UNREACHABLE("Impossible binary operation"); }
  }

  // Emit
  emitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  execution_result()->set_destination(dest);
}

void BytecodeGenerator::VisitPrimitiveCompareOpExpr(
    ast::ComparisonOpExpr *compare) {
  TPL_ASSERT(execution_result()->IsRValue(),
             "Comparison expressions must be R-Values!");

  LocalVar dest = execution_result()->GetOrCreateDestination(compare->type());
  LocalVar left = VisitExpressionForRValue(compare->left());
  LocalVar right = VisitExpressionForRValue(compare->right());

  Bytecode bytecode;
  switch (compare->op()) {
    case parsing::Token::Type::GREATER: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThan),
                              compare->left()->type());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      bytecode = GetIntTypedBytecode(
          GET_BASE_FOR_INT_TYPES(Bytecode::GreaterThanEqual),
          compare->left()->type());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::Equal),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThan),
                                     compare->left()->type());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      bytecode =
          GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::LessThanEqual),
                              compare->left()->type());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      bytecode = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::NotEqual),
                                     compare->left()->type());
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
  TPL_ASSERT(execution_result()->IsRValue(),
             "Comparison expressions must be R-Values!");

  const bool is_primitive_comparison =
      node->type()->IsSpecificBuiltin(ast::BuiltinType::Bool);

  if (!is_primitive_comparison) {
    VisitSqlCompareOpExpr(node);
  } else {
    VisitPrimitiveCompareOpExpr(node);
  }
}

void BytecodeGenerator::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  Visit(node->body());
}

void BytecodeGenerator::BuildAssign(LocalVar dest, LocalVar ptr,
                                    ast::Type *dest_type) {
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

void BytecodeGenerator::BuildDeref(LocalVar dest, LocalVar ptr,
                                   ast::Type *dest_type) {
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

LocalVar BytecodeGenerator::BuildLoadPointer(LocalVar double_ptr,
                                             ast::Type *type) {
  if (double_ptr.GetAddressMode() == LocalVar::AddressMode::Address) {
    return double_ptr.ValueOf();
  }

  // Need to Deref
  LocalVar ptr = current_function()->NewLocal(type);
  emitter()->EmitDeref(Bytecode::Deref8, ptr, double_ptr);
  return ptr.ValueOf();
}

void BytecodeGenerator::VisitMemberExpr(ast::MemberExpr *node) {
  //
  // We first need to compute the address of the object we're selecting into.
  // Thus, we get the L-Value of the object below.
  //

  LocalVar obj_ptr = VisitExpressionForLValue(node->object());

  //
  // We now need to compute the offset of the field in the composite type. TPL
  // unifies C's arrow and dot syntax for field/member access. Thus, the type
  // of the object may be either a pointer to a struct or the actual struct. If
  // the type is a pointer, then the L-Value of the object is actually a double
  // pointer and we need to dereference it; otherwise, we can use the address
  // as is.
  //

  ast::StructType *obj_type = nullptr;
  if (auto *type = node->object()->type(); node->IsSugaredArrow()) {
    // Double pointer, need to dereference
    obj_ptr = BuildLoadPointer(obj_ptr, type);
    obj_type = type->As<ast::PointerType>()->base()->As<ast::StructType>();
  } else {
    obj_type = type->As<ast::StructType>();
  }

  //
  // We're now ready to compute offset. Let's lookup the field's offset in the
  // struct type.
  //

  auto *field_name = node->member()->As<ast::IdentifierExpr>();
  auto offset = obj_type->GetOffsetOfFieldByName(field_name->name());

  //
  // Now that we have a pointer to the composite object, we need to compute a
  // pointer to the field within the object. If the offset of the field in the
  // object is zero, we needn't do anything - we can just reinterpret the object
  // pointer. If the field offset is greater than zero, we generate a LEA.
  //

  LocalVar field_ptr;
  if (offset == 0) {
    field_ptr = obj_ptr;
  } else {
    field_ptr = current_function()->NewLocal(node->type()->PointerTo());
    emitter()->EmitLea(field_ptr, obj_ptr, offset);
    field_ptr = field_ptr.ValueOf();
  }

  if (execution_result()->IsLValue()) {
    TPL_ASSERT(!execution_result()->HasDestination(),
               "L-Values produce their destination");
    execution_result()->set_destination(field_ptr);
    return;
  }

  //
  // The caller wants the actual value of the field. We just computed a pointer
  // to the field in the object, so we need to load/dereference it. If the
  // caller provided a destination variable, use that; otherwise, create a new
  // temporary variable to store the value.
  //

  LocalVar dest = execution_result()->GetOrCreateDestination(node->type());
  BuildDeref(dest, field_ptr, node->type());
  execution_result()->set_destination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) {
  Visit(node->declaration());
}

void BytecodeGenerator::VisitExpressionStmt(ast::ExpressionStmt *node) {
  Visit(node->expression());
}

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

FunctionInfo *BytecodeGenerator::AllocateFunc(
    const std::string &func_name, ast::FunctionType *const func_type) {
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

FunctionId BytecodeGenerator::LookupFuncIdByName(
    const std::string &name) const {
  auto iter = func_map_.find(name);
  if (iter == func_map_.end()) {
    return FunctionInfo::kInvalidFuncId;
  }
  return iter->second;
}

const FunctionInfo *BytecodeGenerator::LookupFuncInfoByName(
    const std::string &name) const {
  const auto iter = func_map_.find(name);
  if (iter == func_map_.end()) {
    return nullptr;
  }
  return &functions_[iter->second];
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

void BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr,
                                                 LocalVar dest) {
  RValueResultScope scope(this, dest);
  Visit(expr);
}

void BytecodeGenerator::VisitExpressionForTest(ast::Expr *expr,
                                               BytecodeLabel *then_label,
                                               BytecodeLabel *else_label,
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

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode,
                                                ast::Type *type) {
  TPL_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto int_kind = type->SafeAs<ast::BuiltinType>()->kind();
  auto kind_idx = static_cast<u8>(int_kind - ast::BuiltinType::Int8);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

Bytecode BytecodeGenerator::GetPCIColumnCode(SqlTableRW *catalog_table,
                                             u32 col_idx) {
  const auto &schema = catalog_table->GetSqlTable()->GetSchema();
  if (schema.GetColumns()[col_idx].GetNullable()) {
    switch (schema.GetColumns()[col_idx].GetType()) {
      case TypeId::INTEGER:
        return Bytecode::PCIGetIntegerNull;
      case TypeId::SMALLINT:
        return Bytecode::PCIGetSmallIntNull;
      case TypeId::BIGINT:
        return Bytecode::PCIGetBigIntNull;
      case TypeId::DECIMAL:
        return Bytecode::PCIGetDecimalNull;
      default:
        // TODO(Amadou): Handle errors here.
        return Bytecode::PCIGetInteger;
    }
  } else {
    switch (schema.GetColumns()[col_idx].GetType()) {
      case TypeId::INTEGER:
        return Bytecode::PCIGetInteger;
      case TypeId::SMALLINT:
        return Bytecode::PCIGetSmallInt;
      case TypeId::BIGINT:
        return Bytecode::PCIGetBigInt;
      case TypeId::DECIMAL:
        return Bytecode::PCIGetDecimal;
      default:
        // TODO(Amadou): Handle errors here.
        return Bytecode::PCIGetInteger;
    }
  }
}

Bytecode BytecodeGenerator::GetIndexIteratorColumnCode(
    SqlTableRW *catalog_table, u32 col_idx) {
  const auto &schema = catalog_table->GetSqlTable()->GetSchema();
  if (schema.GetColumns()[col_idx].GetNullable()) {
    switch (schema.GetColumns()[col_idx].GetType()) {
      case TypeId::INTEGER:
        return Bytecode::IndexIteratorGetIntegerNull;
      case TypeId::SMALLINT:
        return Bytecode::IndexIteratorGetSmallIntNull;
      case TypeId::BIGINT:
        return Bytecode::IndexIteratorGetBigIntNull;
      case TypeId::DECIMAL:
        return Bytecode::IndexIteratorGetDecimalNull;
      default:
        // TODO(Amadou): Handle errors here.
        return Bytecode::IndexIteratorGetInteger;
    }
  } else {
    switch (schema.GetColumns()[col_idx].GetType()) {
      case TypeId::INTEGER:
        return Bytecode::IndexIteratorGetInteger;
      case TypeId::SMALLINT:
        return Bytecode::IndexIteratorGetSmallInt;
      case TypeId::BIGINT:
        return Bytecode::IndexIteratorGetBigInt;
      case TypeId::DECIMAL:
        return Bytecode::IndexIteratorGetDecimal;
      default:
        // TODO(Amadou): Handle errors here.
        return Bytecode::IndexIteratorGetInteger;
    }
  }
}

// static
std::unique_ptr<BytecodeModule> BytecodeGenerator::Compile(
    ast::AstNode *root, const std::string &name,
    std::shared_ptr<exec::ExecutionContext> &exec_context) {
  BytecodeGenerator generator(exec_context);
  generator.Visit(root);

  // NOLINTNEXTLINE
  return std::make_unique<BytecodeModule>(name, std::move(generator.bytecode_),
                                          std::move(generator.functions_));
}

}  // namespace tpl::vm
