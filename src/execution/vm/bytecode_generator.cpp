#include "execution/vm/bytecode_generator.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "common/macros.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sql/sql_def.h"
#include "execution/vm/bytecode_label.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/control_flow_builders.h"
#include "loggers/execution_logger.h"
#include "spdlog/fmt/fmt.h"

namespace noisepage::execution::vm {

/**
 * ExpressionResultScope is an RAII class that provides metadata about the usage of an expression
 * and its result. Callers construct one of its subclasses to let children nodes know the context in
 * which the expression's result is needed (i.e., whether the expression is an L-Value or R-Value).
 * It also tracks <b>where</b> the result of an expression is, somewhat emulating destination-driven
 * code generation.
 *
 * This is a base class for both LValue and RValue result scope objects.
 */
class BytecodeGenerator::ExpressionResultScope {
 public:
  /**
   * Construct an expression scope of kind @em kind. The destination where the result of the
   * expression is written to is @em destination.
   * @param generator The code generator.
   * @param kind The kind of expression.
   * @param destination Where the result of the expression is written to.
   */
  ExpressionResultScope(BytecodeGenerator *generator, ast::Expr::Context kind, LocalVar destination = LocalVar())
      : generator_(generator), outer_scope_(generator->GetExecutionResult()), destination_(destination), kind_(kind) {
    generator_->SetExecutionResult(this);
  }

  /**
   * Destructor.
   */
  virtual ~ExpressionResultScope() { generator_->SetExecutionResult(outer_scope_); }

  /**
   * @return True if the expression is an L-Value expression; false otherwise.
   */
  bool IsLValue() const { return kind_ == ast::Expr::Context::LValue; }

  /**
   * @return True if the expression is an R-Value expression; false otherwise.
   */
  bool IsRValue() const { return kind_ == ast::Expr::Context::RValue; }

  /**
   * @return True if the expression has an assigned destination where the result is written to.
   */
  bool HasDestination() const { return !GetDestination().IsInvalid(); }

  /**
   * Return the destination where the result of the expression is written to. If one does not exist,
   * assign one of with type @em type and set it in this scope.
   * @param type The type of the result of the expression.
   * @return The destination where the result of the expression is written to.
   */
  LocalVar GetOrCreateDestination(ast::Type *type) {
    if (!HasDestination()) {
      destination_ = generator_->GetCurrentFunction()->NewLocal(type);
    }

    return destination_;
  }

  /**
   * @return The destination local where the result is written.
   */
  LocalVar GetDestination() const { return destination_; }

  /**
   * Set the local where the result of the expression is written to.
   */
  void SetDestination(LocalVar destination) { destination_ = destination; }

 private:
  BytecodeGenerator *generator_;
  ExpressionResultScope *outer_scope_;
  LocalVar destination_;
  ast::Expr::Context kind_;
};

/**
 * An expression result scope that indicates the result is used as an L-Value.
 */
class BytecodeGenerator::LValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit LValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::LValue, dest) {}
};

/**
 * An expression result scope that indicates the result is used as an R-Value.
 */
class BytecodeGenerator::RValueResultScope : public BytecodeGenerator::ExpressionResultScope {
 public:
  explicit RValueResultScope(BytecodeGenerator *generator, LocalVar dest = LocalVar())
      : ExpressionResultScope(generator, ast::Expr::Context::RValue, dest) {}
};

/**
 * A handy scoped class that tracks the start and end positions in the bytecode for a given
 * function, automatically setting the range in the function upon going out of scope.
 */
class BytecodeGenerator::BytecodePositionScope {
 public:
  BytecodePositionScope(BytecodeGenerator *generator, FunctionInfo *func)
      : generator_(generator), func_(func), start_offset_(generator->GetEmitter()->GetPosition()) {}

  ~BytecodePositionScope() {
    const std::size_t end_offset = generator_->GetEmitter()->GetPosition();
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

BytecodeGenerator::BytecodeGenerator() noexcept : emitter_(&code_) {}

void BytecodeGenerator::VisitIfStmt(ast::IfStmt *node) {
  IfThenElseBuilder if_builder(this);

  // Generate condition check code
  VisitExpressionForTest(node->Condition(), if_builder.GetThenLabel(), if_builder.GetElseLabel(),
                         TestFallthrough::Then);

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
    VisitExpressionForTest(node->Condition(), &loop_body_label, loop_builder.GetBreakLabel(), TestFallthrough::Then);
  }

  VisitIterationStatement(node, &loop_builder);

  if (node->Next() != nullptr) {
    Visit(node->Next());
  }

  loop_builder.JumpToHeader();
}

void BytecodeGenerator::VisitForInStmt(UNUSED_ATTRIBUTE ast::ForInStmt *node) {
  NOISEPAGE_ASSERT(false, "For-in statements not supported");
}

void BytecodeGenerator::VisitFieldDecl(ast::FieldDecl *node) { AstVisitor::VisitFieldDecl(node); }

void BytecodeGenerator::VisitFunctionDecl(ast::FunctionDecl *node) {
  // The function's TPL type
  auto *func_type = node->TypeRepr()->GetType()->As<ast::FunctionType>();

  // Allocate the function
  FunctionInfo *func_info = AllocateFunc(node->Name().GetData(), func_type);

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

  const std::string local_name = node->Name().GetData();
  LocalVar local = GetCurrentFunction()->LookupLocal(local_name);

  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(local);
    return;
  }

  // The caller wants the R-Value of the identifier. So, we need to load it. If
  // the caller did not provide a destination register, we're done. If the
  // caller provided a destination, we need to move the value of the identifier
  // into the provided destination.

  if (!GetExecutionResult()->HasDestination()) {
    GetExecutionResult()->SetDestination(local.ValueOf());
    return;
  }

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());

  // If the local we want the R-Value of is a parameter, we can't take its
  // pointer for the deref, so we use an assignment. Otherwise, a deref is good.
  if (auto *local_info = GetCurrentFunction()->LookupLocalInfoByName(local_name); local_info->IsParameter()) {
    BuildAssign(dest, local.ValueOf(), node->GetType());
  } else {
    BuildDeref(dest, local, node->GetType());
  }

  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  switch (node->GetCastKind()) {
    case ast::CastKind::SqlBoolToBool: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForSQLValue(node->Input());
      GetEmitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::CastKind::BoolToSqlBool: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForRValue(node->Input());
      GetEmitter()->Emit(Bytecode::InitBool, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::IntToSqlInt: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForRValue(node->Input());
      ast::Expr *arg = node->Input();
      Bytecode bytecode = Bytecode::InitInteger;

      if (arg->IsIntegerLiteral()) {
        int64_t input_val = arg->As<ast::LitExpr>()->Int64Val();
        bool fits_in_int = static_cast<int64_t>(std::numeric_limits<int>::lowest()) <= input_val &&
                           input_val <= static_cast<int64_t>(std::numeric_limits<int>::max());
        if (!fits_in_int) {
          bytecode = Bytecode::InitInteger64;
        }
      }
      GetEmitter()->Emit(bytecode, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::BitCast:
    case ast::CastKind::IntegralCast: {
      LocalVar input = VisitExpressionForRValue(node->Input());
      // As an optimization, we only issue a new assignment if the input and
      // output types of the cast have different sizes.
      if (node->Input()->GetType()->GetSize() != node->GetType()->GetSize()) {
        LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
        BuildAssign(dest, input, node->GetType());
        GetExecutionResult()->SetDestination(dest.ValueOf());
      } else {
        GetExecutionResult()->SetDestination(input);
      }
      break;
    }
    case ast::CastKind::FloatToSqlReal: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForRValue(node->Input());
      GetEmitter()->Emit(Bytecode::InitReal, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    case ast::CastKind::SqlIntToSqlReal: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
      LocalVar input = VisitExpressionForSQLValue(node->Input());
      GetEmitter()->Emit(Bytecode::IntegerToReal, dest, input);
      GetExecutionResult()->SetDestination(dest);
      break;
    }
    default: {
      throw NOT_IMPLEMENTED_EXCEPTION(
          fmt::format("'{}' cast is not implemented", ast::CastKindToString(node->GetCastKind())));
    }
  }
}

void BytecodeGenerator::VisitArrayIndexExpr(ast::IndexExpr *node) {
  // The type and the element's size
  auto *type = node->Object()->GetType()->As<ast::ArrayType>();
  auto elem_size = type->GetElementType()->GetSize();

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

  LocalVar elem_ptr = GetCurrentFunction()->NewLocal(node->GetType()->PointerTo());

  if (node->Index()->IsIntegerLiteral()) {
    const auto index = static_cast<int32_t>(node->Index()->As<ast::LitExpr>()->Int64Val());
    NOISEPAGE_ASSERT(index >= 0, "Array indexes must be non-negative");
    GetEmitter()->EmitLea(elem_ptr, arr, (elem_size * index));
  } else {
    LocalVar index = VisitExpressionForRValue(node->Index());
    GetEmitter()->EmitLeaScaled(elem_ptr, arr, index, elem_size, 0);
  }

  elem_ptr = elem_ptr.ValueOf();

  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(elem_ptr);
    return;
  }

  // The caller wants the value of the array element. We just computed the
  // element's pointer (in element_ptr). Just dereference it into the desired
  // location and be done with it.

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  BuildDeref(dest, elem_ptr, node->GetType());
  GetExecutionResult()->SetDestination(dest.ValueOf());
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
    NOISEPAGE_ASSERT(node->TypeRepr()->GetType() != nullptr,
                     "Variable with explicit type declaration is missing resolved "
                     "type at runtime!");
    type = node->TypeRepr()->GetType();
  } else {
    NOISEPAGE_ASSERT(node->Initial() != nullptr,
                     "Variable without explicit type declaration is missing an "
                     "initialization expression!");
    NOISEPAGE_ASSERT(node->Initial()->GetType() != nullptr, "Variable with initial value is missing resolved type");
    type = node->Initial()->GetType();
  }

  // Register this variable in the function as a local
  LocalVar local = GetCurrentFunction()->NewLocal(type, node->Name().GetData());

  // If there's an initializer, generate code for it now
  if (node->Initial() != nullptr) {
    VisitExpressionForRValue(node->Initial(), local);
  }
}

void BytecodeGenerator::VisitAddressOfExpr(ast::UnaryOpExpr *op) {
  NOISEPAGE_ASSERT(GetExecutionResult()->IsRValue(), "Address-of expressions must be R-values!");
  LocalVar addr = VisitExpressionForLValue(op->Input());
  if (GetExecutionResult()->HasDestination()) {
    LocalVar dest = GetExecutionResult()->GetDestination();
    BuildAssign(dest, addr, op->GetType());
  } else {
    GetExecutionResult()->SetDestination(addr);
  }
}

void BytecodeGenerator::VisitDerefExpr(ast::UnaryOpExpr *op) {
  LocalVar addr = VisitExpressionForRValue(op->Input());
  if (GetExecutionResult()->IsLValue()) {
    GetExecutionResult()->SetDestination(addr);
  } else {
    LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->GetType());
    BuildDeref(dest, addr, op->GetType());
    GetExecutionResult()->SetDestination(dest.ValueOf());
  }
}

void BytecodeGenerator::VisitArithmeticUnaryExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->GetType());
  LocalVar input = VisitExpressionForRValue(op->Input());

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
  GetEmitter()->EmitUnaryOp(bytecode, dest, input);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitLogicalNotExpr(ast::UnaryOpExpr *op) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(op->GetType());
  LocalVar input;
  if (op->GetType()->IsBoolType()) {
    input = VisitExpressionForRValue(op->Input());
    GetEmitter()->EmitUnaryOp(Bytecode::Not, dest, input);
    GetExecutionResult()->SetDestination(dest.ValueOf());
  } else if (op->GetType()->IsSqlBooleanType()) {
    input = VisitExpressionForSQLValue(op->Input());
    GetEmitter()->EmitUnaryOp(Bytecode::NotSql, dest, input);
    GetExecutionResult()->SetDestination(dest);
  }
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
    LocalVar rv = GetCurrentFunction()->GetReturnValueLocal();
    if (node->Ret()->GetType()->IsSqlValueType()) {
      LocalVar result = VisitExpressionForSQLValue(node->Ret());
      BuildDeref(rv.ValueOf(), result, node->Ret()->GetType());
    } else {
      LocalVar result = VisitExpressionForRValue(node->Ret());
      BuildAssign(rv.ValueOf(), result, node->Ret()->GetType());
    }
  }
  GetEmitter()->EmitReturn();
}

void BytecodeGenerator::VisitSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  NOISEPAGE_ASSERT(call->GetType() != nullptr, "No return type set for call!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());

  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::InitBool, dest, input);
      break;
    }
    case ast::Builtin::IntToSql: {
      const auto &arg = call->Arguments()[0];
      Bytecode bytecode = Bytecode::InitInteger;

      if (arg->IsIntegerLiteral()) {
        int64_t input_val = arg->As<ast::LitExpr>()->Int64Val();
        bool fits_in_int = static_cast<int64_t>(std::numeric_limits<int>::lowest()) <= input_val &&
                           input_val <= static_cast<int64_t>(std::numeric_limits<int>::max());
        if (!fits_in_int) {
          bytecode = Bytecode::InitInteger64;
        }
      }
      auto input = VisitExpressionForRValue(arg);
      GetEmitter()->Emit(bytecode, dest, input);
      break;
    }
    case ast::Builtin::FloatToSql: {
      auto input = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::InitReal, dest, input);
      break;
    }
    case ast::Builtin::DateToSql: {
      auto year = VisitExpressionForRValue(call->Arguments()[0]);
      auto month = VisitExpressionForRValue(call->Arguments()[1]);
      auto day = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::InitDate, dest, year, month, day);
      break;
    }
    case ast::Builtin::TimestampToSql: {
      auto usec = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::InitTimestamp, dest, usec);
      break;
    }
    case ast::Builtin::TimestampToSqlYMDHMSMU: {
      auto year = VisitExpressionForRValue(call->Arguments()[0]);
      auto month = VisitExpressionForRValue(call->Arguments()[1]);
      auto day = VisitExpressionForRValue(call->Arguments()[2]);
      auto h = VisitExpressionForRValue(call->Arguments()[3]);
      auto m = VisitExpressionForRValue(call->Arguments()[4]);
      auto s = VisitExpressionForRValue(call->Arguments()[5]);
      auto ms = VisitExpressionForRValue(call->Arguments()[6]);
      auto us = VisitExpressionForRValue(call->Arguments()[7]);
      GetEmitter()->Emit(Bytecode::InitTimestampYMDHMSMU, dest, year, month, day, h, m, s, ms, us);
      break;
    }
    case ast::Builtin::StringToSql: {
      auto string_lit = call->Arguments()[0]->As<ast::LitExpr>()->StringVal();
      auto static_string = NewStaticString(call->GetType()->GetContext(), string_lit);
      GetEmitter()->EmitInitString(dest, static_string, string_lit.GetLength());
      break;
    }
    case ast::Builtin::SqlToBool: {
      auto input = VisitExpressionForSQLValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::ForceBoolTruth, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }

#define GEN_CASE(Builtin, Bytecode)                                \
  case Builtin: {                                                  \
    auto input = VisitExpressionForSQLValue(call->Arguments()[0]); \
    GetEmitter()->Emit(Bytecode, dest, input);                     \
    break;                                                         \
  }
      GEN_CASE(ast::Builtin::ConvertBoolToInteger, Bytecode::BoolToInteger);
      GEN_CASE(ast::Builtin::ConvertIntegerToReal, Bytecode::IntegerToReal);
      GEN_CASE(ast::Builtin::ConvertDateToTimestamp, Bytecode::DateToTimestamp);
      GEN_CASE(ast::Builtin::ConvertStringToBool, Bytecode::StringToBool);
      GEN_CASE(ast::Builtin::ConvertStringToInt, Bytecode::StringToInteger);
      GEN_CASE(ast::Builtin::ConvertStringToReal, Bytecode::StringToReal);
      GEN_CASE(ast::Builtin::ConvertStringToDate, Bytecode::StringToDate);
      GEN_CASE(ast::Builtin::ConvertStringToTime, Bytecode::StringToTimestamp);
#undef GEN_CASE

    default: {
      UNREACHABLE("Impossible SQL conversion call");
    }
  }
}

void BytecodeGenerator::VisitNullValueCall(ast::CallExpr *call, UNUSED_ATTRIBUTE ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::IsValNull: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar input = VisitExpressionForSQLValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::ValIsNull, result, input);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::InitSqlNull: {
      // The type of NULL to be created should have been set in sema.
      // Per discussions with pmenon, the NULL type should be determined during bytecode generation.
      // Currently, all SQL types do not need special behavior for NULLs, and it suffices to create
      // a Val::Null() to handle every use-case. However, if custom NULL objects are required in the
      // future, then the switching on the type of the NULL should also be done in this function.
      // The idea is to avoid the overhead of doing it at runtime.
      auto dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::InitSqlNull, dest);
      break;
    }
    default:
      UNREACHABLE("VisitNullValueCall unknown builtin type.");
  }
}

void BytecodeGenerator::VisitSqlStringLikeCall(ast::CallExpr *call) {
  auto dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  auto input = VisitExpressionForSQLValue(call->Arguments()[0]);
  auto pattern = VisitExpressionForSQLValue(call->Arguments()[1]);
  GetEmitter()->Emit(Bytecode::Like, dest, input, pattern);
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinDateFunctionCall(ast::CallExpr *call, ast::Builtin builtin) {
  auto dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  auto input = VisitExpressionForSQLValue(call->Arguments()[0]);
  auto date_type =
      sql::DatePartType(call->Arguments()[1]->As<ast::CallExpr>()->Arguments()[0]->As<ast::LitExpr>()->Int64Val());

  switch (date_type) {
    case sql::DatePartType::YEAR:
      GetEmitter()->Emit(Bytecode::ExtractYearFromDate, dest, input);
      break;
    default:
      UNREACHABLE("Unimplemented DatePartType");
  }
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinTableIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The first argument to all calls is a pointer to the TVI
  LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::TableIterInit: {
      // The second argument should be the execution context
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      // The third argument is the table oid, which is integer-typed
      LocalVar table_oid = VisitExpressionForRValue(call->Arguments()[2]);
      // The fourth argument is the array of oids
      auto *arr_type = call->Arguments()[3]->GetType()->As<ast::ArrayType>();
      LocalVar arr = VisitExpressionForLValue(call->Arguments()[3]);
      // Emit the initialization codes
      GetEmitter()->EmitTableIterInit(Bytecode::TableVectorIteratorInit, iter, exec_ctx, table_oid, arr,
                                      static_cast<uint32_t>(arr_type->GetLength()));
      GetEmitter()->Emit(Bytecode::TableVectorIteratorPerformInit, iter);
      break;
    }
    case ast::Builtin::TableIterAdvance: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorNext, cond, iter);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableIterGetVPINumTuples: {
      LocalVar num_tuples_vpi = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorGetVPINumTuples, num_tuples_vpi, iter);
      GetExecutionResult()->SetDestination(num_tuples_vpi.ValueOf());
      break;
    }
    case ast::Builtin::TableIterGetVPI: {
      LocalVar vpi = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::TableVectorIteratorGetVPI, vpi, iter);
      GetExecutionResult()->SetDestination(vpi.ValueOf());
      break;
    }
    case ast::Builtin::TableIterClose: {
      GetEmitter()->Emit(Bytecode::TableVectorIteratorFree, iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTableIterParallelCall(ast::CallExpr *call) {
  // The first argument is the table oid, which is integer-typed.
  LocalVar table_oid = VisitExpressionForRValue(call->Arguments()[0]);
  // The second argument is the array of column oids.
  auto *arr_type = call->Arguments()[1]->GetType()->As<ast::ArrayType>();
  LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[1]);
  // The third argument is the query state.
  LocalVar query_state = VisitExpressionForRValue(call->Arguments()[2]);
  // The fourth argument should be the execution context.
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[3]);
  // The fifth argument is the scan function as an identifier.
  const auto scan_fn_name = call->Arguments()[4]->As<ast::IdentifierExpr>()->Name();
  // Emit the bytecode.
  GetEmitter()->EmitParallelTableScan(table_oid, col_oids, static_cast<uint32_t>(arr_type->GetLength()), query_state,
                                      exec_ctx, LookupFuncIdByName(scan_fn_name.GetData()));
}

void BytecodeGenerator::VisitBuiltinVPICall(ast::CallExpr *call, ast::Builtin builtin) {
  NOISEPAGE_ASSERT(call->GetType() != nullptr, "No return type set for call!");

  // The first argument to all calls is a pointer to the VPI
  LocalVar vpi = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::VPIInit: {
      LocalVar vector_projection = VisitExpressionForRValue(call->Arguments()[1]);
      if (call->Arguments().size() == 3) {
        LocalVar tid_list = VisitExpressionForRValue(call->Arguments()[2]);
        GetEmitter()->Emit(Bytecode::VPIInitWithList, vpi, vector_projection, tid_list);
      } else {
        GetEmitter()->Emit(Bytecode::VPIInit, vpi, vector_projection);
      }
      break;
    }
    case ast::Builtin::VPIFree: {
      GetEmitter()->Emit(Bytecode::VPIFree, vpi);
      break;
    }
    case ast::Builtin::VPIIsFiltered: {
      LocalVar is_filtered = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIIsFiltered, is_filtered, vpi);
      GetExecutionResult()->SetDestination(is_filtered.ValueOf());
      break;
    }
    case ast::Builtin::VPIGetSelectedRowCount: {
      LocalVar count = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIGetSelectedRowCount, count, vpi);
      GetExecutionResult()->SetDestination(count.ValueOf());
      break;
    }
    case ast::Builtin::VPIGetVectorProjection: {
      LocalVar vector_projection = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIGetVectorProjection, vector_projection, vpi);
      GetExecutionResult()->SetDestination(vector_projection.ValueOf());
      break;
    }
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIHasNextFiltered: {
      const Bytecode bytecode =
          builtin == ast::Builtin::VPIHasNext ? Bytecode::VPIHasNext : Bytecode::VPIHasNextFiltered;
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(bytecode, cond, vpi);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::VPIAdvance: {
      GetEmitter()->Emit(Bytecode::VPIAdvance, vpi);
      break;
    }
    case ast::Builtin::VPIAdvanceFiltered: {
      GetEmitter()->Emit(Bytecode::VPIAdvanceFiltered, vpi);
      break;
    }
    case ast::Builtin::VPISetPosition: {
      LocalVar index = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::VPISetPosition, vpi, index);
      break;
    }
    case ast::Builtin::VPISetPositionFiltered: {
      LocalVar index = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::VPISetPositionFiltered, vpi, index);
      break;
    }
    case ast::Builtin::VPIMatch: {
      LocalVar match = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::VPIMatch, vpi, match);
      break;
    }
    case ast::Builtin::VPIReset: {
      GetEmitter()->Emit(Bytecode::VPIReset, vpi);
      break;
    }
    case ast::Builtin::VPIResetFiltered: {
      GetEmitter()->Emit(Bytecode::VPIResetFiltered, vpi);
      break;
    }
    case ast::Builtin::VPIGetSlot: {
      LocalVar slot = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::VPIGetSlot, slot, vpi);
      GetExecutionResult()->SetDestination(slot.ValueOf());
      break;
    }

    case ast::Builtin::VPIGetPointer: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      const uint32_t col_idx = call->Arguments()[1]->As<ast::LitExpr>()->Int64Val();
      GetEmitter()->EmitVPIGet(Bytecode::VPIGetPointer, result, vpi, col_idx);
      break;
    }
#define GEN_CASE(BuiltinName, Bytecode)                                              \
  case ast::Builtin::BuiltinName: {                                                  \
    LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType()); \
    const uint32_t col_idx = call->Arguments()[1]->As<ast::LitExpr>()->Int64Val();   \
    GetEmitter()->EmitVPIGet(Bytecode, result, vpi, col_idx);                        \
    break;                                                                           \
  }                                                                                  \
  case ast::Builtin::BuiltinName##Null: {                                            \
    LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType()); \
    const uint32_t col_idx = call->Arguments()[1]->As<ast::LitExpr>()->Int64Val();   \
    GetEmitter()->EmitVPIGet(Bytecode##Null, result, vpi, col_idx);                  \
    break;                                                                           \
  }

      GEN_CASE(VPIGetBool, Bytecode::VPIGetBool);
      GEN_CASE(VPIGetTinyInt, Bytecode::VPIGetTinyInt);
      GEN_CASE(VPIGetSmallInt, Bytecode::VPIGetSmallInt);
      GEN_CASE(VPIGetInt, Bytecode::VPIGetInteger);
      GEN_CASE(VPIGetBigInt, Bytecode::VPIGetBigInt);
      GEN_CASE(VPIGetReal, Bytecode::VPIGetReal);
      GEN_CASE(VPIGetDouble, Bytecode::VPIGetDouble);
      GEN_CASE(VPIGetDate, Bytecode::VPIGetDate);
      GEN_CASE(VPIGetTimestamp, Bytecode::VPIGetTimestamp);
      GEN_CASE(VPIGetString, Bytecode::VPIGetString);
#undef GEN_CASE

#define GEN_CASE(BuiltinName, Bytecode)                                  \
  case ast::Builtin::BuiltinName: {                                      \
    auto input = VisitExpressionForSQLValue(call->Arguments()[1]);       \
    auto col_idx = call->Arguments()[2]->As<ast::LitExpr>()->Int64Val(); \
    GetEmitter()->EmitVPISet(Bytecode, vpi, input, col_idx);             \
    break;                                                               \
  }                                                                      \
  case ast::Builtin::BuiltinName##Null: {                                \
    auto input = VisitExpressionForSQLValue(call->Arguments()[1]);       \
    auto col_idx = call->Arguments()[2]->As<ast::LitExpr>()->Int64Val(); \
    GetEmitter()->EmitVPISet(Bytecode##Null, vpi, input, col_idx);       \
    break;                                                               \
  }

      GEN_CASE(VPISetBool, Bytecode::VPISetBool);
      GEN_CASE(VPISetTinyInt, Bytecode::VPISetTinyInt);
      GEN_CASE(VPISetSmallInt, Bytecode::VPISetSmallInt);
      GEN_CASE(VPISetInt, Bytecode::VPISetInteger);
      GEN_CASE(VPISetBigInt, Bytecode::VPISetBigInt);
      GEN_CASE(VPISetReal, Bytecode::VPISetReal);
      GEN_CASE(VPISetDouble, Bytecode::VPISetDouble);
      GEN_CASE(VPISetDate, Bytecode::VPISetDate);
      GEN_CASE(VPISetTimestamp, Bytecode::VPISetTimestamp);
      GEN_CASE(VPISetString, Bytecode::VPISetString);
#undef GEN_CASE

    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinHashCall(ast::CallExpr *call) {
  NOISEPAGE_ASSERT(call->GetType()->IsSpecificBuiltin(ast::BuiltinType::Uint64),
                   "Return type of @hash(...) expected to be 8-byte unsigned hash");
  NOISEPAGE_ASSERT(!call->Arguments().empty(), "@hash() must contain at least one input argument");
  NOISEPAGE_ASSERT(GetExecutionResult() != nullptr, "Caller of @hash() must use result");

  // The running hash value initialized to zero
  LocalVar hash_val = GetExecutionResult()->GetOrCreateDestination(call->GetType());

  GetEmitter()->EmitAssignImm8(hash_val, 0);

  for (uint32_t idx = 0; idx < call->NumArgs(); idx++) {
    NOISEPAGE_ASSERT(call->Arguments()[idx]->GetType()->IsSqlValueType(), "Input to hash must be a SQL value type");

    LocalVar input = VisitExpressionForSQLValue(call->Arguments()[idx]);
    const auto *type = call->Arguments()[idx]->GetType()->As<ast::BuiltinType>();
    switch (type->GetKind()) {
      case ast::BuiltinType::Integer:
        GetEmitter()->Emit(Bytecode::HashInt, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::Boolean:
        GetEmitter()->Emit(Bytecode::HashBool, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::Real:
        GetEmitter()->Emit(Bytecode::HashReal, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::StringVal:
        GetEmitter()->Emit(Bytecode::HashString, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::Date:
        GetEmitter()->Emit(Bytecode::HashDate, hash_val, input, hash_val.ValueOf());
        break;
      case ast::BuiltinType::Timestamp:
        GetEmitter()->Emit(Bytecode::HashTimestamp, hash_val, input, hash_val.ValueOf());
      default:
        UNREACHABLE("Hashing this type isn't supported!");
    }
  }

  // Set return
  GetExecutionResult()->SetDestination(hash_val.ValueOf());
}

void BytecodeGenerator::VisitBuiltinFilterManagerCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar filter_manager = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::FilterManagerInit: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::FilterManagerInit, filter_manager, exec_ctx);
      break;
    }
    case ast::Builtin::FilterManagerInsertFilter: {
      GetEmitter()->Emit(Bytecode::FilterManagerStartNewClause, filter_manager);

      // Insert all flavors
      for (uint32_t arg_idx = 1; arg_idx < call->NumArgs(); arg_idx++) {
        const std::string func_name = call->Arguments()[arg_idx]->As<ast::IdentifierExpr>()->Name().GetData();
        const FunctionId func_id = LookupFuncIdByName(func_name);
        GetEmitter()->EmitFilterManagerInsertFilter(filter_manager, func_id);
      }
      break;
    }
    case ast::Builtin::FilterManagerRunFilters: {
      LocalVar vpi = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::FilterManagerRunFilters, filter_manager, vpi, exec_ctx);
      break;
    }
    case ast::Builtin::FilterManagerFree: {
      GetEmitter()->Emit(Bytecode::FilterManagerFree, filter_manager);
      break;
    }
    default: {
      UNREACHABLE("Impossible filter manager call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinVectorFilterCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  LocalVar vector_projection = VisitExpressionForRValue(call->Arguments()[1]);
  LocalVar tid_list = VisitExpressionForRValue(call->Arguments()[4]);

#define GEN_CASE(BYTECODE)                                                                         \
  LocalVar left_col = VisitExpressionForRValue(call->Arguments()[2]);                              \
  if (!call->Arguments()[3]->GetType()->IsIntegerType()) {                                         \
    LocalVar right_val = VisitExpressionForSQLValue(call->Arguments()[3]);                         \
    GetEmitter()->Emit(BYTECODE##Val, exec_ctx, vector_projection, left_col, right_val, tid_list); \
  } else {                                                                                         \
    LocalVar right_col = VisitExpressionForRValue(call->Arguments()[3]);                           \
    GetEmitter()->Emit(BYTECODE, exec_ctx, vector_projection, left_col, right_col, tid_list);      \
  }

  switch (builtin) {
    case ast::Builtin::VectorFilterEqual: {
      GEN_CASE(Bytecode::VectorFilterEqual);
      break;
    }
    case ast::Builtin::VectorFilterGreaterThan: {
      GEN_CASE(Bytecode::VectorFilterGreaterThan);
      break;
    }
    case ast::Builtin::VectorFilterGreaterThanEqual: {
      GEN_CASE(Bytecode::VectorFilterGreaterThanEqual);
      break;
    }
    case ast::Builtin::VectorFilterLessThan: {
      GEN_CASE(Bytecode::VectorFilterLessThan);
      break;
    }
    case ast::Builtin::VectorFilterLessThanEqual: {
      GEN_CASE(Bytecode::VectorFilterLessThanEqual);
      break;
    }
    case ast::Builtin::VectorFilterNotEqual: {
      GEN_CASE(Bytecode::VectorFilterNotEqual);
      break;
    }
    case ast::Builtin::VectorFilterLike: {
      GEN_CASE(Bytecode::VectorFilterLike);
      break;
    }
    case ast::Builtin::VectorFilterNotLike: {
      GEN_CASE(Bytecode::VectorFilterNotLike);
      break;
    }
    default: {
      UNREACHABLE("Impossible vector filter executor call");
    }
  }
#undef GEN_CASE
}

void BytecodeGenerator::VisitBuiltinAggHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::AggHashTableInit: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableInit, agg_ht, exec_ctx, entry_size);
      break;
    }
    case ast::Builtin::AggHashTableGetTupleCount: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableGetTupleCount, dest, agg_ht);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableGetInsertCount: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableGetInsertCount, dest, agg_ht);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[1]);
      Bytecode bytecode = Bytecode::AggregationHashTableAllocTuple;
      if (call->Arguments().size() > 2) {
        NOISEPAGE_ASSERT(call->Arguments()[2]->IsBoolLiteral(), "Last argument must be a boolean literal");
        const bool partitioned = call->Arguments()[2]->As<ast::LitExpr>()->BoolVal();
        bytecode = partitioned ? Bytecode::AggregationHashTableAllocTuplePartitioned
                               : Bytecode::AggregationHashTableAllocTuple;
      }
      GetEmitter()->Emit(bytecode, dest, agg_ht, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableLinkEntry: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar entry = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableLinkHashTableEntry, agg_ht, entry);
      break;
    }
    case ast::Builtin::AggHashTableLookup: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[1]);
      auto key_eq_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().GetData());
      LocalVar arg = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->EmitAggHashTableLookup(dest, agg_ht, hash, key_eq_fn, arg);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableProcessBatch: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar vpi = VisitExpressionForRValue(call->Arguments()[1]);
      uint32_t num_keys = call->Arguments()[2]->GetType()->As<ast::ArrayType>()->GetLength();
      LocalVar key_cols = VisitExpressionForLValue(call->Arguments()[2]);
      auto init_agg_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().GetData());
      auto merge_agg_fn = LookupFuncIdByName(call->Arguments()[4]->As<ast::IdentifierExpr>()->Name().GetData());
      LocalVar partitioned = VisitExpressionForRValue(call->Arguments()[5]);
      GetEmitter()->EmitAggHashTableProcessBatch(agg_ht, vpi, num_keys, key_cols, init_agg_fn, merge_agg_fn,
                                                 partitioned);
      break;
    }
    case ast::Builtin::AggHashTableMovePartitions: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar aht_offset = VisitExpressionForRValue(call->Arguments()[2]);
      auto merge_part_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().GetData());
      GetEmitter()->EmitAggHashTableMovePartitions(agg_ht, tls, aht_offset, merge_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableParallelPartitionedScan: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar ctx = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[2]);
      auto scan_part_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().GetData());
      GetEmitter()->EmitAggHashTableParallelPartitionedScan(agg_ht, ctx, tls, scan_part_fn);
      break;
    }
    case ast::Builtin::AggHashTableFree: {
      LocalVar agg_ht = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableFree, agg_ht);
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
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorInit, agg_ht_iter, agg_ht);
      break;
    }
    case ast::Builtin::AggHashTableIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorHasNext, has_more, agg_ht_iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterNext: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorNext, agg_ht_iter);
      break;
    }
    case ast::Builtin::AggHashTableIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorGetRow, row_ptr, agg_ht_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::AggHashTableIterClose: {
      LocalVar agg_ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationHashTableIteratorFree, agg_ht_iter);
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
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorHasNext, has_more, iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterNext: {
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorNext, iter);
      break;
    }
    case ast::Builtin::AggPartIterGetRow: {
      LocalVar row = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRow, row, iter);
      GetExecutionResult()->SetDestination(row.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetRowEntry: {
      LocalVar entry = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetRowEntry, entry, iter);
      GetExecutionResult()->SetDestination(entry.ValueOf());
      break;
    }
    case ast::Builtin::AggPartIterGetHash: {
      LocalVar hash = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregationOverflowPartitionIteratorGetHash, hash, iter);
      GetExecutionResult()->SetDestination(hash.ValueOf());
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregation partition iterator bytecode");
    }
  }
}

namespace {

// All aggregate types and bytecodes. Aggregates implement a common interface. Thus, their bytecode
// can be generated by only knowing the type of aggregate.
// Format: Type, Init Op, Advance Op, Result Op, Merge Op, Reset Op, Free Op
#define AGG_CODES(F)                                                                                                   \
  /* COUNT(col) */                                                                                                     \
  F(CountAggregate, CountAggregateInit, CountAggregateAdvance, CountAggregateGetResult, CountAggregateMerge,           \
    CountAggregateReset, CountAggregateFree)                                                                           \
  /* COUNT(*) */                                                                                                       \
  F(CountStarAggregate, CountStarAggregateInit, CountStarAggregateAdvance, CountStarAggregateGetResult,                \
    CountStarAggregateMerge, CountStarAggregateReset, CountStarAggregateFree)                                          \
  /* AVG(col) */                                                                                                       \
  F(AvgAggregate, AvgAggregateInit, AvgAggregateAdvanceInteger, AvgAggregateGetResult, AvgAggregateMerge,              \
    AvgAggregateReset, AvgAggregateFree)                                                                               \
  /* MAX(int_col) */                                                                                                   \
  F(IntegerMaxAggregate, IntegerMaxAggregateInit, IntegerMaxAggregateAdvance, IntegerMaxAggregateGetResult,            \
    IntegerMaxAggregateMerge, IntegerMaxAggregateReset, IntegerMaxAggregateFree)                                       \
  /* MIN(int_col) */                                                                                                   \
  F(IntegerMinAggregate, IntegerMinAggregateInit, IntegerMinAggregateAdvance, IntegerMinAggregateGetResult,            \
    IntegerMinAggregateMerge, IntegerMinAggregateReset, IntegerMinAggregateFree)                                       \
  /* SUM(int_col) */                                                                                                   \
  F(IntegerSumAggregate, IntegerSumAggregateInit, IntegerSumAggregateAdvance, IntegerSumAggregateGetResult,            \
    IntegerSumAggregateMerge, IntegerSumAggregateReset, IntegerSumAggregateFree)                                       \
  /* MAX(real_col) */                                                                                                  \
  F(RealMaxAggregate, RealMaxAggregateInit, RealMaxAggregateAdvance, RealMaxAggregateGetResult, RealMaxAggregateMerge, \
    RealMaxAggregateReset, RealMaxAggregateFree)                                                                       \
  /* MIN(real_col) */                                                                                                  \
  F(RealMinAggregate, RealMinAggregateInit, RealMinAggregateAdvance, RealMinAggregateGetResult, RealMinAggregateMerge, \
    RealMinAggregateReset, RealMinAggregateFree)                                                                       \
  /* SUM(real_col) */                                                                                                  \
  F(RealSumAggregate, RealSumAggregateInit, RealSumAggregateAdvance, RealSumAggregateGetResult, RealSumAggregateMerge, \
    RealSumAggregateReset, RealSumAggregateFree)                                                                       \
  /* MAX(date_col) */                                                                                                  \
  F(DateMaxAggregate, DateMaxAggregateInit, DateMaxAggregateAdvance, DateMaxAggregateGetResult, DateMaxAggregateMerge, \
    DateMaxAggregateReset, DateMaxAggregateFree)                                                                       \
  /* MIN(date_col) */                                                                                                  \
  F(DateMinAggregate, DateMinAggregateInit, DateMinAggregateAdvance, DateMinAggregateGetResult, DateMinAggregateMerge, \
    DateMinAggregateReset, DateMinAggregateFree)                                                                       \
  /* MAX(string_col) */                                                                                                \
  F(StringMaxAggregate, StringMaxAggregateInit, StringMaxAggregateAdvance, StringMaxAggregateGetResult,                \
    StringMaxAggregateMerge, StringMaxAggregateReset, StringMaxAggregateFree)                                          \
  /* MIN(string_col) */                                                                                                \
  F(StringMinAggregate, StringMinAggregateInit, StringMinAggregateAdvance, StringMinAggregateGetResult,                \
    StringMinAggregateMerge, StringMinAggregateReset, StringMinAggregateFree)

enum class AggOpKind : uint8_t { Init = 0, Advance = 1, GetResult = 2, Merge = 3, Reset = 4, Free = 5 };

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
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
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
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
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
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
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
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
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
#define ENTRY(Type, Init, Advance, GetResult, Merge, Reset, Free) \
  case ast::BuiltinType::Type:                                    \
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
      for (const auto &arg : call->Arguments()) {
        const auto agg_kind = arg->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
        LocalVar input = VisitExpressionForRValue(arg);
        Bytecode bytecode;
        if (builtin == ast::Builtin::AggInit) {
          bytecode = OpForAgg<AggOpKind::Init>(agg_kind);
        } else {
          bytecode = OpForAgg<AggOpKind::Reset>(agg_kind);
        }
        GetEmitter()->Emit(bytecode, input);
      }
      break;
    }
    case ast::Builtin::AggAdvance: {
      const auto &args = call->Arguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar agg = VisitExpressionForRValue(args[0]);
      LocalVar input = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Advance>(agg_kind);

      // Hack to handle advancing AvgAggregates with float/double precision numbers. The default
      // behavior in OpForAgg() is to use AvgAggregateAdvanceInteger.
      if (agg_kind == ast::BuiltinType::AvgAggregate &&
          args[1]->GetType()->GetPointeeType()->IsSpecificBuiltin(ast::BuiltinType::Real)) {
        bytecode = Bytecode::AvgAggregateAdvanceReal;
      }

      GetEmitter()->Emit(bytecode, agg, input);
      break;
    }
    case ast::Builtin::AggMerge: {
      const auto &args = call->Arguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar agg_1 = VisitExpressionForRValue(args[0]);
      LocalVar agg_2 = VisitExpressionForRValue(args[1]);
      Bytecode bytecode = OpForAgg<AggOpKind::Merge>(agg_kind);
      GetEmitter()->Emit(bytecode, agg_1, agg_2);
      break;
    }
    case ast::Builtin::AggResult: {
      const auto &args = call->Arguments();
      const auto agg_kind = args[0]->GetType()->GetPointeeType()->As<ast::BuiltinType>()->GetKind();
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar agg = VisitExpressionForRValue(args[0]);
      Bytecode bytecode = OpForAgg<AggOpKind::GetResult>(agg_kind);
      GetEmitter()->Emit(bytecode, result, agg);
      break;
    }
    default: {
      UNREACHABLE("Impossible aggregator call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinJoinHashTableCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The join hash table is always the first argument to all JHT calls
  LocalVar join_hash_table = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::JoinHashTableInit: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableInit, join_hash_table, exec_ctx, entry_size);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::JoinHashTableAllocTuple, dest, join_hash_table, hash);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableGetTupleCount: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::JoinHashTableGetTupleCount, dest, join_hash_table);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      GetEmitter()->Emit(Bytecode::JoinHashTableBuild, join_hash_table);
      break;
    }
    case ast::Builtin::JoinHashTableBuildParallel: {
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar jht_offset = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableBuildParallel, join_hash_table, tls, jht_offset);
      break;
    }
    case ast::Builtin::JoinHashTableLookup: {
      LocalVar ht_entry_iter = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar hash = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::JoinHashTableLookup, join_hash_table, ht_entry_iter, hash);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      GetEmitter()->Emit(Bytecode::JoinHashTableFree, join_hash_table);
      break;
    }
    default: {
      UNREACHABLE("Impossible join hash table call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinHashTableEntryIteratorCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The hash table entry iterator is always the first argument to all calls
  LocalVar ht_entry_iter = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::HashTableEntryIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::HashTableEntryIteratorHasNext, has_more, ht_entry_iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::HashTableEntryIterGetRow: {
      LocalVar row = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::HashTableEntryIteratorGetRow, row, ht_entry_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible hash table entry iterator call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinJoinHashTableIteratorCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::JoinHashTableIterInit: {
      LocalVar ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar ht = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::JoinHashTableIteratorInit, ht_iter, ht);
      break;
    }
    case ast::Builtin::JoinHashTableIterHasNext: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::JoinHashTableIteratorHasNext, has_more, ht_iter);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableIterNext: {
      LocalVar ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::JoinHashTableIteratorNext, ht_iter);
      break;
    }
    case ast::Builtin::JoinHashTableIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::JoinHashTableIteratorGetRow, row_ptr, ht_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::JoinHashTableIterFree: {
      LocalVar ht_iter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::JoinHashTableIteratorFree, ht_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible hash table naive iteration bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinSorterCall(ast::CallExpr *call, ast::Builtin builtin) {
  switch (builtin) {
    case ast::Builtin::SorterInit: {
      // TODO(pmenon): Fix me so that the comparison function doesn't have be
      // listed by name.
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[1]);
      const std::string cmp_func_name = call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().GetData();
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->EmitSorterInit(Bytecode::SorterInit, sorter, exec_ctx, LookupFuncIdByName(cmp_func_name),
                                   entry_size);
      break;
    }
    case ast::Builtin::SorterGetTupleCount: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterGetTupleCount, dest, sorter);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::SorterInsert: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterAllocTuple, dest, sorter);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::SorterInsertTopK: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterAllocTupleTopK, dest, sorter, top_k);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::SorterInsertTopKFinish: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar top_k = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterAllocTupleTopKFinish, sorter, top_k);
      break;
    }
    case ast::Builtin::SorterSort: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterSort, sorter);
      break;
    }
    case ast::Builtin::SorterSortParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::SorterSortParallel, sorter, tls, sorter_offset);
      break;
    }
    case ast::Builtin::SorterSortTopKParallel: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar tls = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar sorter_offset = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar top_k = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->Emit(Bytecode::SorterSortTopKParallel, sorter, tls, sorter_offset, top_k);
      break;
    }
    case ast::Builtin::SorterFree: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::SorterFree, sorter);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitBuiltinSorterIterCall(ast::CallExpr *call, ast::Builtin builtin) {
  // The first argument to all calls is the sorter iterator instance
  const LocalVar sorter_iter = VisitExpressionForRValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::SorterIterInit: {
      LocalVar sorter = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterIteratorInit, sorter_iter, sorter);
      break;
    }
    case ast::Builtin::SorterIterHasNext: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::SorterIteratorHasNext, cond, sorter_iter);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterNext: {
      GetEmitter()->Emit(Bytecode::SorterIteratorNext, sorter_iter);
      break;
    }
    case ast::Builtin::SorterIterSkipRows: {
      LocalVar n = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::SorterIteratorSkipRows, sorter_iter, n);
      break;
    }
    case ast::Builtin::SorterIterGetRow: {
      LocalVar row_ptr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::SorterIteratorGetRow, row_ptr, sorter_iter);
      GetExecutionResult()->SetDestination(row_ptr.ValueOf());
      break;
    }
    case ast::Builtin::SorterIterClose: {
      GetEmitter()->Emit(Bytecode::SorterIteratorFree, sorter_iter);
      break;
    }
    default: {
      UNREACHABLE("Impossible table iteration call");
    }
  }
}

void BytecodeGenerator::VisitResultBufferCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar input = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::ResultBufferNew: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ResultBufferNew, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::ResultBufferAllocOutRow: {
      LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ResultBufferAllocOutputRow, dest, input);
      GetExecutionResult()->SetDestination(dest.ValueOf());
      break;
    }
    case ast::Builtin::ResultBufferFinalize: {
      GetEmitter()->Emit(Bytecode::ResultBufferFinalize, input);
      break;
    }
    case ast::Builtin::ResultBufferFree: {
      GetEmitter()->Emit(Bytecode::ResultBufferFree, input);
      break;
    }
    default: {
      UNREACHABLE("Invalid result buffer call!");
    }
  }
}

#if 0
void BytecodeGenerator::VisitCSVReaderCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar reader = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::CSVReaderInit: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      NOISEPAGE_ASSERT(call->Arguments()[1]->IsLitExpr(), "Second argument expected to be string literal");
      auto string_lit = call->Arguments()[1]->As<ast::LitExpr>()->StringVal();
      auto file_name = NewStaticString(call->GetType()->GetContext(), string_lit);
      GetEmitter()->EmitCSVReaderInit(reader, file_name, string_lit.GetLength());
      GetEmitter()->Emit(Bytecode::CSVReaderPerformInit, result, reader);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::CSVReaderAdvance: {
      LocalVar has_more = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::CSVReaderAdvance, has_more, reader);
      GetExecutionResult()->SetDestination(has_more.ValueOf());
      break;
    }
    case ast::Builtin::CSVReaderGetField: {
      LocalVar field_index = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar field = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::CSVReaderGetField, reader, field_index, field);
      break;
    }
    case ast::Builtin::CSVReaderGetRecordNumber: {
      LocalVar record_number = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::CSVReaderGetRecordNumber, record_number, reader);
      break;
    }
    case ast::Builtin::CSVReaderClose: {
      GetEmitter()->Emit(Bytecode::CSVReaderClose, reader);
      break;
    }
    default: {
      UNREACHABLE("Invalid CSV reader call!");
    }
  }
}
#endif

void BytecodeGenerator::VisitExecutionContextCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  switch (builtin) {
    case ast::Builtin::ExecutionContextAddRowsAffected: {
      auto rows_affected = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecutionContextAddRowsAffected, exec_ctx, rows_affected);
      break;
    }
    case ast::Builtin::ExecutionContextRegisterHook: {
      auto hook_idx = VisitExpressionForRValue(call->Arguments()[1]);
      const auto hook_fn_name = call->Arguments()[2]->As<ast::IdentifierExpr>()->Name();
      GetEmitter()->EmitRegisterHook(exec_ctx, hook_idx, LookupFuncIdByName(hook_fn_name.GetData()));
      break;
    }
    case ast::Builtin::ExecutionContextClearHooks: {
      GetEmitter()->Emit(Bytecode::ExecutionContextClearHooks, exec_ctx);
      break;
    }
    case ast::Builtin::ExecutionContextInitHooks: {
      auto num_hooks = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecutionContextInitHooks, exec_ctx, num_hooks);
      break;
    }
    case ast::Builtin::ExecutionContextStartResourceTracker: {
      LocalVar metrics_component = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecutionContextStartResourceTracker, exec_ctx, metrics_component);
      break;
    }
    case ast::Builtin::ExecutionContextSetMemoryUseOverride: {
      LocalVar use = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecutionContextSetMemoryUseOverride, exec_ctx, use);
      break;
    }
    case ast::Builtin::ExecutionContextEndResourceTracker: {
      LocalVar name = VisitExpressionForLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecutionContextEndResourceTracker, exec_ctx, name);
      break;
    }
    case ast::Builtin::ExecutionContextStartPipelineTracker: {
      LocalVar pipeline_id = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecutionContextStartPipelineTracker, exec_ctx, pipeline_id);
      break;
    }
    case ast::Builtin::ExecutionContextEndPipelineTracker: {
      LocalVar query_id = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar pipeline_id = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar ouvec = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->Emit(Bytecode::ExecutionContextEndPipelineTracker, exec_ctx, query_id, pipeline_id, ouvec);
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorInitialize: {
      LocalVar ouvector = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar pipeline_id = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar is_parallel = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->Emit(Bytecode::ExecOUFeatureVectorInitialize, exec_ctx, ouvector, pipeline_id, is_parallel);
      break;
    }
    case ast::Builtin::ExecutionContextGetMemoryPool: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ExecutionContextGetMemoryPool, result, exec_ctx);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::ExecutionContextGetTLS: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ExecutionContextGetTLS, result, exec_ctx);
      GetExecutionResult()->SetDestination(result.ValueOf());
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
    case ast::Builtin::ThreadStateContainerGetState: {
      LocalVar result = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::ThreadStateContainerAccessCurrentThreadState, result, tls);
      GetExecutionResult()->SetDestination(result.ValueOf());
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate: {
      LocalVar ctx = VisitExpressionForRValue(call->Arguments()[1]);
      FunctionId iterate_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().GetData());
      GetEmitter()->EmitThreadStateContainerIterate(tls, ctx, iterate_fn);
      break;
    }
    case ast::Builtin::ThreadStateContainerReset: {
      LocalVar entry_size = VisitExpressionForRValue(call->Arguments()[1]);
      FunctionId init_fn = LookupFuncIdByName(call->Arguments()[2]->As<ast::IdentifierExpr>()->Name().GetData());
      FunctionId destroy_fn = LookupFuncIdByName(call->Arguments()[3]->As<ast::IdentifierExpr>()->Name().GetData());
      LocalVar ctx = VisitExpressionForRValue(call->Arguments()[4]);
      GetEmitter()->EmitThreadStateContainerReset(tls, entry_size, init_fn, destroy_fn, ctx);
      break;
    }
    case ast::Builtin::ThreadStateContainerClear: {
      GetEmitter()->Emit(Bytecode::ThreadStateContainerClear, tls);
      break;
    }
    default: {
      UNREACHABLE("Impossible thread state container call");
    }
  }
}

void BytecodeGenerator::VisitBuiltinTrigCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  LocalVar src = VisitExpressionForSQLValue(call->Arguments()[0]);

  switch (builtin) {
    case ast::Builtin::ACos: {
      GetEmitter()->Emit(Bytecode::Acos, dest, src);
      break;
    }
    case ast::Builtin::ASin: {
      GetEmitter()->Emit(Bytecode::Asin, dest, src);
      break;
    }
    case ast::Builtin::ATan: {
      GetEmitter()->Emit(Bytecode::Atan, dest, src);
      break;
    }
    case ast::Builtin::Cosh: {
      GetEmitter()->Emit(Bytecode::Cosh, dest, src);
      break;
    }
    case ast::Builtin::Sinh: {
      GetEmitter()->Emit(Bytecode::Sinh, dest, src);
      break;
    }
    case ast::Builtin::Tanh: {
      GetEmitter()->Emit(Bytecode::Tanh, dest, src);
      break;
    }
    case ast::Builtin::ATan2: {
      LocalVar src2 = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Atan2, dest, src, src2);
      break;
    }
    case ast::Builtin::Cos: {
      GetEmitter()->Emit(Bytecode::Cos, dest, src);
      break;
    }
    case ast::Builtin::Cot: {
      GetEmitter()->Emit(Bytecode::Cot, dest, src);
      break;
    }
    case ast::Builtin::Sin: {
      GetEmitter()->Emit(Bytecode::Sin, dest, src);
      break;
    }
    case ast::Builtin::Tan: {
      GetEmitter()->Emit(Bytecode::Tan, dest, src);
      break;
    }
    case ast::Builtin::Ceil: {
      GetEmitter()->Emit(Bytecode::Ceil, dest, src);
      break;
    }
    case ast::Builtin::Floor: {
      GetEmitter()->Emit(Bytecode::Floor, dest, src);
      break;
    }
    case ast::Builtin::Truncate: {
      GetEmitter()->Emit(Bytecode::Truncate, dest, src);
      break;
    }
    case ast::Builtin::Log10: {
      GetEmitter()->Emit(Bytecode::Log10, dest, src);
      break;
    }
    case ast::Builtin::Log2: {
      GetEmitter()->Emit(Bytecode::Log2, dest, src);
      break;
    }
    case ast::Builtin::Exp: {
      src = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Exp, dest, src);
      break;
    }
    case ast::Builtin::Sqrt: {
      GetEmitter()->Emit(Bytecode::Sqrt, dest, src);
      break;
    }
    case ast::Builtin::Cbrt: {
      GetEmitter()->Emit(Bytecode::Cbrt, dest, src);
      break;
    }
    case ast::Builtin::Round: {
      GetEmitter()->Emit(Bytecode::Round, dest, src);
      break;
    }
    case ast::Builtin::Round2: {
      LocalVar src2 = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Round2, dest, src, src2);
      break;
    }
    case ast::Builtin::Pow: {
      LocalVar src2 = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Pow, dest, src, src2);
      break;
    }
    default: {
      UNREACHABLE("Impossible trigonometric bytecode");
    }
  }

  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinArithmeticCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  const bool is_integer_math = call->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer);

  switch (builtin) {
    case ast::Builtin::Abs: {
      LocalVar src = VisitExpressionForSQLValue(call->Arguments()[0]);
      GetEmitter()->Emit(is_integer_math ? Bytecode::AbsInteger : Bytecode::AbsReal, dest, src);
      break;
    }
    case ast::Builtin::Mod: {
      LocalVar first_input = VisitExpressionForSQLValue(call->Arguments()[0]);
      LocalVar second_input = VisitExpressionForSQLValue(call->Arguments()[1]);
      if (!is_integer_math) {
        NOISEPAGE_ASSERT(call->Arguments()[0]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real) &&
                             call->Arguments()[1]->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real),
                         "Inputs must both be of type Real");
      }
      GetEmitter()->Emit(is_integer_math ? Bytecode::ModInteger : Bytecode::ModReal, dest, first_input, second_input);
      break;
    }
    default:
      UNREACHABLE("Unimplemented arithmetic function!");
  }
  GetExecutionResult()->SetDestination(dest);
}

void BytecodeGenerator::VisitBuiltinAtomicArithmeticCall(ast::CallExpr *call, ast::Builtin builtin) {
  const auto &args = call->Arguments();
  LocalVar dest = VisitExpressionForRValue(args[0]);
  LocalVar val = VisitExpressionForRValue(args[1]);
  LocalVar ret;

  if (GetExecutionResult() != nullptr) {
    ret = GetExecutionResult()->GetOrCreateDestination(call->GetType());
    GetExecutionResult()->SetDestination(ret.ValueOf());
  } else {
    ret = GetCurrentFunction()->NewLocal(call->GetType());
  }

  auto operand_size = args[1]->GetType()->GetSize();  // Base operand size
  Bytecode op_code;
  if (operand_size == 1) {
    op_code = builtin == ast::Builtin::AtomicAnd ? Bytecode::AtomicAnd1 : Bytecode::AtomicOr1;
  } else if (operand_size == 2) {
    op_code = builtin == ast::Builtin::AtomicAnd ? Bytecode::AtomicAnd2 : Bytecode::AtomicOr2;
  } else if (operand_size == 4) {
    op_code = builtin == ast::Builtin::AtomicAnd ? Bytecode::AtomicAnd4 : Bytecode::AtomicOr4;
  } else {
    NOISEPAGE_ASSERT(operand_size == 8, "Unexpected integral size");
    op_code = builtin == ast::Builtin::AtomicAnd ? Bytecode::AtomicAnd8 : Bytecode::AtomicOr8;
  }
  GetEmitter()->Emit(op_code, ret, dest, val);
}

void BytecodeGenerator::VisitBuiltinAtomicCompareExchangeCall(ast::CallExpr *call) {
  const auto &args = call->Arguments();
  LocalVar dest = VisitExpressionForRValue(args[0]);
  LocalVar expected = VisitExpressionForRValue(args[1]);
  LocalVar desired = VisitExpressionForRValue(args[2]);
  LocalVar ret;

  if (GetExecutionResult() != nullptr) {
    ret = GetExecutionResult()->GetOrCreateDestination(call->GetType());
    GetExecutionResult()->SetDestination(ret.ValueOf());
  } else {
    ret = GetCurrentFunction()->NewLocal(call->GetType());
  }

  auto operand_size = args[2]->GetType()->GetSize();  // Base operand size
  if (operand_size == 1) {
    GetEmitter()->Emit(Bytecode::AtomicCompareExchange1, ret, dest, expected, desired);
  } else if (operand_size == 2) {
    GetEmitter()->Emit(Bytecode::AtomicCompareExchange2, ret, dest, expected, desired);
  } else if (operand_size == 4) {
    GetEmitter()->Emit(Bytecode::AtomicCompareExchange4, ret, dest, expected, desired);
  } else {
    NOISEPAGE_ASSERT(operand_size == 8, "Unexpected type size");
    GetEmitter()->Emit(Bytecode::AtomicCompareExchange8, ret, dest, expected, desired);
  }
}

void BytecodeGenerator::VisitBuiltinSizeOfCall(ast::CallExpr *call) {
  ast::Type *target_type = call->Arguments()[0]->GetType();
  LocalVar size_var = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  GetEmitter()->EmitAssignImm4(size_var, target_type->GetSize());
  GetExecutionResult()->SetDestination(size_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinOffsetOfCall(ast::CallExpr *call) {
  auto composite_type = call->Arguments()[0]->GetType()->As<ast::StructType>();
  auto field_name = call->Arguments()[1]->As<ast::IdentifierExpr>();
  const uint32_t offset = composite_type->GetOffsetOfFieldByName(field_name->Name());
  LocalVar offset_var = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  GetEmitter()->EmitAssignImm4(offset_var, offset);
  GetExecutionResult()->SetDestination(offset_var.ValueOf());
}

void BytecodeGenerator::VisitAbortTxn(ast::CallExpr *call) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  GetEmitter()->EmitAbortTxn(Bytecode::AbortTxn, exec_ctx);
}

void BytecodeGenerator::VisitBuiltinTestCatalogLookup(ast::CallExpr *call) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  auto table_name_lit = call->Arguments()[1]->As<ast::LitExpr>()->StringVal();
  LocalVar table_name = NewStaticString(call->GetType()->GetContext(), table_name_lit);
  uint32_t table_name_len = table_name_lit.GetLength();
  auto col_name_lit = call->Arguments()[2]->As<ast::LitExpr>()->StringVal();
  LocalVar col_name = NewStaticString(call->GetType()->GetContext(), col_name_lit);
  uint32_t col_name_len = col_name_lit.GetLength();

  LocalVar oid_var = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  GetEmitter()->EmitTestCatalogLookup(oid_var, exec_ctx, table_name, table_name_len, col_name, col_name_len);
  GetExecutionResult()->SetDestination(oid_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinTestCatalogIndexLookup(ast::CallExpr *call) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  auto table_name_lit = call->Arguments()[1]->As<ast::LitExpr>()->StringVal();
  LocalVar table_name = NewStaticString(call->GetType()->GetContext(), table_name_lit);
  uint32_t table_name_len = table_name_lit.GetLength();

  LocalVar oid_var = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  GetEmitter()->EmitTestCatalogIndexLookup(oid_var, exec_ctx, table_name, table_name_len);
  GetExecutionResult()->SetDestination(oid_var.ValueOf());
}

void BytecodeGenerator::VisitBuiltinPRCall(ast::CallExpr *call, ast::Builtin builtin) {
  // First argument is always a projected row
  LocalVar pr = VisitExpressionForRValue(call->Arguments()[0]);
  ast::Context *ctx = call->GetType()->GetContext();
  switch (builtin) {
    case ast::Builtin::PRSetBool: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetBool, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTinyInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetTinyInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetSmallInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetSmallInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetBigInt: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetBigInt, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetReal: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetReal, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDouble: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetDouble, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDate: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetDateVal, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTimestamp: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetTimestampVal, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetVarlen: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      LocalVar own = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->EmitPRSetVarlen(Bytecode::PRSetVarlen, pr, col_idx, val, own);
      break;
    }
    case ast::Builtin::PRSetBoolNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetBoolNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTinyIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetTinyIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetSmallIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetSmallIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetBigIntNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetBigIntNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetRealNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetRealNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDoubleNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetDoubleNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetDateNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetDateValNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetTimestampNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      GetEmitter()->EmitPRSet(Bytecode::PRSetTimestampValNull, pr, col_idx, val);
      break;
    }
    case ast::Builtin::PRSetVarlenNull: {
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      LocalVar val = VisitExpressionForLValue(call->Arguments()[2]);
      LocalVar own = VisitExpressionForRValue(call->Arguments()[3]);
      GetEmitter()->EmitPRSetVarlen(Bytecode::PRSetVarlenNull, pr, col_idx, val, own);
      break;
    }
    case ast::Builtin::PRGetBool: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetBool, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTinyInt: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetTinyInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetSmallInt: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetSmallInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetInt: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetBigInt: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetBigInt, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetReal: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetReal, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDouble: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetDouble, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDate: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetDateVal, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTimestamp: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Timestamp));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetTimestampVal, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetVarlen: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetVarlen, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetBoolNull: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Boolean));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetBoolNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTinyIntNull: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetTinyIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetSmallIntNull: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetSmallIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetIntNull: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetBigIntNull: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Integer));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetBigIntNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetRealNull: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetRealNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDoubleNull: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Real));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetDoubleNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetDateNull: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetDateValNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetTimestampNull: {
      LocalVar val = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Date));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetTimestampValNull, val, pr, col_idx);
      break;
    }
    case ast::Builtin::PRGetVarlenNull: {
      LocalVar val =
          GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::StringVal));
      auto col_idx = static_cast<uint16_t>(call->Arguments()[1]->As<ast::LitExpr>()->Int64Val());
      GetEmitter()->EmitPRGet(Bytecode::PRGetVarlenNull, val, pr, col_idx);
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
      auto table_oid = VisitExpressionForRValue(call->Arguments()[2]);
      auto *arr_type = call->Arguments()[3]->GetType()->As<ast::ArrayType>();
      auto num_oids = static_cast<uint32_t>(arr_type->GetLength());
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[3]);
      LocalVar is_index_key_update = VisitExpressionForRValue(call->Arguments()[4]);
      GetEmitter()->EmitStorageInterfaceInit(Bytecode::StorageInterfaceInit, storage_interface, exec_ctx, table_oid,
                                             col_oids, num_oids, is_index_key_update);
      break;
    }
    case ast::Builtin::GetTablePR: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::StorageInterfaceGetTablePR, pr, storage_interface);
      break;
    }
    case ast::Builtin::TableInsert: {
      ast::Type *tuple_slot_type = ast::BuiltinType::Get(ctx, ast::BuiltinType::TupleSlot);
      LocalVar tuple_slot = GetExecutionResult()->GetOrCreateDestination(tuple_slot_type);
      GetEmitter()->Emit(Bytecode::StorageInterfaceTableInsert, tuple_slot, storage_interface);
      break;
    }
    case ast::Builtin::TableDelete: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::StorageInterfaceTableDelete, cond, storage_interface, tuple_slot);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::TableUpdate: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::StorageInterfaceTableUpdate, cond, storage_interface, tuple_slot);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::GetIndexPR: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      auto index_oid = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->EmitStorageInterfaceGetIndexPR(Bytecode::StorageInterfaceGetIndexPR, pr, storage_interface,
                                                   index_oid);
      break;
    }
    case ast::Builtin::IndexGetSize: {
      LocalVar index_size = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::StorageInterfaceIndexGetSize, index_size, storage_interface);
      GetExecutionResult()->SetDestination(index_size.ValueOf());
      break;
    }
    case ast::Builtin::StorageInterfaceGetIndexHeapSize: {
      LocalVar size = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::StorageInterfaceGetIndexHeapSize, size, storage_interface);
      GetExecutionResult()->SetDestination(size.ValueOf());
      break;
    }
    case ast::Builtin::IndexInsert: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      GetEmitter()->Emit(Bytecode::StorageInterfaceIndexInsert, cond, storage_interface);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexInsertUnique: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      GetEmitter()->Emit(Bytecode::StorageInterfaceIndexInsertUnique, cond, storage_interface);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexInsertWithSlot: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar unique = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::StorageInterfaceIndexInsertWithSlot, cond, storage_interface, tuple_slot, unique);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexDelete: {
      LocalVar tuple_slot = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::StorageInterfaceIndexDelete, storage_interface, tuple_slot);
      break;
    }

    case ast::Builtin::StorageInterfaceFree: {
      GetEmitter()->Emit(Bytecode::StorageInterfaceFree, storage_interface);
      break;
    }
    default:
      UNREACHABLE("Undefined storage_interface call!");
  }
}

void BytecodeGenerator::VisitBuiltinParamCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  LocalVar param_idx = VisitExpressionForRValue(call->Arguments()[1]);
  LocalVar ret = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  switch (builtin) {
    case ast::Builtin::GetParamBool:
      GetEmitter()->Emit(Bytecode::GetParamBool, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamTinyInt:
      GetEmitter()->Emit(Bytecode::GetParamTinyInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamSmallInt:
      GetEmitter()->Emit(Bytecode::GetParamSmallInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamInt:
      GetEmitter()->Emit(Bytecode::GetParamInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamBigInt:
      GetEmitter()->Emit(Bytecode::GetParamBigInt, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamReal:
      GetEmitter()->Emit(Bytecode::GetParamReal, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamDouble:
      GetEmitter()->Emit(Bytecode::GetParamDouble, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamDate:
      GetEmitter()->Emit(Bytecode::GetParamDateVal, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamTimestamp:
      GetEmitter()->Emit(Bytecode::GetParamTimestampVal, ret, exec_ctx, param_idx);
      break;
    case ast::Builtin::GetParamString:
      GetEmitter()->Emit(Bytecode::GetParamString, ret, exec_ctx, param_idx);
      break;
    default:
      UNREACHABLE("Impossible parameter call!");
  }
}

void BytecodeGenerator::VisitBuiltinStringCall(ast::CallExpr *call, ast::Builtin builtin) {
  LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
  LocalVar ret = GetExecutionResult()->GetOrCreateDestination(call->GetType());
  switch (builtin) {
    case ast::Builtin::SplitPart: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar delim = VisitExpressionForSQLValue(call->Arguments()[2]);
      LocalVar field = VisitExpressionForSQLValue(call->Arguments()[3]);
      GetEmitter()->Emit(Bytecode::SplitPart, ret, exec_ctx, input_string, delim, field);
      break;
    }
    case ast::Builtin::Chr: {
      // input_string here is a integer type number
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Chr, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::CharLength: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::CharLength, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::ASCII: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ASCII, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::Lower: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Lower, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::Upper: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Upper, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::Version: {
      GetEmitter()->Emit(Bytecode::Version, ret, exec_ctx);
      break;
    }
    case ast::Builtin::StartsWith: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar start_str = VisitExpressionForSQLValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::StartsWith, ret, exec_ctx, input_string, start_str);
      break;
    }
    case ast::Builtin::Substring: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar start_ind = VisitExpressionForSQLValue(call->Arguments()[2]);
      LocalVar length = VisitExpressionForSQLValue(call->Arguments()[3]);
      GetEmitter()->Emit(Bytecode::Substring, ret, exec_ctx, input_string, start_ind, length);
      break;
    }
    case ast::Builtin::Reverse: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Reverse, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::Left: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar len = VisitExpressionForSQLValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::Left, ret, exec_ctx, input_string, len);
      break;
    }
    case ast::Builtin::Right: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar len = VisitExpressionForSQLValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::Right, ret, exec_ctx, input_string, len);
      break;
    }
    case ast::Builtin::Repeat: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar num_repeat = VisitExpressionForSQLValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::Repeat, ret, exec_ctx, input_string, num_repeat);
      break;
    }
    case ast::Builtin::Trim: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Trim, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::Trim2: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar trim_str = VisitExpressionForSQLValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::Trim2, ret, exec_ctx, input_string, trim_str);
      break;
    }
    case ast::Builtin::Position: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar sub_string = VisitExpressionForSQLValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::Position, ret, exec_ctx, input_string, sub_string);
      break;
    }
    case ast::Builtin::Length: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::Length, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::InitCap: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::InitCap, ret, exec_ctx, input_string);
      break;
    }
    case ast::Builtin::Lpad: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar len = VisitExpressionForSQLValue(call->Arguments()[2]);
      if (call->NumArgs() == 4) {
        LocalVar pad = VisitExpressionForSQLValue(call->Arguments()[3]);
        GetEmitter()->Emit(Bytecode::LPad3Arg, ret, exec_ctx, input_string, len, pad);
      } else {
        GetEmitter()->Emit(Bytecode::LPad2Arg, ret, exec_ctx, input_string, len);
      }
      break;
    }
    case ast::Builtin::Rpad: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      LocalVar len = VisitExpressionForSQLValue(call->Arguments()[2]);
      if (call->NumArgs() == 4) {
        LocalVar pad = VisitExpressionForSQLValue(call->Arguments()[3]);
        GetEmitter()->Emit(Bytecode::RPad3Arg, ret, exec_ctx, input_string, len, pad);
      } else {
        GetEmitter()->Emit(Bytecode::RPad2Arg, ret, exec_ctx, input_string, len);
      }
      break;
    }
    case ast::Builtin::Ltrim: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      if (call->NumArgs() == 2) {
        GetEmitter()->Emit(Bytecode::LTrim1Arg, ret, exec_ctx, input_string);
      } else {
        LocalVar chars = VisitExpressionForSQLValue(call->Arguments()[2]);
        GetEmitter()->Emit(Bytecode::LTrim2Arg, ret, exec_ctx, input_string, chars);
      }
      break;
    }
    case ast::Builtin::Rtrim: {
      LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[1]);
      if (call->NumArgs() == 2) {
        GetEmitter()->Emit(Bytecode::RTrim1Arg, ret, exec_ctx, input_string);
      } else {
        LocalVar chars = VisitExpressionForSQLValue(call->Arguments()[2]);
        GetEmitter()->Emit(Bytecode::RTrim2Arg, ret, exec_ctx, input_string, chars);
      }
      break;
    }
    case ast::Builtin::Concat: {
      const auto num_inputs = call->NumArgs() - 1;

      const auto string_type = ast::BuiltinType::Get(call->GetType()->GetContext(), ast::BuiltinType::StringVal);
      const auto array_type = ast::ArrayType::Get(num_inputs, string_type->PointerTo());

      LocalVar inputs = GetCurrentFunction()->NewLocal(array_type);
      auto arr_elem_ptr = GetCurrentFunction()->NewLocal(string_type->PointerTo()->PointerTo());
      for (uint32_t i = 0; i < num_inputs; i++) {
        GetEmitter()->EmitLea(arr_elem_ptr, inputs, i * 8);
        LocalVar input_string = VisitExpressionForSQLValue(call->Arguments()[i + 1]);
        GetEmitter()->EmitAssign(Bytecode::Assign8, arr_elem_ptr.ValueOf(), input_string);
      }

      GetEmitter()->EmitConcat(ret, exec_ctx, inputs, num_inputs);
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
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::DateToSql:
    case ast::Builtin::TimestampToSql:
    case ast::Builtin::TimestampToSqlYMDHMSMU:
    case ast::Builtin::StringToSql:
    case ast::Builtin::SqlToBool:
    case ast::Builtin::ConvertBoolToInteger:
    case ast::Builtin::ConvertIntegerToReal:
    case ast::Builtin::ConvertDateToTimestamp:
    case ast::Builtin::ConvertStringToBool:
    case ast::Builtin::ConvertStringToInt:
    case ast::Builtin::ConvertStringToReal:
    case ast::Builtin::ConvertStringToDate:
    case ast::Builtin::ConvertStringToTime: {
      VisitSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::IsValNull:
    case ast::Builtin::InitSqlNull: {
      VisitNullValueCall(call, builtin);
      break;
    }
    case ast::Builtin::Like: {
      VisitSqlStringLikeCall(call);
      break;
    }
    case ast::Builtin::DatePart: {
      VisitBuiltinDateFunctionCall(call, builtin);
      break;
    }
    case ast::Builtin::RegisterThreadWithMetricsManager: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::RegisterThreadWithMetricsManager, exec_ctx);
      break;
    }
    case ast::Builtin::EnsureTrackersStopped: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::EnsureTrackersStopped, exec_ctx);
      break;
    }
    case ast::Builtin::AggregateMetricsThread: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::AggregateMetricsThread, exec_ctx);
      break;
    }
    case ast::Builtin::ExecutionContextAddRowsAffected:
    case ast::Builtin::ExecutionContextRegisterHook:
    case ast::Builtin::ExecutionContextClearHooks:
    case ast::Builtin::ExecutionContextInitHooks:
    case ast::Builtin::ExecutionContextGetMemoryPool:
    case ast::Builtin::ExecutionContextGetTLS:
    case ast::Builtin::ExecutionContextStartResourceTracker:
    case ast::Builtin::ExecutionContextSetMemoryUseOverride:
    case ast::Builtin::ExecutionContextEndResourceTracker:
    case ast::Builtin::ExecutionContextStartPipelineTracker:
    case ast::Builtin::ExecutionContextEndPipelineTracker:
    case ast::Builtin::ExecOUFeatureVectorInitialize: {
      VisitExecutionContextCall(call, builtin);
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorReset: {
      LocalVar ouvector = VisitExpressionForRValue(call->Arguments()[0]);
      GetEmitter()->Emit(Bytecode::ExecOUFeatureVectorReset, ouvector);
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorFilter: {
      LocalVar ouvector = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar filter = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::ExecOUFeatureVectorFilter, ouvector, filter);
      break;
    }
    case ast::Builtin::ExecOUFeatureVectorRecordFeature: {
      LocalVar ouvec = VisitExpressionForRValue(call->Arguments()[0]);
      LocalVar pipeline_id = VisitExpressionForRValue(call->Arguments()[1]);
      LocalVar feature_id = VisitExpressionForRValue(call->Arguments()[2]);
      LocalVar feature_attribute = VisitExpressionForRValue(call->Arguments()[3]);
      LocalVar mode = VisitExpressionForRValue(call->Arguments()[4]);
      LocalVar value = VisitExpressionForRValue(call->Arguments()[5]);
      GetEmitter()->Emit(Bytecode::ExecOUFeatureVectorRecordFeature, ouvec, pipeline_id, feature_id, feature_attribute,
                         mode, value);
      break;
    }
    case ast::Builtin::ThreadStateContainerIterate:
    case ast::Builtin::ThreadStateContainerGetState:
    case ast::Builtin::ThreadStateContainerReset:
    case ast::Builtin::ThreadStateContainerClear: {
      VisitBuiltinThreadStateContainerCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterInit:
    case ast::Builtin::TableIterAdvance:
    case ast::Builtin::TableIterGetVPINumTuples:
    case ast::Builtin::TableIterGetVPI:
    case ast::Builtin::TableIterClose: {
      VisitBuiltinTableIterCall(call, builtin);
      break;
    }
    case ast::Builtin::TableIterParallel: {
      VisitBuiltinTableIterParallelCall(call);
      break;
    }
    case ast::Builtin::VPIInit:
    case ast::Builtin::VPIFree:
    case ast::Builtin::VPIIsFiltered:
    case ast::Builtin::VPIGetSelectedRowCount:
    case ast::Builtin::VPIGetVectorProjection:
    case ast::Builtin::VPIHasNext:
    case ast::Builtin::VPIHasNextFiltered:
    case ast::Builtin::VPIAdvance:
    case ast::Builtin::VPIAdvanceFiltered:
    case ast::Builtin::VPISetPosition:
    case ast::Builtin::VPISetPositionFiltered:
    case ast::Builtin::VPIMatch:
    case ast::Builtin::VPIReset:
    case ast::Builtin::VPIResetFiltered:
    case ast::Builtin::VPIGetSlot:
    case ast::Builtin::VPIGetBool:
    case ast::Builtin::VPIGetBoolNull:
    case ast::Builtin::VPIGetTinyInt:
    case ast::Builtin::VPIGetTinyIntNull:
    case ast::Builtin::VPIGetSmallInt:
    case ast::Builtin::VPIGetSmallIntNull:
    case ast::Builtin::VPIGetInt:
    case ast::Builtin::VPIGetIntNull:
    case ast::Builtin::VPIGetBigInt:
    case ast::Builtin::VPIGetBigIntNull:
    case ast::Builtin::VPIGetReal:
    case ast::Builtin::VPIGetRealNull:
    case ast::Builtin::VPIGetDouble:
    case ast::Builtin::VPIGetDoubleNull:
    case ast::Builtin::VPIGetDate:
    case ast::Builtin::VPIGetDateNull:
    case ast::Builtin::VPIGetTimestamp:
    case ast::Builtin::VPIGetTimestampNull:
    case ast::Builtin::VPIGetString:
    case ast::Builtin::VPIGetStringNull:
    case ast::Builtin::VPIGetPointer:
    case ast::Builtin::VPISetBool:
    case ast::Builtin::VPISetBoolNull:
    case ast::Builtin::VPISetTinyInt:
    case ast::Builtin::VPISetTinyIntNull:
    case ast::Builtin::VPISetSmallInt:
    case ast::Builtin::VPISetSmallIntNull:
    case ast::Builtin::VPISetInt:
    case ast::Builtin::VPISetIntNull:
    case ast::Builtin::VPISetBigInt:
    case ast::Builtin::VPISetBigIntNull:
    case ast::Builtin::VPISetReal:
    case ast::Builtin::VPISetRealNull:
    case ast::Builtin::VPISetDouble:
    case ast::Builtin::VPISetDoubleNull:
    case ast::Builtin::VPISetDate:
    case ast::Builtin::VPISetDateNull:
    case ast::Builtin::VPISetTimestamp:
    case ast::Builtin::VPISetTimestampNull:
    case ast::Builtin::VPISetString:
    case ast::Builtin::VPISetStringNull: {
      VisitBuiltinVPICall(call, builtin);
      break;
    }
    case ast::Builtin::Hash: {
      VisitBuiltinHashCall(call);
      break;
    };
    case ast::Builtin::FilterManagerInit:
    case ast::Builtin::FilterManagerInsertFilter:
    case ast::Builtin::FilterManagerRunFilters:
    case ast::Builtin::FilterManagerFree: {
      VisitBuiltinFilterManagerCall(call, builtin);
      break;
    }
    case ast::Builtin::VectorFilterEqual:
    case ast::Builtin::VectorFilterGreaterThan:
    case ast::Builtin::VectorFilterGreaterThanEqual:
    case ast::Builtin::VectorFilterLessThan:
    case ast::Builtin::VectorFilterLessThanEqual:
    case ast::Builtin::VectorFilterNotEqual:
    case ast::Builtin::VectorFilterLike:
    case ast::Builtin::VectorFilterNotLike: {
      VisitBuiltinVectorFilterCall(call, builtin);
      break;
    }
    case ast::Builtin::AggHashTableInit:
    case ast::Builtin::AggHashTableGetTupleCount:
    case ast::Builtin::AggHashTableGetInsertCount:
    case ast::Builtin::AggHashTableInsert:
    case ast::Builtin::AggHashTableLinkEntry:
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
    case ast::Builtin::AggPartIterGetRowEntry:
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
    case ast::Builtin::JoinHashTableGetTupleCount:
    case ast::Builtin::JoinHashTableBuild:
    case ast::Builtin::JoinHashTableBuildParallel:
    case ast::Builtin::JoinHashTableLookup:
    case ast::Builtin::JoinHashTableFree: {
      VisitBuiltinJoinHashTableCall(call, builtin);
      break;
    }
    case ast::Builtin::HashTableEntryIterHasNext:
    case ast::Builtin::HashTableEntryIterGetRow: {
      VisitBuiltinHashTableEntryIteratorCall(call, builtin);
      break;
    }
    case ast::Builtin::JoinHashTableIterInit:
    case ast::Builtin::JoinHashTableIterHasNext:
    case ast::Builtin::JoinHashTableIterNext:
    case ast::Builtin::JoinHashTableIterGetRow:
    case ast::Builtin::JoinHashTableIterFree: {
      VisitBuiltinJoinHashTableIteratorCall(call, builtin);
      break;
    }
    case ast::Builtin::SorterInit:
    case ast::Builtin::SorterGetTupleCount:
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
    case ast::Builtin::SorterIterSkipRows:
    case ast::Builtin::SorterIterGetRow:
    case ast::Builtin::SorterIterClose: {
      VisitBuiltinSorterIterCall(call, builtin);
      break;
    }
    case ast::Builtin::ResultBufferNew:
    case ast::Builtin::ResultBufferAllocOutRow:
    case ast::Builtin::ResultBufferFinalize:
    case ast::Builtin::ResultBufferFree: {
      VisitResultBufferCall(call, builtin);
      break;
    }
#if 0
    case ast::Builtin::CSVReaderInit:
    case ast::Builtin::CSVReaderAdvance:
    case ast::Builtin::CSVReaderGetField:
    case ast::Builtin::CSVReaderGetRecordNumber:
    case ast::Builtin::CSVReaderClose: {
      VisitCSVReaderCall(call, builtin);
      break;
    }
#endif
    case ast::Builtin::IndexIteratorInit:
    case ast::Builtin::IndexIteratorGetSize:
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
    case ast::Builtin::IndexIteratorGetSlot: {
      VisitBuiltinIndexIteratorCall(call, builtin);
      break;
    }
    case ast::Builtin::Exp:
    case ast::Builtin::ACos:
    case ast::Builtin::ASin:
    case ast::Builtin::ATan:
    case ast::Builtin::ATan2:
    case ast::Builtin::Cosh:
    case ast::Builtin::Sinh:
    case ast::Builtin::Tanh:
    case ast::Builtin::Cos:
    case ast::Builtin::Cot:
    case ast::Builtin::Sin:
    case ast::Builtin::Tan:
    case ast::Builtin::Ceil:
    case ast::Builtin::Floor:
    case ast::Builtin::Truncate:
    case ast::Builtin::Log10:
    case ast::Builtin::Log2:
    case ast::Builtin::Sqrt:
    case ast::Builtin::Cbrt:
    case ast::Builtin::Round:
    case ast::Builtin::Round2:
    case ast::Builtin::Pow: {
      VisitBuiltinTrigCall(call, builtin);
      break;
    }
    case ast::Builtin::Abs:
    case ast::Builtin::Mod: {
      VisitBuiltinArithmeticCall(call, builtin);
      break;
    }
    case ast::Builtin::AtomicAnd:
    case ast::Builtin::AtomicOr: {
      VisitBuiltinAtomicArithmeticCall(call, builtin);
      break;
    }
    case ast::Builtin::AtomicCompareExchange: {
      VisitBuiltinAtomicCompareExchangeCall(call);
      break;
    }
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
    case ast::Builtin::GetTablePR:
    case ast::Builtin::TableInsert:
    case ast::Builtin::TableDelete:
    case ast::Builtin::TableUpdate:
    case ast::Builtin::GetIndexPR:
    case ast::Builtin::IndexGetSize:
    case ast::Builtin::StorageInterfaceGetIndexHeapSize:
    case ast::Builtin::IndexInsert:
    case ast::Builtin::IndexInsertUnique:
    case ast::Builtin::IndexInsertWithSlot:
    case ast::Builtin::IndexDelete:
    case ast::Builtin::StorageInterfaceFree: {
      VisitBuiltinStorageInterfaceCall(call, builtin);
      break;
    }
    case ast::Builtin::SizeOf: {
      VisitBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::OffsetOf: {
      VisitBuiltinOffsetOfCall(call);
      break;
    }
    case ast::Builtin::PtrCast: {
      Visit(call->Arguments()[1]);
      break;
    }
    case ast::Builtin::AbortTxn: {
      VisitAbortTxn(call);
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
    case ast::Builtin::SplitPart:
    case ast::Builtin::Chr:
    case ast::Builtin::CharLength:
    case ast::Builtin::ASCII:
    case ast::Builtin::Lower:
    case ast::Builtin::Upper:
    case ast::Builtin::Version:
    case ast::Builtin::Position:
    case ast::Builtin::Length:
    case ast::Builtin::InitCap:
    case ast::Builtin::StartsWith:
    case ast::Builtin::Substring:
    case ast::Builtin::Left:
    case ast::Builtin::Right:
    case ast::Builtin::Reverse:
    case ast::Builtin::Repeat:
    case ast::Builtin::Trim:
    case ast::Builtin::Trim2:
    case ast::Builtin::Lpad:
    case ast::Builtin::Ltrim:
    case ast::Builtin::Rpad:
    case ast::Builtin::Rtrim:
    case ast::Builtin::Concat: {
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
        GetEmitter()->Emit(Bytecode::NpRunnersEmitInt, exec_ctx, num_tuple, num_col, int_col, real_col);
      } else {
        GetEmitter()->Emit(Bytecode::NpRunnersEmitReal, exec_ctx, num_tuple, num_col, int_col, real_col);
      }

      break;
    }
    case ast::Builtin::NpRunnersDummyInt:
    case ast::Builtin::NpRunnersDummyReal: {
      LocalVar exec_ctx = VisitExpressionForRValue(call->Arguments()[0]);
      if (builtin == ast::Builtin::NpRunnersDummyInt) {
        GetEmitter()->Emit(Bytecode::NpRunnersDummyInt, exec_ctx);
      } else {
        GetEmitter()->Emit(Bytecode::NpRunnersDummyReal, exec_ctx);
      }

      break;
    }
    case ast::Builtin::TestCatalogLookup: {
      VisitBuiltinTestCatalogLookup(call);
      break;
    }
    case ast::Builtin::TestCatalogIndexLookup: {
      VisitBuiltinTestCatalogIndexLookup(call);
      break;
    }
    default:
      UNREACHABLE("Unknown builtin bytecode.");
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
      auto table_oid = VisitExpressionForRValue(call->Arguments()[3]);
      // Index OID
      auto index_oid = VisitExpressionForRValue(call->Arguments()[4]);
      // Col OIDs
      auto *arr_type = call->Arguments()[5]->GetType()->As<ast::ArrayType>();
      LocalVar col_oids = VisitExpressionForLValue(call->Arguments()[5]);
      // Emit the initialization codes
      GetEmitter()->EmitIndexIteratorInit(Bytecode::IndexIteratorInit, iterator, exec_ctx, num_attrs, table_oid,
                                          index_oid, col_oids, static_cast<uint32_t>(arr_type->GetLength()));
      GetEmitter()->Emit(Bytecode::IndexIteratorPerformInit, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetSize: {
      LocalVar index_size = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::IndexIteratorGetSize, index_size, iterator);
      GetExecutionResult()->SetDestination(index_size.ValueOf());
      break;
    }
    case ast::Builtin::IndexIteratorScanKey: {
      GetEmitter()->Emit(Bytecode::IndexIteratorScanKey, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorScanAscending: {
      auto asc_type = VisitExpressionForRValue(call->Arguments()[1]);
      auto limit = VisitExpressionForRValue(call->Arguments()[2]);
      GetEmitter()->Emit(Bytecode::IndexIteratorScanAscending, iterator, asc_type, limit);
      break;
    }
    case ast::Builtin::IndexIteratorScanDescending: {
      GetEmitter()->Emit(Bytecode::IndexIteratorScanDescending, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorScanLimitDescending: {
      auto limit = VisitExpressionForRValue(call->Arguments()[1]);
      GetEmitter()->Emit(Bytecode::IndexIteratorScanLimitDescending, iterator, limit);
      break;
    }
    case ast::Builtin::IndexIteratorAdvance: {
      LocalVar cond = GetExecutionResult()->GetOrCreateDestination(ast::BuiltinType::Get(ctx, ast::BuiltinType::Bool));
      GetEmitter()->Emit(Bytecode::IndexIteratorAdvance, cond, iterator);
      GetExecutionResult()->SetDestination(cond.ValueOf());
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      GetEmitter()->Emit(Bytecode::IndexIteratorFree, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetPR: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::IndexIteratorGetPR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetLoPR: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::IndexIteratorGetLoPR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetHiPR: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::IndexIteratorGetHiPR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetTablePR: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::IndexIteratorGetTablePR, pr, iterator);
      break;
    }
    case ast::Builtin::IndexIteratorGetSlot: {
      LocalVar pr = GetExecutionResult()->GetOrCreateDestination(call->GetType());
      GetEmitter()->Emit(Bytecode::IndexIteratorGetSlot, pr, iterator);
      break;
    }
    default: {
      UNREACHABLE("Impossible bytecode");
    }
  }
}

void BytecodeGenerator::VisitRegularCallExpr(ast::CallExpr *call) {
  bool caller_wants_result = GetExecutionResult() != nullptr;
  NOISEPAGE_ASSERT(!caller_wants_result || GetExecutionResult()->IsRValue(), "Calls can only be R-Values!");

  std::vector<LocalVar> params;

  auto *func_type = call->Function()->GetType()->As<ast::FunctionType>();

  if (!func_type->GetReturnType()->IsNilType()) {
    LocalVar ret_val;
    if (caller_wants_result) {
      ret_val = GetExecutionResult()->GetOrCreateDestination(func_type->GetReturnType());

      // Let the caller know where the result value is
      GetExecutionResult()->SetDestination(ret_val.ValueOf());
    } else {
      ret_val = GetCurrentFunction()->NewLocal(func_type->GetReturnType());
    }

    // Push return value address into parameter list
    params.push_back(ret_val);
  }

  // Collect non-return-value parameters as usual
  for (uint32_t i = 0; i < func_type->GetNumParams(); i++) {
    params.push_back(VisitExpressionForRValue(call->Arguments()[i]));
  }

  // Emit call
  const auto func_id = LookupFuncIdByName(call->GetFuncName().GetData());
  NOISEPAGE_ASSERT(func_id != FunctionInfo::K_INVALID_FUNC_ID, "Function not found!");
  GetEmitter()->EmitCall(func_id, params);
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
  NOISEPAGE_ASSERT(GetExecutionResult()->IsRValue(), "Literal expressions cannot be R-Values!");

  LocalVar target = GetExecutionResult()->GetOrCreateDestination(node->GetType());

  switch (node->GetLiteralKind()) {
    case ast::LitExpr::LitKind::Nil: {
      // Do nothing
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      GetEmitter()->EmitAssignImm1(target, static_cast<int8_t>(node->BoolVal()));
      GetExecutionResult()->SetDestination(target.ValueOf());
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      if (static_cast<int64_t>(std::numeric_limits<int>::lowest()) <= node->Int64Val() &&
          node->Int64Val() <= static_cast<int64_t>(std::numeric_limits<int>::max())) {
        GetEmitter()->EmitAssignImm4(target, node->Int64Val());
      } else {
        GetEmitter()->EmitAssignImm8(target, node->Int64Val());
      }
      GetExecutionResult()->SetDestination(target.ValueOf());
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      GetEmitter()->EmitAssignImm8F(target, static_cast<float>(node->Float64Val()));
      GetExecutionResult()->SetDestination(target.ValueOf());
      break;
    }
    case ast::LitExpr::LitKind::String: {
      LocalVar string = NewStaticString(node->GetType()->GetContext(), node->StringVal());
      GetEmitter()->EmitAssign(Bytecode::Assign8, target, string);
      GetExecutionResult()->SetDestination(string.ValueOf());
      break;
    }
    default: {
      EXECUTION_LOG_ERROR("Non-bool or non-integer literals not supported in bytecode");
      break;
    }
  }
}

void BytecodeGenerator::VisitStructDecl(UNUSED_ATTRIBUTE ast::StructDecl *node) {
  // Nothing to do
}

void BytecodeGenerator::VisitLogicalAndOrExpr(ast::BinaryOpExpr *node) {
  NOISEPAGE_ASSERT(GetExecutionResult()->IsRValue(), "Binary expressions must be R-Values!");
  NOISEPAGE_ASSERT(node->GetType()->IsBoolType(), "Boolean binary operation must be of type bool");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());

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
  GetEmitter()->EmitConditionalJump(conditional_jump, dest.ValueOf(), &end_label);

  // Execute the right child
  VisitExpressionForRValue(node->Right(), dest);

  // Bind the end label
  GetEmitter()->Bind(&end_label);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

#define MATH_BYTECODE(CODE_RESULT, MATH_OP, TPL_TYPE)                                             \
  if (TPL_TYPE->IsIntegerType()) {                                                                \
    CODE_RESULT = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::MATH_OP), TPL_TYPE);       \
  } else {                                                                                        \
    NOISEPAGE_ASSERT(TPL_TYPE->IsFloatType(), "Only integer and floating point math operations"); \
    CODE_RESULT = GetFloatTypedBytecode(GET_BASE_FOR_FLOAT_TYPES(Bytecode::MATH_OP), TPL_TYPE);   \
  }

void BytecodeGenerator::VisitPrimitiveArithmeticExpr(ast::BinaryOpExpr *node) {
  NOISEPAGE_ASSERT(GetExecutionResult()->IsRValue(), "Arithmetic expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
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
      MATH_BYTECODE(bytecode, Mod, node->GetType());
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
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

#undef MATH_BYTECODE

void BytecodeGenerator::VisitSqlArithmeticExpr(ast::BinaryOpExpr *node) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  LocalVar left = VisitExpressionForSQLValue(node->Left());
  LocalVar right = VisitExpressionForSQLValue(node->Right());

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
      bytecode = (is_integer_math ? Bytecode::ModInteger : Bytecode::ModReal);
      break;
    }
    default: {
      UNREACHABLE("Impossible arithmetic SQL operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest);
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

#define SQL_COMPARISON_BYTECODE(CODE_RESULT, COMPARISON_TYPE, ARG_KIND) \
  switch (ARG_KIND) {                                                   \
    case ast::BuiltinType::Kind::Boolean:                               \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Bool;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::Integer:                               \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Integer;                 \
      break;                                                            \
    case ast::BuiltinType::Kind::Real:                                  \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Real;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::Date:                                  \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Date;                    \
      break;                                                            \
    case ast::BuiltinType::Kind::Timestamp:                             \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##Timestamp;               \
      break;                                                            \
    case ast::BuiltinType::Kind::StringVal:                             \
      CODE_RESULT = Bytecode::COMPARISON_TYPE##String;                  \
      break;                                                            \
    default:                                                            \
      UNREACHABLE("Undefined SQL comparison!");                         \
  }

void BytecodeGenerator::VisitSqlCompareOpExpr(ast::ComparisonOpExpr *compare) {
  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(compare->GetType());
  LocalVar left = VisitExpressionForSQLValue(compare->Left());
  LocalVar right = VisitExpressionForSQLValue(compare->Right());

  NOISEPAGE_ASSERT(compare->Left()->GetType() == compare->Right()->GetType(),
                   "Left and right input types to comparison are not equal");

  const auto arg_kind = compare->Left()->GetType()->As<ast::BuiltinType>()->GetKind();

  Bytecode code;
  switch (compare->Op()) {
    case parsing::Token::Type::GREATER: {
      SQL_COMPARISON_BYTECODE(code, GreaterThan, arg_kind);
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, GreaterThanEqual, arg_kind);
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, Equal, arg_kind);
      break;
    }
    case parsing::Token::Type::LESS: {
      SQL_COMPARISON_BYTECODE(code, LessThan, arg_kind);
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, LessThanEqual, arg_kind);
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      SQL_COMPARISON_BYTECODE(code, NotEqual, arg_kind);
      break;
    }
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(code, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest);
}

#undef SQL_COMPARISON_BYTECODE

#define COMPARISON_BYTECODE(CODE_RESULT, COMPARISON_TYPE, TPL_TYPE)                                     \
  if (TPL_TYPE->IsIntegerType()) {                                                                      \
    CODE_RESULT = GetIntTypedBytecode(GET_BASE_FOR_INT_TYPES(Bytecode::COMPARISON_TYPE), TPL_TYPE);     \
  } else if (TPL_TYPE->IsFloatType()) {                                                                 \
    CODE_RESULT = GetFloatTypedBytecode(GET_BASE_FOR_FLOAT_TYPES(Bytecode::COMPARISON_TYPE), TPL_TYPE); \
  } else {                                                                                              \
    NOISEPAGE_ASSERT(TPL_TYPE->IsBoolType(), "Only integer, floating point, and boolean comparisons");  \
    CODE_RESULT = Bytecode::COMPARISON_TYPE##_bool;                                                     \
  }

void BytecodeGenerator::VisitPrimitiveCompareOpExpr(ast::ComparisonOpExpr *compare) {
  NOISEPAGE_ASSERT(GetExecutionResult()->IsRValue(), "Comparison expressions must be R-Values!");

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(compare->GetType());

  // nil comparison
  if (ast::Expr * input_expr; compare->IsLiteralCompareNil(&input_expr)) {
    LocalVar input = VisitExpressionForRValue(input_expr);
    Bytecode bytecode =
        compare->Op() == parsing::Token::Type ::EQUAL_EQUAL ? Bytecode::IsNullPtr : Bytecode::IsNotNullPtr;
    GetEmitter()->Emit(bytecode, dest, input);
    GetExecutionResult()->SetDestination(dest.ValueOf());
    return;
  }

  // regular comparison

  NOISEPAGE_ASSERT(compare->Left()->GetType()->IsArithmetic() || compare->Left()->GetType()->IsBoolType(),
                   "Invalid type to comparison");
  NOISEPAGE_ASSERT(compare->Right()->GetType()->IsArithmetic() || compare->Right()->GetType()->IsBoolType(),
                   "Invalid type to comparison");

  LocalVar left = VisitExpressionForRValue(compare->Left());
  LocalVar right = VisitExpressionForRValue(compare->Right());

  Bytecode bytecode;
  switch (compare->Op()) {
    case parsing::Token::Type::GREATER: {
      COMPARISON_BYTECODE(bytecode, GreaterThan, compare->Left()->GetType());
      break;
    }
    case parsing::Token::Type::GREATER_EQUAL: {
      COMPARISON_BYTECODE(bytecode, GreaterThanEqual, compare->Left()->GetType());
      break;
    }
    case parsing::Token::Type::EQUAL_EQUAL: {
      COMPARISON_BYTECODE(bytecode, Equal, compare->Left()->GetType());
      break;
    }
    case parsing::Token::Type::LESS: {
      COMPARISON_BYTECODE(bytecode, LessThan, compare->Left()->GetType());
      break;
    }
    case parsing::Token::Type::LESS_EQUAL: {
      COMPARISON_BYTECODE(bytecode, LessThanEqual, compare->Left()->GetType());
      break;
    }
    case parsing::Token::Type::BANG_EQUAL: {
      COMPARISON_BYTECODE(bytecode, NotEqual, compare->Left()->GetType());
      break;
    }
    default: {
      UNREACHABLE("Impossible binary operation");
    }
  }

  // Emit
  GetEmitter()->EmitBinaryOp(bytecode, dest, left, right);

  // Mark where the result is
  GetExecutionResult()->SetDestination(dest.ValueOf());
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

void BytecodeGenerator::BuildAssign(LocalVar dest, LocalVar val, ast::Type *dest_type) {
  // Emit the appropriate assignment
  const uint32_t size = dest_type->GetSize();
  if (size == 1) {
    GetEmitter()->EmitAssign(Bytecode::Assign1, dest, val);
  } else if (size == 2) {
    GetEmitter()->EmitAssign(Bytecode::Assign2, dest, val);
  } else if (size == 4) {
    GetEmitter()->EmitAssign(Bytecode::Assign4, dest, val);
  } else {
    GetEmitter()->EmitAssign(Bytecode::Assign8, dest, val);
  }
}

void BytecodeGenerator::BuildDeref(LocalVar dest, LocalVar ptr, ast::Type *dest_type) {
  // Emit the appropriate deref
  const uint32_t size = dest_type->GetSize();
  if (size == 1) {
    GetEmitter()->EmitDeref(Bytecode::Deref1, dest, ptr);
  } else if (size == 2) {
    GetEmitter()->EmitDeref(Bytecode::Deref2, dest, ptr);
  } else if (size == 4) {
    GetEmitter()->EmitDeref(Bytecode::Deref4, dest, ptr);
  } else if (size == 8) {
    GetEmitter()->EmitDeref(Bytecode::Deref8, dest, ptr);
  } else {
    GetEmitter()->EmitDerefN(dest, ptr, size);
  }
}

LocalVar BytecodeGenerator::BuildLoadPointer(LocalVar double_ptr, ast::Type *type) {
  if (double_ptr.GetAddressMode() == LocalVar::AddressMode::Address) {
    return double_ptr.ValueOf();
  }

  // Need to Deref
  LocalVar ptr = GetCurrentFunction()->NewLocal(type);
  GetEmitter()->EmitDeref(Bytecode::Deref8, ptr, double_ptr);
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
    obj_type = type->As<ast::PointerType>()->GetBase()->As<ast::StructType>();
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
    field_ptr = GetCurrentFunction()->NewLocal(node->GetType()->PointerTo());
    GetEmitter()->EmitLea(field_ptr, obj_ptr, offset);
    field_ptr = field_ptr.ValueOf();
  }

  if (GetExecutionResult()->IsLValue()) {
    NOISEPAGE_ASSERT(!GetExecutionResult()->HasDestination(), "L-Values produce their destination");
    GetExecutionResult()->SetDestination(field_ptr);
    return;
  }

  // The caller wants the actual value of the field. We just computed a pointer
  // to the field in the object, so we need to load/dereference it. If the
  // caller provided a destination variable, use that; otherwise, create a new
  // temporary variable to store the value.

  LocalVar dest = GetExecutionResult()->GetOrCreateDestination(node->GetType());
  BuildDeref(dest, field_ptr, node->GetType());
  GetExecutionResult()->SetDestination(dest.ValueOf());
}

void BytecodeGenerator::VisitDeclStmt(ast::DeclStmt *node) { Visit(node->Declaration()); }

void BytecodeGenerator::VisitExpressionStmt(ast::ExpressionStmt *node) { Visit(node->Expression()); }

void BytecodeGenerator::VisitBadExpr(ast::BadExpr *node) {
  NOISEPAGE_ASSERT(false, "Visiting bad expression during code generation!");
}

void BytecodeGenerator::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  NOISEPAGE_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  NOISEPAGE_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  NOISEPAGE_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  NOISEPAGE_ASSERT(false, "Should not visit type-representation nodes!");
}

void BytecodeGenerator::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  NOISEPAGE_ASSERT(false, "Should not visit type-representation nodes!");
}

FunctionInfo *BytecodeGenerator::AllocateFunc(const std::string &func_name, ast::FunctionType *const func_type) {
  // Allocate function
  const auto func_id = static_cast<FunctionId>(functions_.size());
  functions_.emplace_back(func_id, func_name, func_type);
  FunctionInfo *func = &functions_.back();

  // Register return type
  if (auto *return_type = func_type->GetReturnType(); !return_type->IsNilType()) {
    func->NewParameterLocal(return_type->PointerTo(), "hiddenRv");
  }

  // Register parameters
  for (const auto &param : func_type->GetParams()) {
    func->NewParameterLocal(param.type_, param.name_.GetData());
  }

  // Cache
  func_map_[func->GetName()] = func->GetId();

  return func;
}

FunctionId BytecodeGenerator::LookupFuncIdByName(const std::string &name) const {
  auto iter = func_map_.find(name);
  if (iter == func_map_.end()) {
    return FunctionInfo::K_INVALID_FUNC_ID;
  }
  return iter->second;
}

LocalVar BytecodeGenerator::NewStatic(const std::string &name, ast::Type *type, const void *contents) {
  std::size_t offset = data_.size();

  if (!common::MathUtil::IsAligned(offset, type->GetAlignment())) {
    offset = common::MathUtil::AlignTo(offset, type->GetAlignment());
  }

  const std::size_t padded_len = type->GetSize() + (offset - data_.size());
  data_.insert(data_.end(), padded_len, 0);
  std::memcpy(&data_[offset], contents, type->GetSize());

  uint32_t &version = static_locals_versions_[name];
  const std::string name_and_version = name + "_" + std::to_string(version++);
  static_locals_.emplace_back(name_and_version, type, offset, LocalInfo::Kind::Var);

  return LocalVar(offset, LocalVar::AddressMode::Address);
}

LocalVar BytecodeGenerator::NewStaticString(ast::Context *ctx, const ast::Identifier string) {
  // Check cache
  if (auto iter = static_string_cache_.find(string); iter != static_string_cache_.end()) {
    return LocalVar(iter->second.GetOffset(), LocalVar::AddressMode::Address);
  }

  // Create
  auto *type = ast::ArrayType::Get(string.GetLength(), ast::BuiltinType::Get(ctx, ast::BuiltinType::Uint8));
  auto static_local = NewStatic("stringConst", type, static_cast<const void *>(string.GetData()));

  // Cache
  static_string_cache_.emplace(string, static_local);

  return static_local;
}

LocalVar BytecodeGenerator::VisitExpressionForLValue(ast::Expr *expr) {
  LValueResultScope scope(this);
  Visit(expr);
  return scope.GetDestination();
}

LocalVar BytecodeGenerator::VisitExpressionForRValue(ast::Expr *expr) {
  RValueResultScope scope(this);
  Visit(expr);
  return scope.GetDestination();
}

LocalVar BytecodeGenerator::VisitExpressionForSQLValue(ast::Expr *expr) { return VisitExpressionForLValue(expr); }

void BytecodeGenerator::VisitExpressionForSQLValue(ast::Expr *expr, LocalVar dest) {
  VisitExpressionForRValue(expr, dest);
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
      GetEmitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      break;
    }
    case TestFallthrough::Else: {
      GetEmitter()->EmitConditionalJump(Bytecode::JumpIfTrue, cond, then_label);
      break;
    }
    case TestFallthrough::None: {
      GetEmitter()->EmitConditionalJump(Bytecode::JumpIfFalse, cond, else_label);
      GetEmitter()->EmitJump(Bytecode::Jump, then_label);
      break;
    }
  }
}

Bytecode BytecodeGenerator::GetIntTypedBytecode(Bytecode bytecode, ast::Type *type) {
  NOISEPAGE_ASSERT(type->IsIntegerType(), "Type must be integer type");
  auto int_kind = type->SafeAs<ast::BuiltinType>()->GetKind();
  auto kind_idx = static_cast<uint8_t>(int_kind - ast::BuiltinType::Int8);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

Bytecode BytecodeGenerator::GetFloatTypedBytecode(Bytecode bytecode, ast::Type *type) {
  NOISEPAGE_ASSERT(type->IsFloatType(), "Type must be floating-point type");
  auto float_kind = type->SafeAs<ast::BuiltinType>()->GetKind();
  auto kind_idx = static_cast<uint8_t>(float_kind - ast::BuiltinType::Float32);
  return Bytecodes::FromByte(Bytecodes::ToByte(bytecode) + kind_idx);
}

// static
std::unique_ptr<BytecodeModule> BytecodeGenerator::Compile(ast::AstNode *root, const std::string &name) {
  BytecodeGenerator generator{};
  generator.Visit(root);

  // Create the bytecode module. Note that we move the bytecode and functions
  // array from the generator into the module.
  return std::make_unique<BytecodeModule>(name, std::move(generator.code_), std::move(generator.data_),
                                          std::move(generator.functions_), std::move(generator.static_locals_));
}

}  // namespace noisepage::execution::vm
