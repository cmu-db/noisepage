#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/sema.h"

namespace noisepage::execution::sema {

void Sema::ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, ast::Type *expected) {
  GetErrorReporter()->Report(call->Position(), ErrorMessages::kIncorrectCallArgType, call->GetFuncName(), expected,
                             index, call->Arguments()[index]->GetType());
}

void Sema::ReportIncorrectCallArg(ast::CallExpr *call, uint32_t index, const char *expected) {
  GetErrorReporter()->Report(call->Position(), ErrorMessages::kIncorrectCallArgType2, call->GetFuncName(), expected,
                             index, call->Arguments()[index]->GetType());
}

ast::Expr *Sema::ImplCastExprToType(ast::Expr *expr, ast::Type *target_type, ast::CastKind cast_kind) {
  return GetContext()->GetNodeFactory()->NewImplicitCastExpr(expr->Position(), cast_kind, target_type, expr);
}

bool Sema::CheckArgCount(ast::CallExpr *call, uint32_t expected_arg_count) {
  if (call->NumArgs() != expected_arg_count) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(),
                               expected_arg_count, call->NumArgs());
    return false;
  }

  return true;
}

bool Sema::CheckArgCountAtLeast(ast::CallExpr *call, uint32_t expected_arg_count) {
  if (call->NumArgs() < expected_arg_count) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(),
                               expected_arg_count, call->NumArgs());
    return false;
  }

  return true;
}

bool Sema::CheckArgCountBetween(ast::CallExpr *call, uint32_t min_expected_arg_count, uint32_t max_expected_arg_count) {
  if (call->NumArgs() < min_expected_arg_count || call->NumArgs() > max_expected_arg_count) {
    GetErrorReporter()->Report(call->Position(), ErrorMessages::kMismatchedCallArgsBetween, call->GetFuncName(),
                               min_expected_arg_count, max_expected_arg_count, call->NumArgs());
    return false;
  }

  return true;
}

// Logical ops: and, or
Sema::CheckResult Sema::CheckLogicalOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                             ast::Expr *right) {
  //
  // Both left and right types are either primitive booleans or SQL booleans. We
  // need both to be primitive booleans. Cast each expression as appropriate.
  //

  ast::Type *const bool_type = ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool);

  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    left = ImplCastExprToType(left, bool_type, ast::CastKind::SqlBoolToBool);
  }

  if (right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    right = ImplCastExprToType(right, bool_type, ast::CastKind::SqlBoolToBool);
  }

  // If both input expressions are primitive booleans, we're done
  if (left->GetType()->IsBoolType() && right->GetType()->IsBoolType()) {
    return {bool_type, left, right};
  }

  // Okay, there's an error ...

  GetErrorReporter()->Report(pos, ErrorMessages::kMismatchedTypesToBinary, left->GetType(), right->GetType(), op);

  return {nullptr, left, right};
}

// Arithmetic ops: +, -, *, etc.
Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                                ast::Expr *right) {
  // If neither inputs are arithmetic, fail early
  if (!left->GetType()->IsArithmetic() || !right->GetType()->IsArithmetic()) {
    GetErrorReporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(), right->GetType());
    return {nullptr, left, right};
  }

  // If the input types are equal, finish.
  if (left->GetType() == right->GetType()) {
    return {left->GetType(), left, right};
  }

  // primitive int <OP> primitive int
  if (left->GetType()->IsIntegerType() && right->GetType()->IsIntegerType()) {
    if (left->GetType()->GetSize() < right->GetType()->GetSize()) {
      auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::IntegralCast);
      return {right->GetType(), new_left, right};
    }
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::IntegralCast);
    return {left->GetType(), left, new_right};
  }

  // primitive int <OP> SQL int
  if (left->GetType()->IsIntegerType() && right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::IntToSqlInt);
    return {right->GetType(), new_left, right};
  }

  // SQL int <OP> primitive int
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer) && right->GetType()->IsIntegerType()) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::IntToSqlInt);
    return {left->GetType(), left, new_right};
  }

  // primitive float <OP> SQL real
  if (left->GetType()->IsFloatType() && right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real)) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::FloatToSqlReal);
    return {right->GetType(), new_left, right};
  }

  // SQL real <OP> primitive float
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real) && right->GetType()->IsFloatType()) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::FloatToSqlReal);
    return {left->GetType(), left, new_right};
  }

  // SQL int <OP> SQL real
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer) &&
      right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real)) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::SqlIntToSqlReal);
    return {right->GetType(), new_left, right};
  }

  // SQL real <OP> SQL int
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real) &&
      right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::SqlIntToSqlReal);
    return {left->GetType(), left, new_right};
  }

  // TODO(pmenon): Fix me to support other arithmetic types
  GetErrorReporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(), right->GetType());
  return {nullptr, left, right};
}

Sema::CheckResult Sema::CheckSqlComparisonOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                                   ast::Expr *right) {
  NOISEPAGE_ASSERT(left->GetType()->IsSqlValueType() || right->GetType()->IsSqlValueType(),
                   "At least one input to comparison must be SQL value");

  // Primitive bool <cmp> SQL Boolean -> cast left.
  if (left->GetType()->IsBoolType() && right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::BoolToSqlBool);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean), new_left, right};
  }

  // SQL Boolean <cmp> Primitive bool -> cast right.
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean) && right->GetType()->IsBoolType()) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::BoolToSqlBool);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean), left, new_right};
  }

  // Primitive float <cmp> SQL Float -> cast left.
  if (left->GetType()->IsFloatType() && right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real)) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::FloatToSqlReal);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean), new_left, right};
  }

  // SQL Float <cmp> Primitive Float -> cast right.
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real) && right->GetType()->IsFloatType()) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::FloatToSqlReal);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean), left, new_right};
  }

  // Primitive int <cmp> SQL Integer -> cast left.
  if (left->GetType()->IsIntegerType() && right->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::IntToSqlInt);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean), new_left, right};
  }

  // SQL Integer <cmp> Primitive int -> cast right.
  if (left->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer) && right->GetType()->IsIntegerType()) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::IntToSqlInt);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean), left, new_right};
  }

  // Error.
  GetErrorReporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(), right->GetType());
  return {nullptr, left, right};
}

// Comparisons: <, <=, >, >=, ==, !=
Sema::CheckResult Sema::CheckComparisonOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                                ast::Expr *right) {
  // Check for raw pointer comparison.
  if (left->GetType()->IsPointerType() || right->GetType()->IsPointerType()) {
    if (!parsing::Token::IsEqualityOp(op)) {
      GetErrorReporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(), right->GetType());
      return {nullptr, left, right};
    }

    auto lhs = left->GetType()->GetPointeeType();
    auto rhs = right->GetType()->GetPointeeType();

    bool same_pointee_type = (lhs != nullptr && lhs == rhs);
    bool compare_with_nil =
        (lhs == nullptr && left->GetType()->IsNilType()) || (rhs == nullptr && right->GetType()->IsNilType());
    if (same_pointee_type || compare_with_nil) {
      auto *ret_type = ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool);
      return {ret_type, left, right};
    }

    // Error
    GetErrorReporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(), right->GetType());
    return {nullptr, left, right};
  }

  // If the input types are the same, we don't need to do any work.
  if (left->GetType() == right->GetType()) {
    ast::Type *result_type = left->GetType()->IsSqlValueType()
                                 ? ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Boolean)
                                 : ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool);
    return {result_type, left, right};
  }

  // TODO(pmenon): revisit with a fix for down/up-casting integers #1174
  // If both input types are integers, we don't need to do any work.
  if (left->GetType()->IsIntegerType() && right->GetType()->IsIntegerType()) {
    ast::Type *result_type = ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool);
    return {result_type, left, right};
  }

  // Check SQL comparisons separately.
  if (left->GetType()->IsSqlValueType() || right->GetType()->IsSqlValueType()) {
    return CheckSqlComparisonOperands(op, pos, left, right);
  }

  // If neither input expression is arithmetic, it's an ill-formed operation.
  if (!left->GetType()->IsArithmetic() || !right->GetType()->IsArithmetic()) {
    GetErrorReporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->GetType(), right->GetType());
    return {nullptr, left, right};
  }

  // Primitive int -> primitive float
  if (left->GetType()->IsIntegerType() && right->GetType()->IsFloatType()) {
    auto new_left = ImplCastExprToType(left, right->GetType(), ast::CastKind::IntToFloat);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool), new_left, right};
  }

  if (left->GetType()->IsFloatType() && right->GetType()->IsIntegerType()) {
    auto new_right = ImplCastExprToType(right, left->GetType(), ast::CastKind::IntToFloat);
    return {ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool), left, new_right};
  }

  UNREACHABLE("Impossible");
}

bool Sema::CheckAssignmentConstraints(ast::Type *target_type, ast::Expr **expr) {
  // If the target and expression types are the same, nothing to do
  if ((*expr)->GetType() == target_type) {
    return true;
  }

  // Null pointers
  if (target_type->IsPointerType() && (*expr)->GetType()->IsNilType()) {
    return true;
  }

  // Integer expansion
  if (target_type->IsIntegerType() && (*expr)->GetType()->IsIntegerType()) {
    if (target_type->GetSize() > (*expr)->GetType()->GetSize()) {
      *expr = ImplCastExprToType(*expr, target_type, ast::CastKind::IntegralCast);
    }
    return true;
  }

  // Float to integer expansion
  if (target_type->IsIntegerType() && (*expr)->GetType()->IsFloatType()) {
    *expr = ImplCastExprToType(*expr, target_type, ast::CastKind::FloatToInt);
    return true;
  }

  // Integer to float expansion
  if (target_type->IsFloatType() && (*expr)->GetType()->IsIntegerType()) {
    *expr = ImplCastExprToType(*expr, target_type, ast::CastKind::IntToFloat);
    return true;
  }

  // Convert *[N]Type to [*]Type
  if (auto *target_arr = target_type->SafeAs<ast::ArrayType>()) {
    if (auto *expr_base = (*expr)->GetType()->GetPointeeType()) {
      if (auto *expr_arr = expr_base->SafeAs<ast::ArrayType>()) {
        if (target_arr->HasUnknownLength() && expr_arr->HasKnownLength()) {
          *expr = ImplCastExprToType(*expr, target_type, ast::CastKind::BitCast);
          return true;
        }
      }
    }
  }

  // *T to *U
  if (target_type->IsPointerType() || (*expr)->GetType()->IsPointerType()) {
    *expr = ImplCastExprToType(*expr, target_type, ast::CastKind::BitCast);
    return true;
  }

  // SQL bool to primitive bool
  if (target_type->IsBoolType() && (*expr)->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    *expr = ImplCastExprToType(*expr, target_type, ast::CastKind::SqlBoolToBool);
    return true;
  }

  // Not a valid assignment
  return false;
}

}  // namespace noisepage::execution::sema
