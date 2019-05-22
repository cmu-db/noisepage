#include "execution/sema/sema.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

#include "loggers/execution_logger.h"

namespace tpl::sema {

void Sema::VisitBadExpr(ast::BadExpr *node) { TPL_ASSERT(false, "Bad expression in type checker!"); }

Sema::CheckResult Sema::CheckLogicalOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                             ast::Expr *right) {
  // Cache a primitive boolean type here because it's used throughout this func
  ast::Type *const bool_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);

  // If either the left or right expressions are SQL booleans, implicitly cast
  // to a primitive boolean type in preparation for the comparison
  if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    left =
        context()->node_factory()->NewImplicitCastExpr(left->position(), ast::CastKind::SqlBoolToBool, bool_type, left);
    left->set_type(bool_type);
  }

  if (right->type()->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
    right = context()->node_factory()->NewImplicitCastExpr(right->position(), ast::CastKind::SqlBoolToBool, bool_type,
                                                           right);
    right->set_type(bool_type);
  }

  // If both input expressions are primitive booleans, we're done
  if (left->type()->IsBoolType() && right->type()->IsBoolType()) {
    return {bool_type, left, right};
  }

  // Okay, there's an error ...

  error_reporter()->Report(pos, ErrorMessages::kMismatchedTypesToBinary, left->type(), right->type(), op);

  return {nullptr, left, right};
}

Sema::CheckResult Sema::CheckArithmeticOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                                ast::Expr *right) {
  // If neither input expression is arithmetic, it's an ill-formed operation
  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(), right->type());
    return {nullptr, left, right};
  }

  // If the input types are the same, we don't need to do any work
  if (left->type() == right->type()) {
    return {left->type(), left, right};
  }

  // Cache a SQL integer type here because it's used throughout this function
  ast::Type *const sql_int_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Integer);

  // If either the left or right types aren't SQL integers, cast them to SQL
  // integers to perform arithmetic operation
  if (!right->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    right = context()->node_factory()->NewImplicitCastExpr(right->position(), ast::CastKind::IntToSqlInt, sql_int_type,
                                                           right);
    right->set_type(sql_int_type);
  }

  if (!left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    left = context()->node_factory()->NewImplicitCastExpr(left->position(), ast::CastKind::IntToSqlInt, sql_int_type,
                                                          left);
    left->set_type(sql_int_type);
  }

  // Done
  return {sql_int_type, left, right};
}

Sema::CheckResult Sema::CheckComparisonOperands(parsing::Token::Type op, const SourcePosition &pos, ast::Expr *left,
                                                ast::Expr *right) {
  // If neither input expression is arithmetic, it's an ill-formed operation
  if (!left->type()->IsArithmetic() || !right->type()->IsArithmetic()) {
    error_reporter()->Report(pos, ErrorMessages::kIllegalTypesForBinary, op, left->type(), right->type());
    return {nullptr, left, right};
  }

  // If the input types are the same, we don't need to do any work
  if (left->type() == right->type()) {
    ast::Type *ret_type = nullptr;
    if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer) ||
        left->type()->IsSpecificBuiltin(ast::BuiltinType::Decimal)) {
      ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Boolean);
    } else {
      ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);
    }
    return {ret_type, left, right};
  }

  // Cache a SQL integer type here because it's used throughout this function
  ast::Type *const sql_int_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Integer);

  // If either the left or right types aren't SQL integers, cast them up to one
  if (!right->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    right = context()->node_factory()->NewImplicitCastExpr(right->position(), ast::CastKind::IntToSqlInt, sql_int_type,
                                                           right);
    right->set_type(sql_int_type);
  }

  if (!left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer)) {
    left = context()->node_factory()->NewImplicitCastExpr(left->position(), ast::CastKind::IntToSqlInt, sql_int_type,
                                                          left);
    left->set_type(sql_int_type);
  }

  // Done
  ast::Type *ret_type = nullptr;
  if (left->type()->IsSpecificBuiltin(ast::BuiltinType::Integer) ||
      left->type()->IsSpecificBuiltin(ast::BuiltinType::Decimal)) {
    ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Boolean);
  } else {
    ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Bool);
  }
  return {ret_type, left, right};
}

void Sema::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  ast::Type *left_type = Resolve(node->left());
  ast::Type *right_type = Resolve(node->right());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      // clang-tidy still has false-positives of unused fields in structured bindings
      // NOLINTNEXTLINE
      auto [result_type, left, right] = CheckLogicalOperands(node->op(), node->position(), node->left(), node->right());
      node->set_type(result_type);
      if (node->left() != left) node->set_left(left);
      if (node->right() != right) node->set_right(right);
      break;
    }
    case parsing::Token::Type::AMPERSAND:
    case parsing::Token::Type::BIT_XOR:
    case parsing::Token::Type::BIT_OR:
    case parsing::Token::Type::PLUS:
    case parsing::Token::Type::MINUS:
    case parsing::Token::Type::STAR:
    case parsing::Token::Type::SLASH:
    case parsing::Token::Type::PERCENT: {
      // clang-tidy still has false-positives of unused fields in structured bindings
      // NOLINTNEXTLINE
      auto [result_type, left, right] =
          CheckArithmeticOperands(node->op(), node->position(), node->left(), node->right());
      node->set_type(result_type);
      if (node->left() != left) node->set_left(left);
      if (node->right() != right) node->set_right(right);
      break;
    }
    default: { EXECUTION_LOG_ERROR("{} is not a binary operation!", parsing::Token::GetString(node->op())); }
  }
}

void Sema::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  ast::Type *left_type = Resolve(node->left());
  ast::Type *right_type = Resolve(node->right());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::BANG_EQUAL:
    case parsing::Token::Type::EQUAL_EQUAL:
    case parsing::Token::Type::GREATER:
    case parsing::Token::Type::GREATER_EQUAL:
    case parsing::Token::Type::LESS:
    case parsing::Token::Type::LESS_EQUAL: {
      // clang-tidy still has false-positives of unused fields in structured bindings
      // NOLINTNEXTLINE
      auto [result_type, left, right] =
          CheckComparisonOperands(node->op(), node->position(), node->left(), node->right());
      node->set_type(result_type);
      if (node->left() != left) node->set_left(left);
      if (node->right() != right) node->set_right(right);
      break;
    }
    default: { EXECUTION_LOG_ERROR("{} is not a comparison operation", parsing::Token::GetString(node->op())); }
  }
}

void Sema::CheckBuiltinMapCall(UNUSED ast::CallExpr *call) {}

void Sema::CheckBuiltinSqlConversionCall(ast::CallExpr *call, ast::Builtin builtin) {
  auto input_type = call->arguments()[0]->type();
  switch (builtin) {
    case ast::Builtin::BoolToSql: {
      if (!input_type->IsSpecificBuiltin(ast::BuiltinType::Bool)) {
        error_reporter()->Report(call->position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Boolean));
      break;
    }
    case ast::Builtin::IntToSql: {
      if (!input_type->IsIntegerType()) {
        error_reporter()->Report(call->position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Integer));
      break;
    }
    case ast::Builtin::FloatToSql: {
      if (!input_type->IsFloatType()) {
        error_reporter()->Report(call->position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Real));
      break;
    }
    case ast::Builtin::SqlToBool: {
      if (!input_type->IsSpecificBuiltin(ast::BuiltinType::Boolean)) {
        error_reporter()->Report(call->position(), ErrorMessages::kInvalidSqlCastToBool, input_type);
        return;
      }
      call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Bool));
      break;
    }
    default: { UNREACHABLE("Impossible SQL conversion call"); }
  }
}

void Sema::CheckBuiltinFilterCall(ast::CallExpr *call) {
  if (call->num_args() != 3) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 3,
                             call->num_args());
    return;
  }

  // The first call argument must be a pointer to a ProjectedColumnsIterator
  ast::Type *pci = call->arguments()[0]->type()->GetPointeeType();
  if (pci == nullptr || !pci->IsSpecificBuiltin(ast::BuiltinType::ProjectedColumnsIterator)) {
    pci = ast::BuiltinType::Get(context(), ast::BuiltinType::ProjectedColumnsIterator);
    error_reporter()->Report(call->position(), ErrorMessages::kIncorrectCallArgType, call->arguments()[0]->type(), pci,
                             call->GetFuncName());
    return;
  }

  // The second call argument must be a string
  if (!call->arguments()[1]->type()->IsStringType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kIncorrectCallArgType, call->arguments()[1]->type(),
                             ast::StringType::Get(context()), call->GetFuncName());
  }

  // Set return type
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Int32));
}

void Sema::CheckBuiltinJoinHashTableInit(ast::CallExpr *call) {
  if (call->num_args() != 3) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 3,
                             call->num_args());
    return;
  }

  // First argument must be a pointer to a JoinHashTable
  auto *jht_type = call->arguments()[0]->type()->GetPointeeType();
  if (jht_type == nullptr || !jht_type->IsSpecificBuiltin(ast::BuiltinType::JoinHashTable)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableInit, call->arguments()[0]->type(), 0);
    return;
  }

  // Second argument must be a pointer to a RegionAllocator
  auto *region_type = call->arguments()[1]->type()->GetPointeeType();
  if (region_type == nullptr || !region_type->IsSpecificBuiltin(ast::BuiltinType::RegionAlloc)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableInit, call->arguments()[1]->type(), 1);
    return;
  }

  // Third and last argument must be a 32-bit number representing the tuple size
  auto *entry_size_type = call->arguments()[2]->type();
  if (!entry_size_type->IsIntegerType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableInit, call->arguments()[2]->type(), 2);
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinJoinHashTableInsert(ast::CallExpr *call) {
  if (call->num_args() != 2) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 2,
                             call->num_args());
    return;
  }

  // First argument is a pointer to a JoinHashTable
  auto *jht_type = call->arguments()[0]->type()->GetPointeeType();
  if (jht_type == nullptr || !jht_type->IsSpecificBuiltin(ast::BuiltinType::JoinHashTable)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableInsert, call->arguments()[0]->type(),
                             0);
    return;
  }

  // Second argument is a hash value
  if (!call->arguments()[1]->type()->IsIntegerType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableInsert, call->arguments()[1]->type(),
                             1);
    return;
  }

  // This call returns nothing
  ast::Type *ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Uint8)->PointerTo();
  call->set_type(ret_type);
}

void Sema::CheckBuiltinJoinHashTableBuild(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  // The first and only argument must be a pointer to a JoinHashTable
  auto *jht_type = call->arguments()[0]->type()->GetPointeeType();
  if (jht_type == nullptr || !jht_type->IsSpecificBuiltin(ast::BuiltinType::JoinHashTable)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableBuild, call->arguments()[0]->type());
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinJoinHashTableFree(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  // The first and only argument must be a pointer to a JoinHashTable
  ast::Type *const jht_type = call->arguments()[0]->type()->GetPointeeType();
  if (jht_type == nullptr || !jht_type->IsSpecificBuiltin(ast::BuiltinType::JoinHashTable)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableBuild, call->arguments()[0]->type());
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinRegionCall(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  ast::Type *const region_type = call->arguments()[0]->type()->GetPointeeType();
  if (region_type == nullptr || !region_type->IsSpecificBuiltin(ast::BuiltinType::RegionAlloc)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToRegionFunction, call->arguments()[0]->type());
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSizeOfCall(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  // This call returns an unsigned 32-bit value for the size of the type
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Uint32));
}

void Sema::CheckBuiltinPtrCastCall(ast::CallExpr *call) {
  if (call->num_args() != 2) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 2,
                             call->num_args());
    return;
  }

  // The first argument will be a UnaryOpExpr with the '*' (star) op. This is
  // because parsing function calls assumes expression arguments, not types. So,
  // something like '*Type', which would be the first argument to @ptrCast, will
  // get parsed as a dereference expression before a type expression.
  // TODO(pmenon): Fix the above to parse correctly

  auto unary_op = call->arguments()[0]->SafeAs<ast::UnaryOpExpr>();
  if (unary_op == nullptr || unary_op->op() != parsing::Token::Type::STAR) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToPtrCast, call->arguments()[0]->type(), 1);
    return;
  }

  // Replace the unary with a PointerTypeRepr node and resolve it
  call->set_argument(0, context()->node_factory()->NewPointerType(call->arguments()[0]->position(), unary_op->expr()));

  for (auto *arg : call->arguments()) {
    auto *resolved_type = Resolve(arg);
    if (resolved_type == nullptr) {
      return;
    }
  }

  // Both arguments must be pointer types
  if (!call->arguments()[0]->type()->IsPointerType() || !call->arguments()[1]->type()->IsPointerType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToPtrCast, call->arguments()[0]->type(), 1);
    return;
  }

  // Apply the cast
  call->set_type(call->arguments()[0]->type());
}

void Sema::CheckBuiltinSorterInit(ast::CallExpr *call) {
  if (call->num_args() != 4) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 4,
                             call->num_args());
    return;
  }

  // First argument must be a pointer to a Sorter
  auto *sorter_type = call->arguments()[0]->type()->GetPointeeType();
  if (sorter_type == nullptr || !sorter_type->IsSpecificBuiltin(ast::BuiltinType::Sorter)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToSorterInit, call->arguments()[0]->type(), 0);
    return;
  }

  // Second argument must be a pointer to a RegionAllocator
  auto *region_type = call->arguments()[1]->type()->GetPointeeType();
  if (region_type == nullptr || !region_type->IsSpecificBuiltin(ast::BuiltinType::RegionAlloc)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToHashTableInit, call->arguments()[1]->type(), 1);
    return;
  }

  // Second argument must be a function
  auto *cmp_func_type = call->arguments()[2]->type()->SafeAs<ast::FunctionType>();
  if (!cmp_func_type->IsFunctionType() || cmp_func_type->num_params() != 2 ||
      !cmp_func_type->return_type()->IsIntegerType() || !cmp_func_type->params()[0].type->IsPointerType() ||
      !cmp_func_type->params()[1].type->IsPointerType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToSorterInit, call->arguments()[2]->type(), 2);
    return;
  }

  // Third and last argument must be a 32-bit number representing the tuple size
  auto *entry_size_type = call->arguments()[3]->type();
  if (!entry_size_type->IsIntegerType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToSorterInit, call->arguments()[3]->type(), 3);
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterInsert(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  // First argument must be a pointer to a Sorter
  auto *sorter_type = call->arguments()[0]->type()->GetPointeeType();
  if (sorter_type == nullptr || !sorter_type->IsSpecificBuiltin(ast::BuiltinType::Sorter)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToSorterInsert, call->arguments()[0]->type(), 0);
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Uint8)->PointerTo());
}

void Sema::CheckBuiltinSorterSort(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 3,
                             call->num_args());
    return;
  }

  // First argument must be a pointer to a Sorter
  auto *sorter_type = call->arguments()[0]->type()->GetPointeeType();
  if (sorter_type == nullptr || !sorter_type->IsSpecificBuiltin(ast::BuiltinType::Sorter)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToSorterSort, call->arguments()[0]->type(), 0);
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinSorterFree(ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  // First argument must be a pointer to a Sorter
  auto *sorter_type = call->arguments()[0]->type()->GetPointeeType();
  if (sorter_type == nullptr || !sorter_type->IsSpecificBuiltin(ast::BuiltinType::Sorter)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToSorterFree, call->arguments()[0]->type(), 0);
    return;
  }

  // This call returns nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinOutputAlloc(tpl::ast::CallExpr *call) {
  if (call->num_args() != 0) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 0,
                             call->num_args());
    return;
  }
  // Return a byte*
  ast::Type *ret_type = ast::BuiltinType::Get(context(), ast::BuiltinType::Uint8)->PointerTo();
  call->set_type(ret_type);
}

void Sema::CheckBuiltinOutputAdvance(tpl::ast::CallExpr *call) {
  if (call->num_args() != 0) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 0,
                             call->num_args());
    return;
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinOutputFinalize(tpl::ast::CallExpr *call) {
  if (call->num_args() != 0) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 0,
                             call->num_args());
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinInsert(tpl::ast::CallExpr *call) {
  if (call->num_args() != 3) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 3,
                             call->num_args());
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinOutputSetNull(tpl::ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }

  // The argument should be an integer
  auto *entry_size_type = call->arguments()[0]->type();
  // TODO(Amadou): Write an error specific to this function
  if (!entry_size_type->IsIntegerType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToOutputSetNull, call->arguments()[0]->type(), 0);
    return;
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorInit(tpl::ast::CallExpr *call) {
  if (call->num_args() != 2) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 2,
                             call->num_args());
    return;
  }

  // First argument must be a pointer to a IndexIterator
  auto *index_type = call->arguments()[0]->type()->GetPointeeType();
  if (index_type == nullptr || !index_type->IsSpecificBuiltin(ast::BuiltinType::IndexIterator)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToIndexIteratorInit, call->arguments()[0]->type(),
                             0);
    return;
  }
  // The second call argument must be a string
  if (!call->arguments()[1]->type()->IsStringType()) {
    error_reporter()->Report(call->position(), ErrorMessages::kIncorrectCallArgType, call->arguments()[1]->type(),
                             ast::StringType::Get(context()), call->GetFuncName());
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorScanKey(tpl::ast::CallExpr *call) {
  if (call->num_args() != 2) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 2,
                             call->num_args());
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto *index_type = call->arguments()[0]->type()->GetPointeeType();
  if (index_type == nullptr || !index_type->IsSpecificBuiltin(ast::BuiltinType::IndexIterator)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToIndexIteratorScanKey,
                             call->arguments()[0]->type(), 0);
    return;
  }
  // Second argument should be a byte array
  auto *byte_type = call->arguments()[1]->type()->GetPointeeType();
  if (byte_type == nullptr || !byte_type->IsSpecificBuiltin(ast::BuiltinType::Int8)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToIndexIteratorScanKey,
                             call->arguments()[1]->type(), 1);
    return;
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinIndexIteratorFree(tpl::ast::CallExpr *call) {
  if (call->num_args() != 1) {
    error_reporter()->Report(call->position(), ErrorMessages::kMismatchedCallArgs, call->GetFuncName(), 1,
                             call->num_args());
    return;
  }
  // First argument must be a pointer to a IndexIterator
  auto *index_type = call->arguments()[0]->type()->GetPointeeType();
  if (index_type == nullptr || !index_type->IsSpecificBuiltin(ast::BuiltinType::IndexIterator)) {
    error_reporter()->Report(call->position(), ErrorMessages::kBadArgToIndexIteratorFree, call->arguments()[0]->type(),
                             0);
    return;
  }
  // Return nothing
  call->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
}

void Sema::CheckBuiltinCall(ast::CallExpr *call, ast::Builtin builtin) {
  if (builtin == ast::Builtin::PtrCast) {
    CheckBuiltinPtrCastCall(call);
    return;
  }

  // First, resolve all call arguments. If any fail, exit immediately.
  for (auto *arg : call->arguments()) {
    auto *resolved_type = Resolve(arg);
    if (resolved_type == nullptr) {
      return;
    }
  }

  switch (builtin) {
    case ast::Builtin::BoolToSql:
    case ast::Builtin::IntToSql:
    case ast::Builtin::FloatToSql:
    case ast::Builtin::SqlToBool: {
      CheckBuiltinSqlConversionCall(call, builtin);
      break;
    }
    case ast::Builtin::FilterEq:
    case ast::Builtin::FilterGe:
    case ast::Builtin::FilterGt:
    case ast::Builtin::FilterLt:
    case ast::Builtin::FilterLe: {
      CheckBuiltinFilterCall(call);
      break;
    }
    case ast::Builtin::RegionInit:
    case ast::Builtin::RegionFree: {
      CheckBuiltinRegionCall(call);
      break;
    }
    case ast::Builtin::JoinHashTableInit: {
      CheckBuiltinJoinHashTableInit(call);
      break;
    }
    case ast::Builtin::JoinHashTableInsert: {
      CheckBuiltinJoinHashTableInsert(call);
      break;
    }
    case ast::Builtin::JoinHashTableBuild: {
      CheckBuiltinJoinHashTableBuild(call);
      break;
    }
    case ast::Builtin::JoinHashTableFree: {
      CheckBuiltinJoinHashTableFree(call);
      break;
    }
    case ast::Builtin::SorterInit: {
      CheckBuiltinSorterInit(call);
      break;
    }
    case ast::Builtin::SorterInsert: {
      CheckBuiltinSorterInsert(call);
      break;
    }
    case ast::Builtin::SorterSort: {
      CheckBuiltinSorterSort(call);
      break;
    }
    case ast::Builtin::SorterFree: {
      CheckBuiltinSorterFree(call);
      break;
    }
    case ast::Builtin::Map: {
      CheckBuiltinMapCall(call);
      break;
    }
    case ast::Builtin::SizeOf: {
      CheckBuiltinSizeOfCall(call);
      break;
    }
    case ast::Builtin::OutputAlloc: {
      CheckBuiltinOutputAlloc(call);
      break;
    }
    case ast::Builtin::OutputAdvance: {
      CheckBuiltinOutputAdvance(call);
      break;
    }
    case ast::Builtin::OutputSetNull: {
      CheckBuiltinOutputSetNull(call);
      break;
    }
    case ast::Builtin::OutputFinalize: {
      CheckBuiltinOutputFinalize(call);
      break;
    }
    case ast::Builtin::Insert: {
      CheckBuiltinInsert(call);
      break;
    }
    case ast::Builtin::IndexIteratorInit: {
      CheckBuiltinIndexIteratorInit(call);
      break;
    }
    case ast::Builtin::IndexIteratorScanKey: {
      CheckBuiltinIndexIteratorScanKey(call);
      break;
    }
    case ast::Builtin::IndexIteratorFree: {
      CheckBuiltinIndexIteratorFree(call);
      break;
    }
    default: {
      // No-op
      break;
    }
  }
}

void Sema::VisitCallExpr(ast::CallExpr *node) {
  // Is this a built in?
  if (ast::Builtin builtin; context()->IsBuiltinFunction(node->GetFuncName(), &builtin)) {
    // Mark this node as a call into a builtin function
    node->set_call_kind(ast::CallExpr::CallKind::Builtin);

    // Check it
    CheckBuiltinCall(node, builtin);

    // Finish
    return;
  }

  // Resolve the function type
  ast::Type *type = Resolve(node->function());

  if (type == nullptr) {
    // Some error occurred
    return;
  }

  if (!type->IsFunctionType()) {
    error_reporter()->Report(node->position(), ErrorMessages::kNonFunction);
    return;
  }

  // First, check to make sure we have the right number of function arguments
  auto *func_type = type->As<ast::FunctionType>();
  if (func_type->num_params() != node->num_args()) {
    error_reporter()->Report(node->position(), ErrorMessages::kMismatchedCallArgs, node->GetFuncName(),
                             func_type->num_params(), node->num_args());
    return;
  }

  // Now, let's resolve each function argument's type
  for (auto *arg : node->arguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Now, let's make sure the arguments match up
  const auto &actual_call_arg_types = node->arguments();
  const auto &expected_arg_params = func_type->params();
  for (size_t i = 0; i < actual_call_arg_types.size(); i++) {
    if (actual_call_arg_types[i]->type() != expected_arg_params[i].type) {
      error_reporter()->Report(node->position(), ErrorMessages::kIncorrectCallArgType, actual_call_arg_types[i]->type(),
                               expected_arg_params[i].type, node->GetFuncName());
      return;
    }
  }

  // All looks good ...
  node->set_call_kind(ast::CallExpr::CallKind::Regular);
  node->set_type(func_type->return_type());
}

void Sema::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  // Resolve the type, if not resolved already
  if (auto *type = node->type_repr()->type(); type == nullptr) {
    type = Resolve(node->type_repr());
    if (type == nullptr) {
      return;
    }
  }

  // Good function type, insert into node
  auto *func_type = node->type_repr()->type()->As<ast::FunctionType>();
  node->set_type(func_type);

  // The function scope
  FunctionSemaScope function_scope(this, node);

  // Declare function parameters in scope
  for (const auto &param : func_type->params()) {
    current_scope()->Declare(param.name, param.type);
  }

  // Recurse into the function body
  Visit(node->body());

  // Check the return value. We allow functions to be empty or elide a final
  // "return" statement only if the function has a "nil" return type. In this
  // case, we automatically insert a "return" statement.
  if (node->IsEmpty() || !ast::Stmt::IsTerminating(node->body())) {
    if (!func_type->return_type()->IsNilType()) {
      error_reporter()->Report(node->body()->right_brace_position(), ErrorMessages::kMissingReturn);
      return;
    }

    ast::ReturnStmt *empty_ret = context()->node_factory()->NewReturnStmt(node->position(), nullptr);
    node->body()->statements().push_back(empty_ret);
  }
}

void Sema::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Check the current context
  if (auto *type = current_scope()->Lookup(node->name())) {
    node->set_type(type);
    return;
  }

  // Check the builtin types
  if (auto *type = context()->LookupBuiltinType(node->name())) {
    node->set_type(type);
    return;
  }

  // Error
  error_reporter()->Report(node->position(), ErrorMessages::kUndefinedVariable, node->name());
}

void Sema::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  throw std::runtime_error("Should never perform semantic checking on implicit cast expressions");
}

void Sema::VisitIndexExpr(ast::IndexExpr *node) {
  ast::Type *obj_type = Resolve(node->object());
  ast::Type *index_type = Resolve(node->index());

  if (obj_type == nullptr || index_type == nullptr) {
    // Error
    return;
  }

  if (!obj_type->IsArrayType() && !obj_type->IsMapType()) {
    error_reporter()->Report(node->position(), ErrorMessages::kInvalidIndexOperation, obj_type);
    return;
  }

  if (!index_type->IsIntegerType()) {
    error_reporter()->Report(node->position(), ErrorMessages::kInvalidArrayIndexValue);
    return;
  }

  if (auto *arr_type = obj_type->SafeAs<ast::ArrayType>()) {
    node->set_type(arr_type->element_type());
  } else {
    node->set_type(obj_type->As<ast::MapType>()->value_type());
  }
}

void Sema::VisitLitExpr(ast::LitExpr *node) {
  switch (node->literal_kind()) {
    case ast::LitExpr::LitKind::Nil: {
      node->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Nil));
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      node->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Bool));
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      // Literal floats default to float32
      node->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Float32));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      // Literal integers default to int32
      node->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Int32));
      break;
    }
    case ast::LitExpr::LitKind::String: {
      node->set_type(ast::StringType::Get(context()));
      break;
    }
  }
}

void Sema::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->expr());

  if (expr_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->op()) {
    case parsing::Token::Type::BANG: {
      if (!expr_type->IsBoolType()) {
        error_reporter()->Report(node->position(), ErrorMessages::kInvalidOperation, node->op(), expr_type);
        return;
      }

      node->set_type(expr_type);
      break;
    }
    case parsing::Token::Type::MINUS: {
      if (!expr_type->IsArithmetic()) {
        error_reporter()->Report(node->position(), ErrorMessages::kInvalidOperation, node->op(), expr_type);
        return;
      }

      node->set_type(expr_type);
      break;
    }
    case parsing::Token::Type::STAR: {
      if (!expr_type->IsPointerType()) {
        error_reporter()->Report(node->position(), ErrorMessages::kInvalidOperation, node->op(), expr_type);
        return;
      }

      node->set_type(expr_type->As<ast::PointerType>()->base());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      if (expr_type->IsFunctionType()) {
        error_reporter()->Report(node->position(), ErrorMessages::kInvalidOperation, node->op(), expr_type);
        return;
      }

      node->set_type(expr_type->PointerTo());
      break;
    }
    default: { UNREACHABLE("Impossible unary operation!"); }
  }
}

void Sema::VisitMemberExpr(ast::MemberExpr *node) {
  ast::Type *obj_type = Resolve(node->object());

  if (obj_type == nullptr) {
    // Some error
    return;
  }

  if (auto *pointer_type = obj_type->SafeAs<ast::PointerType>()) {
    obj_type = pointer_type->base();
  }

  if (!obj_type->IsStructType()) {
    error_reporter()->Report(node->position(), ErrorMessages::kMemberObjectNotComposite, obj_type);
    return;
  }

  if (!node->member()->IsIdentifierExpr()) {
    error_reporter()->Report(node->member()->position(), ErrorMessages::kExpectedIdentifierForMember);
    return;
  }

  ast::Identifier member = node->member()->As<ast::IdentifierExpr>()->name();

  ast::Type *member_type = obj_type->As<ast::StructType>()->LookupFieldByName(member);

  if (member_type == nullptr) {
    error_reporter()->Report(node->member()->position(), ErrorMessages::kFieldObjectDoesNotExist, member, obj_type);
    return;
  }

  node->set_type(member_type);
}

}  // namespace tpl::sema
