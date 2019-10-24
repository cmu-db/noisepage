#include "execution/sema/sema.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

#include "loggers/execution_logger.h"

namespace terrier::execution::sema {

void Sema::VisitBadExpr(ast::BadExpr *node) { TERRIER_ASSERT(false, "Bad expression in type checker!"); }

void Sema::VisitBinaryOpExpr(ast::BinaryOpExpr *node) {
  ast::Type *left_type = Resolve(node->Left());
  ast::Type *right_type = Resolve(node->Right());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::AND:
    case parsing::Token::Type::OR: {
      // NOLINTNEXTLINE
      auto [result_type, left, right] = CheckLogicalOperands(node->Op(), node->Position(), node->Left(), node->Right());
      node->SetType(result_type);
      if (node->Left() != left) node->SetLeft(left);
      if (node->Right() != right) node->SetRight(right);
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
      // NOLINTNEXTLINE
      auto [result_type, left, right] =
          CheckArithmeticOperands(node->Op(), node->Position(), node->Left(), node->Right());
      node->SetType(result_type);
      if (node->Left() != left) node->SetLeft(left);
      if (node->Right() != right) node->SetRight(right);
      break;
    }
    default: {
      EXECUTION_LOG_ERROR("{} is not a binary operation!", parsing::Token::GetString(node->Op()));
    }
  }
}

void Sema::VisitComparisonOpExpr(ast::ComparisonOpExpr *node) {
  ast::Type *left_type = Resolve(node->Left());
  ast::Type *right_type = Resolve(node->Right());

  if (left_type == nullptr || right_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::BANG_EQUAL:
    case parsing::Token::Type::EQUAL_EQUAL:
    case parsing::Token::Type::GREATER:
    case parsing::Token::Type::GREATER_EQUAL:
    case parsing::Token::Type::LESS:
    case parsing::Token::Type::LESS_EQUAL: {
      // NOLINTNEXTLINE
      auto [result_type, left, right] =
          CheckComparisonOperands(node->Op(), node->Position(), node->Left(), node->Right());
      node->SetType(result_type);
      if (node->Left() != left) node->SetLeft(left);
      if (node->Right() != right) node->SetRight(right);
      break;
    }
    default: {
      EXECUTION_LOG_ERROR("{} is not a comparison operation", parsing::Token::GetString(node->Op()));
    }
  }
}

void Sema::VisitCallExpr(ast::CallExpr *node) {
  // If the call claims to be to a builtin, validate it
  if (node->GetCallKind() == ast::CallExpr::CallKind::Builtin) {
    CheckBuiltinCall(node);
    return;
  }

  // Resolve the function type
  ast::Type *type = Resolve(node->Function());
  if (type == nullptr) {
    return;
  }

  // Check that the resolved function type is actually a function
  auto *func_type = type->SafeAs<ast::FunctionType>();
  if (func_type == nullptr) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kNonFunction);
    return;
  }

  // Check argument count matches
  if (!CheckArgCount(node, func_type->NumParams())) {
    return;
  }

  // Resolve function arguments
  for (auto *arg : node->Arguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Check args
  bool has_errors = false;

  const auto &actual_args = node->Arguments();
  for (uint32_t arg_num = 0; arg_num < actual_args.size(); arg_num++) {
    ast::Type *expected_type = func_type->Params()[arg_num].type_;
    ast::Expr *arg = actual_args[arg_num];

    // Function application simplifies to performing an assignment of the
    // actual call arguments to the function parameters. Do the check now, which
    // may apply an implicit cast to make the assignment work.
    if (!CheckAssignmentConstraints(expected_type, &arg)) {
      has_errors = true;
      error_reporter_->Report(arg->Position(), ErrorMessages::kIncorrectCallArgType, node->GetFuncName(), expected_type,
                              arg_num, arg->GetType());
      continue;
    }

    // If the check applied an implicit cast, set the argument
    if (arg != actual_args[arg_num]) {
      node->SetArgument(arg_num, arg);
    }
  }

  if (has_errors) {
    return;
  }

  // Looks good ...
  node->SetType(func_type->ReturnType());
}

void Sema::VisitFunctionLitExpr(ast::FunctionLitExpr *node) {
  // Resolve the type, if not resolved already
  if (auto *type = node->TypeRepr()->GetType(); type == nullptr) {
    type = Resolve(node->TypeRepr());
    if (type == nullptr) {
      return;
    }
  }

  // Good function type, insert into node
  auto *func_type = node->TypeRepr()->GetType()->As<ast::FunctionType>();
  node->SetType(func_type);

  // The function scope
  FunctionSemaScope function_scope(this, node);

  // Declare function parameters in scope
  for (const auto &param : func_type->Params()) {
    CurrentScope()->Declare(param.name_, param.type_);
  }

  // Recurse into the function body
  Visit(node->Body());

  // Check the return value. We allow functions to be empty or elide a final
  // "return" statement only if the function has a "nil" return type. In this
  // case, we automatically insert a "return" statement.
  if (node->IsEmpty() || !ast::Stmt::IsTerminating(node->Body())) {
    if (!func_type->ReturnType()->IsNilType()) {
      GetErrorReporter()->Report(node->Body()->RightBracePosition(), ErrorMessages::kMissingReturn);
      return;
    }

    ast::ReturnStmt *empty_ret = GetContext()->NodeFactory()->NewReturnStmt(node->Position(), nullptr);
    node->Body()->Statements().push_back(empty_ret);
  }
}

void Sema::VisitIdentifierExpr(ast::IdentifierExpr *node) {
  // Check the current context
  if (auto *type = CurrentScope()->Lookup(node->Name())) {
    node->SetType(type);
    return;
  }

  // Check the builtin types
  if (auto *type = GetContext()->LookupBuiltinType(node->Name())) {
    node->SetType(type);
    return;
  }

  // Error
  GetErrorReporter()->Report(node->Position(), ErrorMessages::kUndefinedVariable, node->Name());
}

void Sema::VisitImplicitCastExpr(ast::ImplicitCastExpr *node) {
  throw std::runtime_error("Should never perform semantic checking on implicit cast expressions");
}

void Sema::VisitIndexExpr(ast::IndexExpr *node) {
  ast::Type *obj_type = Resolve(node->Object());
  ast::Type *index_type = Resolve(node->Index());

  if (obj_type == nullptr || index_type == nullptr) {
    // Error
    return;
  }

  if (!obj_type->IsArrayType() && !obj_type->IsMapType()) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidIndexOperation, obj_type);
    return;
  }

  if (!index_type->IsIntegerType()) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidArrayIndexValue);
    return;
  }

  if (auto *arr_type = obj_type->SafeAs<ast::ArrayType>()) {
    node->SetType(arr_type->ElementType());
  } else {
    node->SetType(obj_type->As<ast::MapType>()->ValueType());
  }
}

void Sema::VisitLitExpr(ast::LitExpr *node) {
  switch (node->LiteralKind()) {
    case ast::LitExpr::LitKind::Nil: {
      node->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Nil));
      break;
    }
    case ast::LitExpr::LitKind::Boolean: {
      node->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Bool));
      break;
    }
    case ast::LitExpr::LitKind::Float: {
      // Literal floats default to float64
      node->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Float64));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      // Literal integers default to int64
      node->SetType(ast::BuiltinType::Get(GetContext(), ast::BuiltinType::Int64));
      break;
    }
    case ast::LitExpr::LitKind::String: {
      node->SetType(ast::StringType::Get(GetContext()));
      break;
    }
  }
}

void Sema::VisitUnaryOpExpr(ast::UnaryOpExpr *node) {
  // Resolve the type of the sub expression
  ast::Type *expr_type = Resolve(node->Expression());

  if (expr_type == nullptr) {
    // Some error occurred
    return;
  }

  switch (node->Op()) {
    case parsing::Token::Type::BANG: {
      if (!expr_type->IsBoolType()) {
        GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(), expr_type);
        return;
      }

      node->SetType(expr_type);
      break;
    }
    case parsing::Token::Type::MINUS: {
      if (!expr_type->IsArithmetic()) {
        GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(), expr_type);
        return;
      }

      node->SetType(expr_type);
      break;
    }
    case parsing::Token::Type::STAR: {
      if (!expr_type->IsPointerType()) {
        GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(), expr_type);
        return;
      }

      node->SetType(expr_type->As<ast::PointerType>()->Base());
      break;
    }
    case parsing::Token::Type::AMPERSAND: {
      if (expr_type->IsFunctionType()) {
        GetErrorReporter()->Report(node->Position(), ErrorMessages::kInvalidOperation, node->Op(), expr_type);
        return;
      }

      node->SetType(expr_type->PointerTo());
      break;
    }
    default: {
      UNREACHABLE("Impossible unary operation!");
    }
  }
}

void Sema::VisitMemberExpr(ast::MemberExpr *node) {
  ast::Type *obj_type = Resolve(node->Object());

  if (obj_type == nullptr) {
    // Some error
    return;
  }

  if (auto *pointer_type = obj_type->SafeAs<ast::PointerType>()) {
    obj_type = pointer_type->Base();
  }

  if (!obj_type->IsStructType()) {
    GetErrorReporter()->Report(node->Position(), ErrorMessages::kMemberObjectNotComposite, obj_type);
    return;
  }

  if (!node->Member()->IsIdentifierExpr()) {
    GetErrorReporter()->Report(node->Member()->Position(), ErrorMessages::kExpectedIdentifierForMember);
    return;
  }

  ast::Identifier member = node->Member()->As<ast::IdentifierExpr>()->Name();

  ast::Type *member_type = obj_type->As<ast::StructType>()->LookupFieldByName(member);

  if (member_type == nullptr) {
    GetErrorReporter()->Report(node->Member()->Position(), ErrorMessages::kFieldObjectDoesNotExist, member, obj_type);
    return;
  }

  node->SetType(member_type);
}

}  // namespace terrier::execution::sema
