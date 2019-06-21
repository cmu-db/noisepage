#include "execution/sema/sema.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

#include "loggers/execution_logger.h"

namespace tpl::sema {

void Sema::VisitBadExpr(ast::BadExpr *node) { TPL_ASSERT(false, "Bad expression in type checker!"); }

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

void Sema::VisitCallExpr(ast::CallExpr *node) {
  // If the call claims to be to a builtin, validate it
  if (node->call_kind() == ast::CallExpr::CallKind::Builtin) {
    CheckBuiltinCall(node);
    return;
  }

  // Resolve the function type
  ast::Type *type = Resolve(node->function());
  if (type == nullptr) {
    return;
  }

  // Check that the resolved function type is actually a function
  auto *func_type = type->SafeAs<ast::FunctionType>();
  if (func_type == nullptr) {
    error_reporter()->Report(node->position(), ErrorMessages::kNonFunction);
    return;
  }

  // Check argument count matches
  if (!CheckArgCount(node, func_type->num_params())) {
    return;
  }

  // Resolve function arguments
  for (auto *arg : node->arguments()) {
    ast::Type *arg_type = Resolve(arg);
    if (arg_type == nullptr) {
      return;
    }
  }

  // Check args
  bool has_errors = false;

  const auto &actual_args = node->arguments();
  for (u32 arg_num = 0; arg_num < actual_args.size(); arg_num++) {
    ast::Type *expected_type = func_type->params()[arg_num].type;
    ast::Expr *arg = actual_args[arg_num];

    // Function application simplifies to performing an assignment of the
    // actual call arguments to the function parameters. Do the check now, which
    // may apply an implicit cast to make the assignment work.
    if (!CheckAssignmentConstraints(expected_type, &arg)) {
      has_errors = true;
      error_reporter_->Report(arg->position(), ErrorMessages::kIncorrectCallArgType, node->GetFuncName(), expected_type,
                              arg_num, arg->type());
      continue;
    }

    // If the check applied an implicit cast, set the argument
    if (arg != actual_args[arg_num]) {
      node->set_argument(arg_num, arg);
    }
  }

  if (has_errors) {
    return;
  }

  // Looks good ...
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
      // Literal floats default to float64
      node->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Float64));
      break;
    }
    case ast::LitExpr::LitKind::Int: {
      // Literal integers default to int64
      node->set_type(ast::BuiltinType::Get(context(), ast::BuiltinType::Int64));
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
