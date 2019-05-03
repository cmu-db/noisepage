#include "execution/sema/sema.h"

#include <utility>

#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace tpl::sema {

void Sema::VisitArrayTypeRepr(ast::ArrayTypeRepr *node) {
  uint64_t actual_length = 0;
  if (node->length() != nullptr) {
    auto *len_expr = node->length()->SafeAs<ast::LitExpr>();
    if (len_expr == nullptr || len_expr->literal_kind() != ast::LitExpr::LitKind::Int) {
      error_reporter()->Report(node->length()->position(), ErrorMessages::kNonIntegerArrayLength);
      return;
    }

    auto len = len_expr->int32_val();
    if (len < 0) {
      error_reporter()->Report(node->length()->position(), ErrorMessages::kNegativeArrayLength);
      return;
    }

    actual_length = static_cast<uint64_t>(len);
  }

  ast::Type *elem_type = Resolve(node->element_type());

  if (elem_type == nullptr) {
    return;
  }

  node->set_type(ast::ArrayType::Get(actual_length, elem_type));
}

void Sema::VisitFunctionTypeRepr(ast::FunctionTypeRepr *node) {
  // Handle parameters
  util::RegionVector<ast::Field> param_types(context()->region());
  for (auto *param : node->parameters()) {
    Visit(param);
    ast::Type *param_type = param->type_repr()->type();
    if (param_type == nullptr) {
      return;
    }
    param_types.emplace_back(param->name(), param_type);
  }

  // Handle return type
  ast::Type *ret = Resolve(node->return_type());
  if (ret == nullptr) {
    return;
  }

  // Create type
  ast::FunctionType *func_type = ast::FunctionType::Get(std::move(param_types), ret);
  node->set_type(func_type);
}

void Sema::VisitPointerTypeRepr(ast::PointerTypeRepr *node) {
  ast::Type *base_type = Resolve(node->base());
  if (base_type == nullptr) {
    return;
  }

  node->set_type(base_type->PointerTo());
}

void Sema::VisitStructTypeRepr(ast::StructTypeRepr *node) {
  util::RegionVector<ast::Field> field_types(context()->region());
  for (auto *field : node->fields()) {
    Visit(field);
    ast::Type *field_type = field->type_repr()->type();
    if (field_type == nullptr) {
      return;
    }
    field_types.emplace_back(field->name(), field_type);
  }

  node->set_type(ast::StructType::Get(context(), std::move(field_types)));
}

void Sema::VisitMapTypeRepr(ast::MapTypeRepr *node) {
  ast::Type *key_type = Resolve(node->key());
  ast::Type *value_type = Resolve(node->val());

  if (key_type == nullptr || value_type == nullptr) {
    // Error
    return;
  }

  node->set_type(ast::MapType::Get(key_type, value_type));
}

}  // namespace tpl::sema
