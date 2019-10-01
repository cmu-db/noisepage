#include "execution/sema/sema.h"

#include <utility>

#include "catalog/schema.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"

namespace terrier::execution::sema {

Sema::Sema(ast::Context *ctx)
    : ctx_(ctx),
      error_reporter_(ctx->GetErrorReporter()),
      scope_(nullptr),
      num_cached_scopes_(0),
      curr_func_(nullptr) {}

// Main entry point to semantic analysis and type checking an AST
bool Sema::Run(ast::AstNode *root) {
  Visit(root);
  return GetErrorReporter()->HasErrors();
}

ast::Type *Sema::GetBuiltinType(const uint16_t builtin_kind) {
  return ast::BuiltinType::Get(GetContext(), static_cast<ast::BuiltinType::Kind>(builtin_kind));
}

}  // namespace terrier::execution::sema
