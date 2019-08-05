#pragma once
#include "execution/ast/context.h"
#include "execution/ast/ast.h"
#include "catalog/catalog_defs.h"
#include "catalog/catalog_accessor.h"

namespace terrier::catalog {
  class CatalogAccessor;
}

namespace tpl::parsing {
class Rewriter {
 public:
  Rewriter(ast::Context *ctx, terrier::catalog::CatalogAccessor * accessor);

  ast::Expr * RewriteBuiltinCall(ast::CallExpr* call);

 private:
  using IndexProjectionMap = std::unordered_map<terrier::catalog::indexkeycol_oid_t, uint16_t>;

  ast::Expr * RewritePCIGet(ast::CallExpr* call, ast::Builtin builtin);
  ast::Expr * RewriteTableAndIndexInitCall(ast::CallExpr* call, ast::Builtin old_builtin);
  ast::Expr * RewriteIndexIteratorGet(ast::CallExpr* call, ast::Builtin old_builtin);
  ast::Expr * RewriteIndexIteratorSetKey(ast::CallExpr* call, ast::Builtin old_builtin);
  ast::Expr * RewriteFilterCall(ast::CallExpr* call, ast::Builtin old_builtin);

  ast::Context * ctx_;
  terrier::catalog::CatalogAccessor * accessor_;
  // Map from alias to table oid
  std::unordered_map<std::string, terrier::catalog::table_oid_t> table_oids_;

  // Caches to avoid multiple catalog calls.
  std::unordered_map<std::string, terrier::catalog::Schema> table_schemas_;
  std::unordered_map<std::string, std::vector<terrier::catalog::col_oid_t>> col_oids_;
  std::unordered_map<std::string, terrier::storage::ProjectionMap> table_offsets_;
  std::unordered_map<std::string, terrier::catalog::IndexSchema> index_schemas_;
  std::unordered_map<std::string, IndexProjectionMap> index_offsets_;

};

}  // namespace tpl::parsing
