#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "execution/ast/ast.h"
#include "execution/ast/context.h"

namespace terrier::catalog {
class CatalogAccessor;
}

namespace terrier::parsing {

/**
 * This rewriter is ONLY for hand-writen TPL code.
 * It takes convenience builtins (those with a 'Bind' suffix) that take in string literal and converts them to
 * the real builtins that take in OIDs and raw offsets.
 * @warning This class performs the binding step in a sequential order. This means table & index call
 * should only be made after initialization in the TPL test (see q1.tpl for an example).
 */
class Rewriter {
 public:
  /**
   * Construct
   * @param ctx The ast context
   * @param accessor The catalog accessor
   */
  Rewriter(ast::Context *ctx, terrier::catalog::CatalogAccessor *accessor);

  /**
   * Attempts to rewrite a builtin call.
   * Returns the new expression if rewriting takes place.
   * Otherwise keeps the expression intact.
   * @param call The builtin call to rewrite
   * @return possibly rewritten builtin
   */
  ast::Expr *RewriteBuiltinCall(ast::CallExpr *call);

 private:
  using IndexProjectionMap = std::unordered_map<terrier::catalog::indexkeycol_oid_t, uint16_t>;

  ast::Expr *RewritePCIGet(ast::CallExpr *call, ast::Builtin old_builtin);
  ast::Expr *RewriteTableAndIndexInitCall(ast::CallExpr *call, ast::Builtin old_builtin);
  ast::Expr *RewriteIndexIteratorGet(ast::CallExpr *call, ast::Builtin old_builtin);
  ast::Expr *RewriteIndexIteratorSetKey(ast::CallExpr *call, ast::Builtin old_builtin);
  ast::Expr *RewriteFilterCall(ast::CallExpr *call, ast::Builtin old_builtin);

  ast::Context *ctx_;
  terrier::catalog::CatalogAccessor *accessor_;
  // Map from alias to table oid
  std::unordered_map<std::string, terrier::catalog::table_oid_t> table_oids_;

  // Caches to avoid multiple catalog calls.
  std::unordered_map<std::string, terrier::catalog::Schema> table_schemas_;
  std::unordered_map<std::string, std::vector<terrier::catalog::col_oid_t>> col_oids_;
  std::unordered_map<std::string, terrier::storage::ProjectionMap> table_offsets_;
  std::unordered_map<std::string, terrier::catalog::IndexSchema> index_schemas_;
  std::unordered_map<std::string, IndexProjectionMap> index_offsets_;
};

}  // namespace terrier::parsing
