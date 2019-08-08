#include "execution/parsing/rewriter.h"

#include <string>
#include <utility>

#include "llvm/ADT/SmallVector.h"

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/error_message.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/macros.h"

namespace terrier::execution::parsing {

Rewriter::Rewriter(execution::ast::Context *ctx, catalog::CatalogAccessor *accessor) : ctx_(ctx), accessor_(accessor) {}

// Generate a call to the given builtin using the given arguments
ast::Expr *GenCallBuiltin(ast::Context *ctx, SourcePosition pos, ast::Builtin builtin,
                          const llvm::SmallVectorImpl<ast::Expr *> &args) {
  auto *name = ctx->node_factory()->NewIdentifierExpr(pos, ctx->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  return ctx->node_factory()->NewBuiltinCallExpr(name, {args.begin(), args.end(), ctx->region()});
}

ast::Expr *Rewriter::RewriteBuiltinCall(execution::ast::CallExpr *call) {
  ast::Builtin builtin;
  if (!ctx_->IsBuiltinFunction(call->GetFuncName(), &builtin)) {
    ctx_->error_reporter()->Report(call->function()->position(), sema::ErrorMessages::kInvalidBuiltinFunction,
                                   call->GetFuncName());
    UNREACHABLE("Function should only be called with a builtin function");
  }

  switch (builtin) {
    case ast::Builtin::PCIGetBind:
      return RewritePCIGet(call, builtin);
    case ast::Builtin::TableIterConstructBind:
    case ast::Builtin::TableIterAddColBind:
    case ast::Builtin::TableIterPerformInitBind:
    case ast::Builtin::IndexIteratorConstructBind:
    case ast::Builtin::IndexIteratorAddColBind:
    case ast::Builtin::IndexIteratorPerformInitBind:
      return RewriteTableAndIndexInitCall(call, builtin);
    case ast::Builtin::FilterEqBind:
    case ast::Builtin::FilterNeBind:
    case ast::Builtin::FilterLeBind:
    case ast::Builtin::FilterLtBind:
    case ast::Builtin::FilterGeBind:
    case ast::Builtin::FilterGtBind:
      return RewriteFilterCall(call, builtin);
    case ast::Builtin::IndexIteratorGetBind:
      return RewriteIndexIteratorGet(call, builtin);
    case ast::Builtin::IndexIteratorSetKeyBind:
      return RewriteIndexIteratorSetKey(call, builtin);
    default:
      // No need to rewrite anything
      return call;
  }
}

ast::Expr *Rewriter::RewritePCIGet(execution::ast::CallExpr *call, ast::Builtin old_builtin) {
  TPL_ASSERT(call->arguments().size() == 3, "PCIGetBind call takes in 3 arguments");
  ast::Expr *pci = call->arguments()[0];
  auto alias = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
  auto col_name = call->arguments()[2]->SafeAs<ast::LitExpr>()->raw_string_val().data();

  auto schema = table_schemas_.at(alias);
  auto col = schema.GetColumn(col_name);
  u32 col_offset = table_offsets_.at(alias).at(col.Oid());

  bool nullable = col.Nullable();
  ast::Builtin builtin;
  switch (col.Type()) {
    case type::TypeId::INTEGER:
      builtin = nullable ? ast::Builtin::PCIGetIntNull : ast::Builtin::PCIGetInt;
      break;
    case type::TypeId::TINYINT:
      builtin = nullable ? ast::Builtin::PCIGetTinyIntNull : ast::Builtin::PCIGetTinyInt;
      break;
    case type::TypeId::SMALLINT:
      builtin = nullable ? ast::Builtin::PCIGetSmallIntNull : ast::Builtin::PCIGetSmallInt;
      break;
    case type::TypeId::BIGINT:
      builtin = nullable ? ast::Builtin::PCIGetBigIntNull : ast::Builtin::PCIGetBigInt;
      break;
    case type::TypeId::DECIMAL:
      builtin = nullable ? ast::Builtin::PCIGetDoubleNull : ast::Builtin::PCIGetDouble;
      break;
    case type::TypeId::DATE:
      builtin = nullable ? ast::Builtin::PCIGetDateNull : ast::Builtin::PCIGetDate;
      break;
    case type::TypeId::VARCHAR:
      builtin = nullable ? ast::Builtin::PCIGetVarlenNull : ast::Builtin::PCIGetVarlen;
      break;
    default:
      UNREACHABLE("Cannot @pciGetBind unsupported type");
  }

  ast::Expr *col_offset_expr = ctx_->node_factory()->NewIntLiteral(pci->position(), col_offset);
  llvm::SmallVector<ast::Expr *, 2> args = {pci, col_offset_expr};
  return GenCallBuiltin(ctx_, call->position(), builtin, args);
}

ast::Expr *Rewriter::RewriteTableAndIndexInitCall(execution::ast::CallExpr *call, ast::Builtin old_builtin) {
  switch (old_builtin) {
    case ast::Builtin::TableIterConstructBind: {
      TPL_ASSERT(call->arguments().size() == 4, "TableIterConstructBind call takes in 4 arguments");
      ast::Expr *tvi = call->arguments()[0];
      auto table_name = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
      auto exec_ctx = call->arguments()[2];
      auto alias = call->arguments()[3]->SafeAs<ast::LitExpr>()->raw_string_val().data();

      // Fill information
      TPL_ASSERT(table_oids_.count(alias) == 0, "Alias already exists");
      auto table_oid = accessor_->GetTableOid(accessor_->GetDefaultNamespace(), table_name);
      table_oids_[alias] = table_oid;
      table_schemas_[alias] = accessor_->GetSchema(table_oid);

      // Make Call
      auto table_oid_expr = ctx_->node_factory()->NewIntLiteral(tvi->position(), !table_oid);
      llvm::SmallVector<ast::Expr *, 3> args = {tvi, table_oid_expr, exec_ctx};
      return GenCallBuiltin(ctx_, call->position(), ast::Builtin::TableIterConstruct, args);
    }
    case ast::Builtin::IndexIteratorConstructBind: {
      TPL_ASSERT(call->arguments().size() == 5, "IndexIterConstructBind call takes in 5 arguments");
      ast::Expr *index = call->arguments()[0];
      auto table_name = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
      auto index_name = call->arguments()[2]->SafeAs<ast::LitExpr>()->raw_string_val().data();
      auto exec_ctx = call->arguments()[3];
      auto alias = call->arguments()[4]->SafeAs<ast::LitExpr>()->raw_string_val().data();

      // Fill information
      TPL_ASSERT(table_oids_.count(alias) == 0, "Alias already exists");
      auto table_oid = accessor_->GetTableOid(accessor_->GetDefaultNamespace(), table_name);
      auto index_oid = accessor_->GetIndexOid(index_name);
      table_oids_[alias] = table_oid;
      table_schemas_[alias] = accessor_->GetSchema(table_oid);
      index_schemas_[alias] = accessor_->GetIndexSchema(index_oid);
      index_offsets_[alias] = accessor_->GetIndex(index_oid)->GetKeyOidToOffsetMap();

      // Make Call
      ast::Expr *table_oid_expr = ctx_->node_factory()->NewIntLiteral(index->position(), !table_oid);
      ast::Expr *index_oid_expr = ctx_->node_factory()->NewIntLiteral(index->position(), !index_oid);
      llvm::SmallVector<ast::Expr *, 4> args = {index, table_oid_expr, index_oid_expr, exec_ctx};
      return GenCallBuiltin(ctx_, call->position(), ast::Builtin::IndexIteratorConstruct, args);
    }
    case ast::Builtin::TableIterPerformInitBind:
    case ast::Builtin::IndexIteratorPerformInitBind: {
      TPL_ASSERT(call->arguments().size() == 2, "PerformInitBind call in 2 arguments");
      ast::Expr *tvi = call->arguments()[0];
      auto alias = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();

      // Get Projection map
      auto builtin = (old_builtin == ast::Builtin::TableIterPerformInitBind) ? ast::Builtin::TableIterPerformInit
                                                                             : ast::Builtin::IndexIteratorPerformInit;
      const auto table_oid = table_oids_[alias];
      const auto &col_oids = col_oids_[alias];
      auto sql_table = accessor_->GetTable(table_oid);
      table_offsets_[alias] = sql_table->ProjectionMapForOids(col_oids);
      llvm::SmallVector<ast::Expr *, 1> args = {tvi};
      return GenCallBuiltin(ctx_, call->position(), builtin, args);
    }
    case ast::Builtin::TableIterAddColBind:
    case ast::Builtin::IndexIteratorAddColBind: {
      TPL_ASSERT(call->arguments().size() == 3, "AddColBind call takes in 3 arguments");
      ast::Expr *tvi = call->arguments()[0];
      auto alias = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
      auto col_name = call->arguments()[2]->SafeAs<ast::LitExpr>()->raw_string_val().data();

      // Add oid to list
      const auto &schema = table_schemas_.at(alias);
      auto col_oid = schema.GetColumn(col_name).Oid();
      col_oids_[alias].emplace_back(col_oid);

      // Make Call
      auto builtin = (old_builtin == ast::Builtin::TableIterAddColBind) ? ast::Builtin::TableIterAddCol
                                                                        : ast::Builtin::IndexIteratorAddCol;
      ast::Expr *col_oid_expr = ctx_->node_factory()->NewIntLiteral(tvi->position(), !col_oid);
      llvm::SmallVector<ast::Expr *, 2> args = {tvi, col_oid_expr};
      return GenCallBuiltin(ctx_, call->position(), builtin, args);
    }
    default: { UNREACHABLE("Impossible TableIterBind call!!"); }
  }
}

ast::Expr *Rewriter::RewriteIndexIteratorGet(execution::ast::CallExpr *call, execution::ast::Builtin old_builtin) {
  TPL_ASSERT(call->arguments().size() == 3, "IndexIteratorGetBind call takes in 3 arguments");
  ast::Expr *index = call->arguments()[0];
  auto alias = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
  auto col_name = call->arguments()[2]->SafeAs<ast::LitExpr>()->raw_string_val().data();

  auto schema = table_schemas_.at(alias);
  auto col = schema.GetColumn(col_name);
  u32 col_offset = table_offsets_.at(alias).at(col.Oid());

  bool nullable = col.Nullable();
  ast::Builtin builtin;
  switch (col.Type()) {
    case type::TypeId::INTEGER:
      builtin = nullable ? ast::Builtin::IndexIteratorGetIntNull : ast::Builtin::IndexIteratorGetInt;
      break;
    case type::TypeId::TINYINT:
      builtin = nullable ? ast::Builtin::IndexIteratorGetTinyIntNull : ast::Builtin::IndexIteratorGetTinyInt;
      break;
    case type::TypeId::SMALLINT:
      builtin = nullable ? ast::Builtin::IndexIteratorGetSmallIntNull : ast::Builtin::IndexIteratorGetSmallInt;
      break;
    case type::TypeId::BIGINT:
      builtin = nullable ? ast::Builtin::IndexIteratorGetBigIntNull : ast::Builtin::IndexIteratorGetBigInt;
      break;
    case type::TypeId::DECIMAL:
      builtin = nullable ? ast::Builtin::IndexIteratorGetDoubleNull : ast::Builtin::IndexIteratorGetDouble;
      break;
    // case type::TypeId::DATE:
    // builtin = nullable ? ast::Builtin::IndexIteratorGetDateNull : ast::Builtin::IndexIteratorGetDate;
    // break;
    // case type::TypeId::VARCHAR:
    // builtin = nullable ? ast::Builtin::IndexIteratorGetVarlenNull : ast::Builtin::IndexIteratorGetVarlen;
    // break;
    default:
      UNREACHABLE("Cannot @indexIteratorGetBind unsupported type");
  }

  ast::Expr *col_offset_expr = ctx_->node_factory()->NewIntLiteral(index->position(), col_offset);
  llvm::SmallVector<ast::Expr *, 2> args = {index, col_offset_expr};
  return GenCallBuiltin(ctx_, call->position(), builtin, args);
}

ast::Expr *Rewriter::RewriteIndexIteratorSetKey(execution::ast::CallExpr *call, execution::ast::Builtin old_builtin) {
  TPL_ASSERT(call->arguments().size() == 4, "IndexIteratorGetBind call takes in 3 arguments");
  ast::Expr *index = call->arguments()[0];
  auto alias = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
  auto col_name = call->arguments()[2]->SafeAs<ast::LitExpr>()->raw_string_val().data();
  ast::Expr *val = call->arguments()[3];

  const auto &index_schema = index_schemas_.at(alias);
  const auto &offsets = index_offsets_.at(alias);
  auto col = index_schema.GetColumn(col_name);
  u32 col_offset = offsets.at(col.Oid());

  ast::Builtin builtin;

  switch (col.Type()) {
    case type::TypeId::INTEGER:
      builtin = ast::Builtin::IndexIteratorSetKeyInt;
      break;
    case type::TypeId::TINYINT:
      builtin = ast::Builtin::IndexIteratorSetKeyTinyInt;
      break;
    case type::TypeId::SMALLINT:
      builtin = ast::Builtin::IndexIteratorSetKeySmallInt;
      break;
    case type::TypeId::BIGINT:
      builtin = ast::Builtin::IndexIteratorSetKeyBigInt;
      break;
    case type::TypeId::DECIMAL:
      builtin = ast::Builtin::IndexIteratorSetKeyDouble;
      break;
      // case type::TypeId::DATE:
      // builtin = nullable ? ast::Builtin::IndexIteratorGetDateNull : ast::Builtin::IndexIteratorGetDate;
      // break;
      // case type::TypeId::VARCHAR:
      // builtin = nullable ? ast::Builtin::IndexIteratorGetVarlenNull : ast::Builtin::IndexIteratorGetVarlen;
      // break;
    default:
      UNREACHABLE("Cannot @indexIteratorSetKeyBind unsupported type");
  }

  ast::Expr *col_offset_expr = ctx_->node_factory()->NewIntLiteral(index->position(), col_offset);
  llvm::SmallVector<ast::Expr *, 3> args = {index, col_offset_expr, val};
  return GenCallBuiltin(ctx_, call->position(), builtin, args);
}

ast::Expr *Rewriter::RewriteFilterCall(execution::ast::CallExpr *call, execution::ast::Builtin old_builtin) {
  TPL_ASSERT(call->arguments().size() == 4, "PCIGetBind call takes in 3 arguments");
  ast::Expr *pci = call->arguments()[0];
  auto alias = call->arguments()[1]->SafeAs<ast::LitExpr>()->raw_string_val().data();
  auto col_name = call->arguments()[2]->SafeAs<ast::LitExpr>()->raw_string_val().data();
  ast::Expr *filter_val = call->arguments()[3];

  auto schema = table_schemas_.at(alias);
  auto col = schema.GetColumn(col_name);
  u32 col_offset = table_offsets_.at(alias).at(col.Oid());

  ast::Builtin builtin;
  switch (old_builtin) {
    case ast::Builtin::FilterEqBind:
      builtin = ast::Builtin::FilterEq;
      break;
    case ast::Builtin::FilterNeBind:
      builtin = ast::Builtin::FilterNe;
      break;
    case ast::Builtin::FilterLeBind:
      builtin = ast::Builtin::FilterLe;
      break;
    case ast::Builtin::FilterLtBind:
      builtin = ast::Builtin::FilterLt;
      break;
    case ast::Builtin::FilterGeBind:
      builtin = ast::Builtin::FilterGe;
      break;
    case ast::Builtin::FilterGtBind:
      builtin = ast::Builtin::FilterGt;
      break;
    default:
      UNREACHABLE("Impossible @pciFilterBind call!!");
  }

  ast::Expr *col_offset_expr = ctx_->node_factory()->NewIntLiteral(pci->position(), col_offset);
  ast::Expr *type_expr = ctx_->node_factory()->NewIntLiteral(pci->position(), static_cast<i8>(col.Type()));
  llvm::SmallVector<ast::Expr *, 4> args = {pci, col_offset_expr, type_expr, filter_val};
  return GenCallBuiltin(ctx_, call->position(), builtin, args);
}

}  // namespace terrier::execution::parsing
