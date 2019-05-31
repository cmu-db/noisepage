#include "execution/parsing/rewriter.h"

#include <string>
#include <utility>

#include "llvm/ADT/SmallVector.h"

#include "execution/ast/ast.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/util/macros.h"

namespace tpl::parsing {

namespace {

ast::Decl *DeclareVar(ast::Context *ctx, SourcePosition pos, const std::string &name, ast::Expr *type_repr,
                      ast::Expr *init) {
  auto name_ident = ctx->GetIdentifier(name);
  return ctx->node_factory()->NewVariableDecl(pos, name_ident, type_repr, init);
}

ast::Decl *DeclareVarNoType(ast::Context *ctx, SourcePosition pos, const std::string &name, ast::Expr *init) {
  return DeclareVar(ctx, pos, name, nullptr, init);
}

ast::Decl *DeclareVarNoInit(ast::Context *ctx, SourcePosition pos, const std::string &name, ast::Expr *type_repr) {
  return DeclareVar(ctx, pos, name, type_repr, nullptr);
}

ast::Decl *DeclareVarNoInit(ast::Context *ctx, SourcePosition pos, const std::string &name,
                            ast::BuiltinType::Kind kind) {
  auto *type = ast::BuiltinType::Get(ctx, kind);
  auto type_ident = ctx->GetIdentifier(type->tpl_name());
  auto type_expr = ctx->node_factory()->NewIdentifierExpr(pos, type_ident);
  return DeclareVarNoInit(ctx, pos, name, type_expr);
}

// Generate a call to the given builtin using the given arguments
ast::Expr *GenCallBuiltin(ast::Context *ctx, SourcePosition pos, ast::Builtin builtin,
                          const llvm::SmallVectorImpl<ast::Expr *> &args) {
  auto *name = ctx->node_factory()->NewIdentifierExpr(pos, ctx->GetIdentifier(ast::Builtins::GetFunctionName(builtin)));
  return ctx->node_factory()->NewBuiltinCallExpr(name, {args.begin(), args.end(), ctx->region()});
}

}  // namespace

ast::Stmt *RewriteForInScan(ast::Context *ctx, ast::ForInStmt *for_in) {
  // We want to convert the following:
  //
  // for (target in "table_name") {
  //   body
  // }
  //
  // into:
  //
  // var $tvi: TableVectorIterator
  // for (@tableIterInit(&$tvi, "table_name"); @tableIterAdvance(&$tvi); ) {
  //   var target = @tableIterGetPCI(&$tvi)
  //   (body)
  //   @pciReset(target)
  // }
  // @tableIterClose(&$tvi)
  //

  TPL_ASSERT(for_in->target()->IsIdentifierExpr(), "Target must be an identifier");
  TPL_ASSERT(for_in->iter()->IsLitExpr(), "Iterable must be a literal expression");
  TPL_ASSERT(for_in->iter()->As<ast::LitExpr>()->IsStringLitExpr(), "Iterable must be a string literal");

  const auto pos = for_in->position();
  auto *factory = ctx->node_factory();

  util::RegionVector<ast::Stmt *> statements(ctx->region());

  // The iterator's name
  ast::Identifier tvi_name = ctx->GetIdentifier("$tvi");

  // An expression computing the address of the iterator. Used all over here.
  ast::Expr *tvi_addr = factory->NewUnaryOpExpr(pos, Token::Type::AMPERSAND, factory->NewIdentifierExpr(pos, tvi_name));

  // Declare the TVI
  {
    auto *tvi_decl = DeclareVarNoInit(ctx, pos, tvi_name.data(), ast::BuiltinType::TableVectorIterator);
    statements.push_back(factory->NewDeclStmt(tvi_decl));
  }

  // Now the for-loop
  ast::Stmt *init = nullptr, *next = nullptr;
  ast::Expr *cond = nullptr;

  // First the initialization
  {
    llvm::SmallVector<ast::Expr *, 2> args = {tvi_addr, for_in->iter()};
    auto *call = GenCallBuiltin(ctx, pos, ast::Builtin::TableIterInit, args);
    init = factory->NewExpressionStmt(call);
  }

  // Next, the loop condition
  {
    llvm::SmallVector<ast::Expr *, 1> args = {tvi_addr};
    cond = GenCallBuiltin(ctx, pos, ast::Builtin::TableIterAdvance, args);
  }

  // Splice in the target initialization at the start of the body
  {
    auto &body = for_in->body()->statements();

    // Pull out PCI
    llvm::SmallVector<ast::Expr *, 1> args = {tvi_addr};
    auto *call = GenCallBuiltin(ctx, pos, ast::Builtin::TableIterGetPCI, args);
    auto pci_ident = for_in->target()->As<ast::IdentifierExpr>()->name();
    auto *pci_decl = DeclareVarNoType(ctx, pos, pci_ident.data(), call);
    body.insert(body.begin(), factory->NewDeclStmt(pci_decl));
  }

  {
    auto &body = for_in->body()->statements();

    auto pci_ident = for_in->target()->As<ast::IdentifierExpr>();
    llvm::SmallVector<ast::Expr *, 1> args = {pci_ident};
    auto *call = GenCallBuiltin(ctx, pos, ast::Builtin::PCIReset, args);
    body.push_back(factory->NewExpressionStmt(call));
  }

  // Add the loop to the running statements list
  statements.push_back(factory->NewForStmt(pos, init, cond, next, for_in->body()));

  // Close the iterator after the loop
  {
    llvm::SmallVector<ast::Expr *, 1> args = {tvi_addr};
    auto *call = GenCallBuiltin(ctx, pos, ast::Builtin::TableIterClose, args);
    statements.push_back(factory->NewExpressionStmt(call));
  }

  // Done
  return ctx->node_factory()->NewBlockStmt(pos, pos, std::move(statements));
}

}  // namespace tpl::parsing
