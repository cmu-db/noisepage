#pragma once

namespace tpl::ast {
class Context;
class ForInStmt;
class Stmt;
}  // namespace tpl::ast

namespace tpl::parsing {

/**
 * Rewrites a for in statement to a normal for stmt
 * @param ctx ast context to use
 * @param for_in the for in stmt to rewrite
 * @return the rewritten stmt,
 */
ast::Stmt *RewriteForInScan(ast::Context *ctx, ast::ForInStmt *for_in);

}  // namespace tpl::parsing
