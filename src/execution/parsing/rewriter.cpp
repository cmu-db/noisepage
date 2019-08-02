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
ast::Stmt *RewriteForInScan(ast::Context *ctx, ast::ForInStmt *for_in) {
  UNREACHABLE("Rewrite for-in loops is not yet supported. Use explicit loops.");
}

}  // namespace tpl::parsing
