#pragma once

#include <string>

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"

namespace noisepage::execution::ast {

class AstNode;

class AstClone {
 public:
  /**
   * Clones an ASTNode and its descendants.
   * @param node The root of the AST to clone.
   * @param factory The AstNodeFactory instance from which AST nodes are allocated.
   * @param prefix The
   * @param old_context
   * @param new_context
   * @return
   */
  static AstNode *Clone(AstNode *node, AstNodeFactory *factory, Context *old_context, Context *new_context);
};

}  // namespace noisepage::execution::ast
