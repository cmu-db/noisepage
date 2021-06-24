#pragma once

#include <string>

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"

namespace noisepage::execution::ast {

class AstNode;

/**
 * The AstClone class encapsulates the logic necessary to clone an AST.
 */
class AstClone {
 public:
  /**
   * Clones an ASTNode and its descendants.
   * @param node The root of the AST to clone
   * @param factory The AstNodeFactory instance from which AST nodes are allocated
   * @param old_context The old AST context
   * @param new_context The new AST context
   * @return
   */
  static AstNode *Clone(AstNode *node, AstNodeFactory *factory, Context *old_context, Context *new_context);
};

}  // namespace noisepage::execution::ast
