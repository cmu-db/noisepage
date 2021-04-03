#pragma once

#include <string>

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"

namespace noisepage::execution::ast {

class AstNode;

/**
 * TODO(Kyle): Document.
 */
class AstClone {
 public:
  /**
   * Clones an ASTNode and its descendants.
   * TODO(Kyle): Document.
   */
  static AstNode *Clone(AstNode *node, AstNodeFactory *factory, std::string prefix, Context *old_context,
                        Context *new_context);
};

}  // namespace noisepage::execution::ast
