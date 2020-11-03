#pragma once

#include <iosfwd>

namespace noisepage::execution::ast {

class AstNode;

/**
 * Utility class to pretty print an AST as a textual TPL program.
 */
class AstPrettyPrint {
 public:
  /**
   * Dump the provided AST node to the output stream supplied.
   */
  static void Dump(std::ostream &os, AstNode *node);
};

}  // namespace noisepage::execution::ast
