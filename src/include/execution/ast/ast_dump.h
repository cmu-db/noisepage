#pragma once

namespace terrier::ast {

class AstNode;

/**
 * Class to dump the AST to standard output
 */
class AstDump {
 public:
  /**
   * Dumps ast to std out
   * @param node node to dump
   */
  static void Dump(AstNode *node);
};

}  // namespace terrier::ast
