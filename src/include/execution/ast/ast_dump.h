#pragma once
#include <string>

namespace terrier::execution::ast {

class AstNode;

/**
 * Class to dump the AST to a string.
 */
class AstDump {
 public:
  /**
   * Dumps ast to a string
   * @param node node to dump
   * @return output string
   */
  static std::string Dump(AstNode *node);
};

}  // namespace terrier::execution::ast
