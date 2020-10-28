#pragma once
#include <string>

namespace noisepage::execution::ast {

class AstNode;

/**
 * Utility class to dump the AST to a string.
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

}  // namespace noisepage::execution::ast
