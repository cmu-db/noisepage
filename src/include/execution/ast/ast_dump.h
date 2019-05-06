#pragma once

namespace tpl::ast {

class AstNode;

/// Class to dump the AST to standard output
class AstDump {
 public:
  static void Dump(AstNode *node);
};

}  // namespace tpl::ast
