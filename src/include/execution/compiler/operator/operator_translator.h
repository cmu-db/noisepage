#pragma once

namespace tpl::compiler {

class OperatorTranslator {
 public:
  void InitializeQueryState();
  void TeardownQueryState();

  void Produce();
};

}