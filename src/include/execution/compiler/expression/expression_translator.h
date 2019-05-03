#pragma once

namespace tpl::compiler {

class ExpressionTranslator {
 public:
  virtual void InitializeQueryState();
  virtual void TeardownQueryState();

 private:
};

}  // namespace tpl::compiler