#pragma once

namespace tpl::compiler {

class OperatorTranslator {
 public:
  virtual void InitializeQueryState() = 0;
  virtual void TeardownQueryState() = 0;

  virtual void Produce() const = 0;

 private:
};

}  // namespace tpl::compiler