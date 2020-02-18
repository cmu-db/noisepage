#pragma once

#include <vector>
#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node.h"

namespace terrier::optimizer {

class ExpressionNode : public AbstractOptimizerNode {
 public:
  ExpressionNode(common::ManagedPointer<AbstractOptimizerNodeContents> contents) {
    // TODO(esargent): bring over assert for contents being expression-based
    contents_ = contents;
  }

  const std::vector<common::ManagedPointer<AbstractOptimizerNode>> &GetChildren() cosnt { return children_; }

  const common::ManagedPointer<AbstractOptimizerNodeContents> Contents() const {
    // TODO(esargent): bring over assert for contents being expression-based
    return contents_;
  }

  const std::string GetInfo() const {
    // TODO(): create proper info statement?
    return "";
  }

 private:
  common::ManagedPointer<AbstractOptimizerNodeContents> contents_;
  std::vector < common::ManagedPointer<AbstractOptimizerNode> children_;
};

}  // namespace terrier::optimizer