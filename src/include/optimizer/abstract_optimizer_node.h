#pragma once

#include <memory>
#include <string>
#include <vector>
#include "common/hash_util.h"
#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node_contents.h"
#include "optimizer/optimizer_defs.h"

namespace terrier::optimizer {

class AbstractOptimizerNode {
 public:
  AbstractOptimizerNode();

  ~AbstractOptimizerNode();

  /**
   * Pushes a child node onto this node's children
   * @param child a child node
   */
  virtual void PushChild(std::unique_ptr<AbstractOptimizerNode> child) = 0;

  /**
   * @return The vector of this node's children.
   */
  virtual const std::vector<common::ManagedPointer<AbstractOptimizerNode>> &GetChildren() const = 0;

  /**
   * @return A pointer to the AbstractOptimizerNodeContents that this node contains.
   */
  virtual const common::ManagedPointer<AbstractOptimizerNodeContents> Contents() const = 0;

  /**
   * @return String info on the node.
   */
  virtual const std::string GetInfo() const = 0;

 private:
};

}  // namespace terrier::optimizer
