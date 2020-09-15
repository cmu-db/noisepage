#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/hash_util.h"
#include "common/managed_pointer.h"
#include "optimizer/abstract_optimizer_node_contents.h"
#include "optimizer/optimizer_defs.h"

namespace terrier::optimizer {

/**
 * An abstract class for expression-based and operator-based nodes for the rewriter
 * and optimizer, respectively
 */
class AbstractOptimizerNode {
 public:
  AbstractOptimizerNode() = default;

  virtual ~AbstractOptimizerNode() = default;

  /**
   * Pushes a child node onto this node's children
   * @param child a child node
   */
  virtual void PushChild(std::unique_ptr<AbstractOptimizerNode> child) = 0;

  /**
   * @return The vector of this node's children.
   */
  virtual std::vector<common::ManagedPointer<AbstractOptimizerNode>> GetChildren() const = 0;

  /**
   * @return A pointer to the AbstractOptimizerNodeContents that this node contains.
   */
  virtual common::ManagedPointer<AbstractOptimizerNodeContents> Contents() const = 0;

  /**
   * @return a copy of the node
   */
  virtual std::unique_ptr<AbstractOptimizerNode> Copy() = 0;

 private:
};

}  // namespace terrier::optimizer
