#pragma once

#include <memory>
#include <vector>

#include "optimizer/operator_node.h"

namespace terrier::optimizer {

/**
 * Class defining a Pattern used for binding
 */
class Pattern {
 public:
  /**
   * Creates a new pattern
   * @param op Operator that node should match
   */
  explicit Pattern(OpType op) : _type(op) {}

  /**
   * Destructor. Deletes all children
   */
  ~Pattern() {
    for (auto child : children) {
      delete child;
    }
  }

  /**
   * Adds a child to the pattern.
   * Memory control of child passes to this pattern.
   *
   * @param child Pointer to child
   */
  void AddChild(Pattern *child) { children.push_back(child); }

  /**
   * Gets a vector of the children
   * @returns managed children of the pattern node
   */
  const std::vector<Pattern *> &Children() const { return children; }

  /**
   * Gets number of children
   * @returns number of children
   */
  size_t GetChildPatternsSize() const { return children.size(); }

  /**
   * Gets the operator this Pattern supposed to represent
   * @returns OpType that Pattern matches against
   */
  OpType Type() const { return _type; }

 private:
  OpType _type;
  std::vector<Pattern *> children;
};

}  // namespace terrier::optimizer
