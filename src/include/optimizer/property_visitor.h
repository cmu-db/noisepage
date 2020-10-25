#pragma once

namespace noisepage::optimizer {

// Forward Declaration
class PropertySort;

/**
 * Defines an abstract interface for visitng properties
 */
class PropertyVisitor {
 public:
  /**
   * Trivial destructor
   */
  virtual ~PropertyVisitor() = default;

  /**
   * Virtual function for visiting PropertySort
   * @param prop PropertySort being visited
   */
  virtual void Visit(const PropertySort *prop) = 0;
};

}  // namespace noisepage::optimizer
