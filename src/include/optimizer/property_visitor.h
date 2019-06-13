#pragma once

namespace terrier {
namespace optimizer {

// Forward Declaration
class PropertySort;

//===--------------------------------------------------------------------===//
// Property Visitor
//===--------------------------------------------------------------------===//

// Visit physical properties
class PropertyVisitor {
 public:
  virtual ~PropertyVisitor(){};

  virtual void Visit(const PropertySort *) = 0;
};

} // namespace optimizer
} // namespace terrier
