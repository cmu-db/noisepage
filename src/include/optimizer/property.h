#pragma once

#include <typeinfo>

#include "common/hash_util.h"
#include "optimizer/optimizer_defs.h"

namespace noisepage::optimizer {

class PropertyVisitor;

/**
 * Enum defining the types of properties
 */
enum class PropertyType { SORT };

/**
 * Abstract interface defining a physical property (i.e sort).
 *
 * Physical properties are those fields that can be directly added to the plan,
 * and don't need to perform transformations on.
 *
 * Note: Sometimes there're different choices of physical properties. E.g., the
 * sorting order might be provided by either a sort executor or a underlying
 * sort merge join. But those different implementations are directly specified
 * when constructing the physical operator tree, other than using rule
 * transformation.
 */
class Property {
 public:
  /**
   * Trivial destructor for Property
   */
  virtual ~Property() = default;

  /**
   * @returns Type of the Property
   */
  virtual PropertyType Type() const = 0;

  /**
   * Copy
   */
  virtual Property *Copy() = 0;

  /**
   * Hashes the given Property
   * @returns Hash code
   */
  virtual common::hash_t Hash() const {
    PropertyType t = Type();
    return common::HashUtil::Hash(t);
  }

  /**
   * Checks whether this >= r
   * @returns TRUE if this and r share the same type
   */
  virtual bool operator>=(const Property &r) const { return Type() == r.Type(); }

  /**
   * Accept function used for visitor pattern
   * @param v Visitor
   */
  virtual void Accept(PropertyVisitor *v) const = 0;

  /**
   * Casts a generic Property to a specialized class
   * @returns specialized pointer or nullptr if invalid
   */
  template <typename T>
  const T *As() const {
    if (typeid(*this) == typeid(T)) {
      return reinterpret_cast<const T *>(this);
    }
    return nullptr;
  }
};

}  // namespace noisepage::optimizer
