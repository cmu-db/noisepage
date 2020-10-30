#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "optimizer/property.h"

namespace noisepage::optimizer {

/**
 * PropertySet represents a set of properties
 */
class PropertySet {
 public:
  /**
   * Default constructor for PropertySet
   */
  PropertySet() = default;

  /**
   * Destructor
   */
  ~PropertySet() {
    for (auto prop : properties_) {
      delete prop;
    }
  }

  /**
   * Copy
   */
  PropertySet *Copy() {
    std::vector<Property *> props;
    for (auto prop : properties_) {
      props.push_back(prop->Copy());
    }

    return new PropertySet(props);
  }

  /**
   * Constructor for PropertySet with vector of properties
   * PropertySet acquires ownership of the property.
   * @param properties properties to add to PropertySet
   */
  explicit PropertySet(std::vector<Property *> properties) : properties_(std::move(properties)) {}

  /**
   * Gets the properties in the PropertySet
   * @returns properties stored in PropertySet
   */
  const std::vector<Property *> &Properties() const { return properties_; }

  /**
   * Adds a property to the PropertySet
   * @param property Property to add to PropertySet
   */
  void AddProperty(Property *property);

  /**
   * Gets a property of a given type from PropertySet
   * @param type Type of the property to retrieve
   * @returns nullptr or pointer to property
   */
  const Property *GetPropertyOfType(PropertyType type) const;

  /**
   * Gets a property of a given type with additional type-check
   * @param type Type of the property to retrieve
   * @returns Pointer to Property of specified type
   */
  template <typename T>
  const T *GetPropertyOfTypeAs(PropertyType type) const {
    auto property = GetPropertyOfType(type);
    if (property) return property->As<T>();
    return nullptr;
  }

  /**
   * Hashes this PropertySet into a hash code
   * @returns Hash code
   */
  common::hash_t Hash() const;

  /**
   * Checks whether the PropertySet has a given property.
   * This check includes checking whether another property is >= parameter.
   * @param r_property Property to see whether included
   * @returns TRUE if r_property covered by the PropertySet
   */
  bool HasProperty(const Property &r_property) const;

  /**
   * Checks whether this PropertySet is >= another PropertySet
   * @param r PropertySet to check against
   * @returns TRUE if this >= r
   */
  bool operator>=(const PropertySet &r) const;

  /**
   * Checks whether this PropertySet is == another PropertySet
   * @param r PropertySet to check against
   * @returns TRUE if this == r
   */
  bool operator==(const PropertySet &r) const;

 private:
  std::vector<Property *> properties_;
};

/**
 * Defines struct for hashing a property set
 */
struct PropSetPtrHash {
  /**
   * Hashes a PropertySet
   * @param s PropertySet to hash
   * @returns hash code
   */
  std::size_t operator()(PropertySet *const &s) const { return s->Hash(); }
};

/**
 * Defines struct for checking property set equality
 */
struct PropSetPtrEq {
  /**
   * Checks whether two PropertySet are equal or not
   * @param t1 One of the PropertySet
   * @param t2 Other PropertySet
   * @returns whether t1 is equal to t2
   */
  bool operator()(PropertySet *const &t1, PropertySet *const &t2) const { return *t1 == *t2; }
};

}  // namespace noisepage::optimizer

namespace std {

/**
 * Implementation of std::hash for PropertySet
 */
template <>
struct hash<noisepage::optimizer::PropertySet> {
  /**
   * Defines argument_type to be PropertySet
   */
  using argument_type = noisepage::optimizer::PropertySet;

  /**
   * Defines result_type to be size_t
   */
  using result_type = std::size_t;

  /**
   * Implementation of hash() for PropertySet
   * @param s PropertySet to hash
   * @returns hash code
   */
  result_type operator()(argument_type const &s) const { return s.Hash(); }
};

}  // namespace std
