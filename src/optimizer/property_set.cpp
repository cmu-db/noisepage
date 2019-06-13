#include "common/hash_util.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/property_set.h"

namespace terrier {
namespace optimizer {

/**
 * Adds a property to the PropertySet
 * @param property Property to add to PropertySet
 */
void PropertySet::AddProperty(Property* property) {
  OPTIMIZER_LOG_TRACE("Add property with type %d", static_cast<int>(property->Type()));
  auto iter = properties_.begin();
  for (; iter != properties_.end(); iter++) {
    // Iterate until point where preserve descending order
    if (property->Type() < (*iter)->Type()) {
      break;
    }
  }

  properties_.insert(iter, property);
}

/**
 * Gets a property of a given type from PropertySet
 * @param type Type of the property to retrieve
 * @returns shared pointer to the Property or nullptr
 */
const Property* PropertySet::GetPropertyOfType(PropertyType type) const {
  for (auto &prop : properties_) {
    if (prop->Type() == type) {
      return prop;
    }
  }

  OPTIMIZER_LOG_TRACE("Didn't find property with type %d", static_cast<int>(type)); 
  return nullptr;
}

/**
 * Checks whether the PropertySet has a given property.
 * This check includes checking whether another property is >= parameter.
 * @param r_property Property to see whether included
 * @returns TRUE if r_property covered by the PropertySet
 */
bool PropertySet::HasProperty(const Property &r_property) const {
  for (auto property : properties_) {
    if (*property >= r_property) {
      return true;
    }
  }

  return false;
}

/**
 * Checks whether this PropertySet is >= another PropertySet
 * @param r PropertySet to check against
 * @returns TRUE if this >= r
 */
bool PropertySet::operator>=(const PropertySet &r) const {
  for (auto r_property : r.properties_) {
    if (HasProperty(*r_property) == false) return false;
  }
  return true;
}

/**
 * Checks whether this PropertySet is == another PropertySet
 * @param r PropertySet to check against
 * @returns TRUE if this == r
 */
bool PropertySet::operator==(const PropertySet &r) const {
  return *this >= r && r >= *this;
}

/**
 * Hashes this PropertySet into a hash code
 * @returns Hash code
 */
common::hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  common::hash_t hash = common::HashUtil::Hash<size_t>(prop_size);
  for (auto &prop : properties_) {
    hash = common::HashUtil::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

}  // namespace optimizer
}  // namespace terrier
