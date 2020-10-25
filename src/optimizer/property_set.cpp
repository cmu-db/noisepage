#include "optimizer/property_set.h"

#include "common/hash_util.h"
#include "loggers/optimizer_logger.h"

namespace noisepage::optimizer {

void PropertySet::AddProperty(Property *property, bool optional) {
  OPTIMIZER_LOG_TRACE("Add property with type {0}", static_cast<int>(property->Type()));
  auto iter = properties_.begin();
  for (; iter != properties_.end(); iter++) {
    // Iterate until point where preserve descending order
    if (property->Type() < iter->first->Type()) {
      break;
    }
  }

  properties_.insert(iter, {property, optional});
}

std::pair<Property *, bool> PropertySet::GetPropertyOfType(PropertyType type) const {
  for (auto &prop : properties_) {
    if (prop.first->Type() == type) {
      return prop;
    }
  }

  OPTIMIZER_LOG_TRACE("Didn't find property with type {0}", static_cast<int>(type));
  return {nullptr, false};
}

bool PropertySet::HasProperty(const Property &r_property) const {
  for (auto property : properties_) {
    if (*property.first >= r_property) {
      return true;
    }
  }

  return false;
}

bool PropertySet::operator>=(const PropertySet &r) const {
  for (auto r_property : r.properties_) {
    if (!HasProperty(*(r_property.first))) return false;
  }
  return true;
}

bool PropertySet::operator==(const PropertySet &r) const { return *this >= r && r >= *this; }

common::hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  common::hash_t hash = common::HashUtil::Hash<size_t>(prop_size);
  for (auto &prop : properties_) {
    hash = common::HashUtil::CombineHashes(hash, prop.first->Hash());
  }
  return hash;
}

}  // namespace noisepage::optimizer
