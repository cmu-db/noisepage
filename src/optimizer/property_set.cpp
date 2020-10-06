#include "optimizer/property_set.h"

#include "common/hash_util.h"
#include "loggers/optimizer_logger.h"

namespace terrier::optimizer {

void PropertySet::AddProperty(Property *property) {
  OPTIMIZER_LOG_TRACE("Add property with type {0}", static_cast<int>(property->Type()));
  auto iter = properties_.begin();
  for (; iter != properties_.end(); iter++) {
    // Iterate until point where preserve descending order
    if (property->Type() < (*iter)->Type()) {
      break;
    }
  }

  properties_.insert(iter, property);
}

const Property *PropertySet::GetPropertyOfType(PropertyType type) const {
  for (auto &prop : properties_) {
    if (prop->Type() == type) {
      return prop;
    }
  }

  OPTIMIZER_LOG_TRACE("Didn't find property with type {0}", static_cast<int>(type));
  return nullptr;
}

bool PropertySet::HasProperty(const Property &r_property) const {
  for (auto property : properties_) {
    if (*property >= r_property) {
      return true;
    }
  }

  return false;
}

bool PropertySet::operator>=(const PropertySet &r) const {
  for (auto r_property : r.properties_) {
    if (!HasProperty(*r_property)) return false;
  }
  return true;
}

bool PropertySet::operator==(const PropertySet &r) const { return *this >= r && r >= *this; }

common::hash_t PropertySet::Hash() const {
  size_t prop_size = properties_.size();
  common::hash_t hash = common::HashUtil::Hash<size_t>(prop_size);
  for (auto &prop : properties_) {
    hash = common::HashUtil::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

}  // namespace terrier::optimizer
