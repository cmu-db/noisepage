#include "optimizer/property.h"

#include "common/hash_util.h"

namespace terrier::optimizer {

hash_t Property::Hash() const {
  PropertyType t = Type();
  return common::HashUtil::Hash(t);
}

}  // namespace terrier::optimizer
