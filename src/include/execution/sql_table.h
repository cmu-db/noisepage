#pragma once
#include "common/typedefs.h"

namespace terrier::execution {
class SqlTable {
 public:
  oid_t TableOid() const { return oid_t(0); }
};
}  // namespace terrier::execution
