//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pool_proxy.cpp
//
// Identification: src/execution/proxy/pool_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/pool_proxy.h"

namespace terrier::execution {


DEFINE_TYPE(AbstractPool, "type::AbstractPool", opaque);
DEFINE_TYPE(EphemeralPool, "type::EphemeralPool", opaque);


}  // namespace terrier::execution
