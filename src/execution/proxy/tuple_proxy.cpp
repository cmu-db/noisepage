//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_proxy.cpp
//
// Identification: src/execution/proxy/tuple_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/proxy/tuple_proxy.h"

namespace terrier::execution {

DEFINE_TYPE(Tuple, "storage::Tuple", opaque);

}  // namespace terrier::execution
