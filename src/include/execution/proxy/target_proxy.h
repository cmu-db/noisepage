//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// target_proxy.h
//
// Identification: src/include/execution/proxy/target_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "common/internal_types.h"
#include "planner/project_info.h"

namespace terrier::execution {


PROXY(Target) {
  DECLARE_MEMBER(0, char[sizeof(peloton::Target)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(Target, peloton::Target);


}  // namespace terrier::execution
