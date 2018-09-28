//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// varlen_proxy.h
//
// Identification: src/include/execution/proxy/varlen_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/proxy/proxy.h"
#include "type/varlen_type.h"

namespace terrier::execution {


PROXY(Varlen) {
  DECLARE_MEMBER(0, uint32_t, length);
  DECLARE_MEMBER(1, const char, ptr);
  DECLARE_TYPE;
};


}  // namespace terrier::execution